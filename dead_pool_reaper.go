package work

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	deadTime          = 10 * time.Second // 2 x heartbeat
	reapPeriod        = 10 * time.Minute
	reapJitterSecs    = 30
	requeueKeysPerJob = 4
)

type deadPoolReaper struct {
	namespace   string
	pool        *redis.Pool
	deadTime    time.Duration
	reapPeriod  time.Duration
	curJobTypes []string

	stopChan         chan struct{}
	doneStoppingChan chan struct{}
	commander        DBCommand
}

func newDeadPoolReaper(namespace string, pool *redis.Pool, curJobTypes []string, commanders ...DBCommand) *deadPoolReaper {
	var commander DBCommand
	if len(commanders) == 0 {
		commander = &RedisDBCommand{}
	} else {
		commander = commanders[0]
	}
	return &deadPoolReaper{
		namespace:        namespace,
		pool:             pool,
		deadTime:         deadTime,
		reapPeriod:       reapPeriod,
		curJobTypes:      curJobTypes,
		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
		commander:        commander,
	}
}

func (r *deadPoolReaper) start() {
	go r.loop()
}

func (r *deadPoolReaper) stop() {
	r.stopChan <- struct{}{}
	<-r.doneStoppingChan
}

func (r *deadPoolReaper) loop() {
	// Reap immediately after we provide some time for initialization
	timer := time.NewTimer(r.deadTime)
	defer timer.Stop()

	for {
		select {
		case <-r.stopChan:
			r.doneStoppingChan <- struct{}{}
			return
		case <-timer.C:
			// Schedule next occurrence periodically with jitter
			timer.Reset(r.reapPeriod + time.Duration(rand.Intn(reapJitterSecs))*time.Second)

			// Reap
			if err := r.reap(); err != nil {
				logError("dead_pool_reaper.reap", err)
			}
		}
	}
}

func (r *deadPoolReaper) reap() error {
	// Get dead pools
	deadPoolIDs, err := r.findDeadPools()
	if err != nil {
		return err
	}

	conn := r.pool.Get()
	defer conn.Close()

	workerPoolsKey := r.commander.KeyWorkerPools(r.namespace)

	// Cleanup all dead pools
	for deadPoolID, jobTypes := range deadPoolIDs {
		lockJobTypes := jobTypes
		// if we found jobs from the heartbeat, requeue them and remove the heartbeat
		if len(jobTypes) > 0 {
			r.requeueInProgressJobs(deadPoolID, jobTypes)
			if _, err = conn.Do("DEL", r.commander.KeyHeartbeat(r.namespace, deadPoolID)); err != nil {
				return err
			}
		} else {
			// try to clean up locks for the current set of jobs if heartbeat was not found
			lockJobTypes = r.curJobTypes
		}
		// Remove dead pool from worker pools set
		if _, err = conn.Do("SREM", workerPoolsKey, deadPoolID); err != nil {
			return err
		}
		// Cleanup any stale lock info
		if err = r.cleanStaleLockInfo(deadPoolID, lockJobTypes); err != nil {
			return err
		}
	}

	return nil
}

func (r *deadPoolReaper) cleanStaleLockInfo(poolID string, jobTypes []string) error {
	numKeys := len(jobTypes) * 2
	redisReapLocksScript := redis.NewScript(numKeys, r.commander.ReapStaleLocks())
	var scriptArgs = make([]interface{}, 0, numKeys+1) // +1 for argv[1]

	for _, jobType := range jobTypes {
		scriptArgs = append(scriptArgs, r.commander.KeyJobsLock(r.namespace, jobType), r.commander.KeyJobsLockInfo(r.namespace, jobType))
	}
	scriptArgs = append(scriptArgs, poolID) // ARGV[1]

	conn := r.pool.Get()
	defer conn.Close()
	if _, err := redisReapLocksScript.Do(conn, scriptArgs...); err != nil {
		return err
	}

	return nil
}

func (r *deadPoolReaper) requeueInProgressJobs(poolID string, jobTypes []string) error {
	numKeys := len(jobTypes) * requeueKeysPerJob
	redisRequeueScript := redis.NewScript(numKeys, r.commander.ReenqueueJob())
	var scriptArgs = make([]interface{}, 0, numKeys+1)

	for _, jobType := range jobTypes {
		// pops from in progress, push into job queue and decrement the queue lock
		scriptArgs = append(scriptArgs, r.commander.KeyJobsInProgress(r.namespace, poolID, jobType), r.commander.KeyJobs(r.namespace, jobType), redisKeyJobsLock(r.namespace, jobType), redisKeyJobsLockInfo(r.namespace, jobType)) // KEYS[1-4 * N]
	}
	scriptArgs = append(scriptArgs, poolID) // ARGV[1]

	conn := r.pool.Get()
	defer conn.Close()

	// Keep moving jobs until all queues are empty
	for {
		values, err := redis.Values(redisRequeueScript.Do(conn, scriptArgs...))
		if err == redis.ErrNil {
			return nil
		} else if err != nil {
			return err
		}

		if len(values) != 3 {
			return fmt.Errorf("need 3 elements back")
		}
	}
}

func (r *deadPoolReaper) findDeadPools() (map[string][]string, error) {
	conn := r.pool.Get()
	defer conn.Close()

	workerPoolsKey := r.commander.KeyWorkerPools(r.namespace)

	workerPoolIDs, err := redis.Strings(conn.Do("SMEMBERS", workerPoolsKey))
	if err != nil {
		return nil, err
	}

	deadPools := map[string][]string{}
	for _, workerPoolID := range workerPoolIDs {
		heartbeatKey := r.commander.KeyHeartbeat(r.namespace, workerPoolID)
		heartbeatAt, err := redis.Int64(conn.Do("HGET", heartbeatKey, "heartbeat_at"))
		if err == redis.ErrNil {
			// heartbeat expired, save dead pool and use cur set of jobs from reaper
			deadPools[workerPoolID] = []string{}
			continue
		}
		if err != nil {
			return nil, err
		}

		// Check that last heartbeat was long enough ago to consider the pool dead
		if time.Unix(heartbeatAt, 0).Add(r.deadTime).After(time.Now()) {
			continue
		}

		jobTypesList, err := redis.String(conn.Do("HGET", heartbeatKey, "job_names"))
		if err == redis.ErrNil {
			continue
		}
		if err != nil {
			return nil, err
		}

		deadPools[workerPoolID] = strings.Split(jobTypesList, ",")
	}

	return deadPools, nil
}
