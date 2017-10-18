package work

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type RedisDBCommand struct {}

func (r *RedisDBCommand) NamespacePrefix(namespace string) string {
	l := len(namespace)
	if (l > 0) && (namespace[l-1] != ':') {
		namespace = namespace + ":"
	}
	return namespace
}

func (r *RedisDBCommand) KeyKnownJobs(namespace string) string {
	return r.NamespacePrefix(namespace) + "known_jobs"
}

// returns "<namespace>:jobs:"
// so that we can just append the job name and be good to go
func (r *RedisDBCommand) KeyJobsPrefix(namespace string) string {
	return r.NamespacePrefix(namespace) + "jobs:"
}

func (r *RedisDBCommand) KeyJobs(namespace, jobName string) string {
	return r.KeyJobsPrefix(namespace) + jobName
}

func (r *RedisDBCommand) KeyJobsInProgress(namespace, poolID, jobName string) string {
	return fmt.Sprintf("%s:%s:inprogress", r.KeyJobs(namespace, jobName), poolID)
}

func (r *RedisDBCommand) KeyRetry(namespace string) string {
	return r.NamespacePrefix(namespace) + "retry"
}

func (r *RedisDBCommand) KeyDead(namespace string) string {
	return r.NamespacePrefix(namespace) + "dead"
}

func (r *RedisDBCommand) KeyScheduled(namespace string) string {
	return r.NamespacePrefix(namespace) + "scheduled"
}

func (r *RedisDBCommand) KeyWorkerObservation(namespace, workerID string) string {
	return r.NamespacePrefix(namespace) + "worker:" + workerID
}

func (r *RedisDBCommand) KeyWorkerPools(namespace string) string {
	return r.NamespacePrefix(namespace) + "worker_pools"
}

func (r *RedisDBCommand) KeyHeartbeat(namespace, workerPoolID string) string {
	return r.NamespacePrefix(namespace) + "worker_pools:" + workerPoolID
}

func (r *RedisDBCommand) KeyJobsPaused(namespace, jobName string) string {
	return r.KeyJobs(namespace, jobName) + ":paused"
}

func (r *RedisDBCommand) KeyJobsLock(namespace, jobName string) string {
	return r.KeyJobs(namespace, jobName) + ":lock"
}

func (r *RedisDBCommand) KeyJobsLockInfo(namespace, jobName string) string {
	return r.KeyJobs(namespace, jobName) + ":lock_info"
}

func (r *RedisDBCommand) KeyJobsConcurrency(namespace, jobName string) string {
	return r.KeyJobs(namespace, jobName) + ":max_concurrency"
}

func (r *RedisDBCommand) KeyUniqueJob(namespace, jobName string, args map[string]interface{}) (string, error) {
	var buf bytes.Buffer

	buf.WriteString(r.NamespacePrefix(namespace))
	buf.WriteString("unique:")
	buf.WriteString(jobName)
	buf.WriteRune(':')

	if args != nil {
		err := json.NewEncoder(&buf).Encode(args)
		if err != nil {
			return "", err
		}
	}

	return buf.String(), nil
}

func (r *RedisDBCommand) KeyLastPeriodicEnqueue(namespace string) string {
	return r.NamespacePrefix(namespace) + "last_periodic_enqueue"
}

// Used to fetch the next job to run
//
// KEYS[1] = the 1st job queue we want to try, eg, "work:jobs:emails"
// KEYS[2] = the 1st job queue's in prog queue, eg, "work:jobs:emails:97c84119d13cb54119a38743:inprogress"
// KEYS[3] = the 2nd job queue...
// KEYS[4] = the 2nd job queue's in prog queue...
// ...
// KEYS[N] = the last job queue...
// KEYS[N+1] = the last job queue's in prog queue...
// ARGV[1] = job queue's workerPoolID
func (r *RedisDBCommand) FetchJob() string {
	return fmt.Sprintf(`
local function acquireLock(lockKey, lockInfoKey, workerPoolID)
  redis.call('incr', lockKey)
  redis.call('hincrby', lockInfoKey, workerPoolID, 1)
end

local function haveJobs(jobQueue)
  return redis.call('llen', jobQueue) > 0
end

local function isPaused(pauseKey)
  return redis.call('get', pauseKey)
end

local function canRun(lockKey, maxConcurrency)
  local activeJobs = tonumber(redis.call('get', lockKey))
  if (not maxConcurrency or maxConcurrency == 0) or (not activeJobs or activeJobs < maxConcurrency) then
    -- default case: maxConcurrency not defined or set to 0 means no cap on concurrent jobs OR
    -- maxConcurrency set, but lock does not yet exist OR
    -- maxConcurrency set, lock is set, but not yet at max concurrency
    return true
  else
    -- we are at max capacity for running jobs
    return false
  end
end

local res, jobQueue, inProgQueue, pauseKey, lockKey, maxConcurrency, workerPoolID, concurrencyKey, lockInfoKey
local keylen = #KEYS
workerPoolID = ARGV[1]

for i=1,keylen,%d do
  jobQueue = KEYS[i]
  inProgQueue = KEYS[i+1]
  pauseKey = KEYS[i+2]
  lockKey = KEYS[i+3]
  lockInfoKey = KEYS[i+4]
  concurrencyKey = KEYS[i+5]

  maxConcurrency = tonumber(redis.call('get', concurrencyKey))

  if haveJobs(jobQueue) and not isPaused(pauseKey) and canRun(lockKey, maxConcurrency) then
    acquireLock(lockKey, lockInfoKey, workerPoolID)
    res = redis.call('rpoplpush', jobQueue, inProgQueue)
    return {res, jobQueue, inProgQueue}
  end
end
return nil`, fetchKeysPerJobType)
}

// Used by the reaper to re-enqueue jobs that were in progress
//
// KEYS[1] = the 1st job's in progress queue
// KEYS[2] = the 1st job's job queue
// KEYS[3] = the 2nd job's in progress queue
// KEYS[4] = the 2nd job's job queue
// ...
// KEYS[N] = the last job's in progress queue
// KEYS[N+1] = the last job's job queue
// ARGV[1] = workerPoolID for job queue
func (r *RedisDBCommand) ReenqueueJob() string {
	return fmt.Sprintf(`
local function releaseLock(lockKey, lockInfoKey, workerPoolID)
  redis.call('decr', lockKey)
  redis.call('hincrby', lockInfoKey, workerPoolID, -1)
end

local keylen = #KEYS
local res, jobQueue, inProgQueue, workerPoolID, lockKey, lockInfoKey
workerPoolID = ARGV[1]

for i=1,keylen,%d do
  inProgQueue = KEYS[i]
  jobQueue = KEYS[i+1]
  lockKey = KEYS[i+2]
  lockInfoKey = KEYS[i+3]
  res = redis.call('rpoplpush', inProgQueue, jobQueue)
  if res then
    releaseLock(lockKey, lockInfoKey, workerPoolID)
    return {res, inProgQueue, jobQueue}
  end
end
return nil`, requeueKeysPerJob)
}

// Used by the reaper to clean up stale locks
//
// KEYS[1] = the 1st job's lock
// KEYS[2] = the 1st job's lock info hash
// KEYS[3] = the 2nd job's lock
// KEYS[4] = the 2nd job's lock info hash
// ...
// KEYS[N] = the last job's lock
// KEYS[N+1] = the last job's lock info haash
// ARGV[1] = the dead worker pool id
func (r *RedisDBCommand) ReapStaleLocks() string {
	return `
local keylen = #KEYS
local lock, lockInfo, deadLockCount
local deadPoolID = ARGV[1]

for i=1,keylen,2 do
  lock = KEYS[i]
  lockInfo = KEYS[i+1]
  deadLockCount = tonumber(redis.call('hget', lockInfo, deadPoolID))

  if deadLockCount then
    redis.call('decrby', lock, deadLockCount)
    redis.call('hdel', lockInfo, deadPoolID)

    if tonumber(redis.call('get', lock)) < 0 then
      redis.call('set', lock, 0)
    end
  end
end
return nil
`
}

// KEYS[1] = zset of jobs (retry or scheduled), eg work:retry
// KEYS[2] = zset of dead, eg work:dead. If we don't know the jobName of a job, we'll put it in dead.
// KEYS[3...] = known job queues, eg ["work:jobs:create_watch", "work:jobs:send_email", ...]
// ARGV[1] = jobs prefix, eg, "work:jobs:". We'll take that and append the job name from the JSON object in order to queue up a job
// ARGV[2] = current time in epoch seconds
func (r *RedisDBCommand) ZremLpushCmd() string {
	return `
	local res, j, queue
	res = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[2], 'LIMIT', 0, 1)
	if #res > 0 then
		j = cjson.decode(res[1])
		redis.call('zrem', KEYS[1], res[1])
		queue = ARGV[1] .. j['name']
		for _,v in pairs(KEYS) do
			if v == queue then
				j['t'] = tonumber(ARGV[2])
				redis.call('lpush', queue, cjson.encode(j))
				return 'ok'
			end
		end
		j['err'] = 'unknown job when requeueing'
		j['failed_at'] = tonumber(ARGV[2])
		redis.call('zadd', KEYS[2], ARGV[2], cjson.encode(j))
		return 'dead' -- put on dead queue
	end
	return nil
	`
}

// KEYS[1] = zset of (dead|scheduled|retry), eg, work:dead
// ARGV[1] = died at. The z rank of the job.
// ARGV[2] = job ID to requeue
// Returns:
// - number of jobs deleted (typically 1 or 0)
// - job bytes (last job only)
func (r *RedisDBCommand) DeleteSingleCmd() string {
	return `
local jobs, i, j, deletedCount, jobBytes
jobs = redis.call('zrangebyscore', KEYS[1], ARGV[1], ARGV[1])
local jobCount = #jobs
jobBytes = ''
deletedCount = 0
for i=1,jobCount do
  j = cjson.decode(jobs[i])
  if j['id'] == ARGV[2] then
    redis.call('zrem', KEYS[1], jobs[i])
    deletedCount = deletedCount + 1
    jobBytes = jobs[i]
  end
end
return {deletedCount, jobBytes}
`
}

// KEYS[1] = zset of dead jobs, eg, work:dead
// KEYS[2...] = known job queues, eg ["work:jobs:create_watch", "work:jobs:send_email", ...]
// ARGV[1] = jobs prefix, eg, "work:jobs:". We'll take that and append the job name from the JSON object in order to queue up a job
// ARGV[2] = current time in epoch seconds
// ARGV[3] = died at. The z rank of the job.
// ARGV[4] = job ID to requeue
// Returns: number of jobs requeued (typically 1 or 0)
func (r *RedisDBCommand) RequeueSingleDeadCmd() string {
	return `
local jobs, i, j, queue, found, requeuedCount
jobs = redis.call('zrangebyscore', KEYS[1], ARGV[3], ARGV[3])
local jobCount = #jobs
requeuedCount = 0
for i=1,jobCount do
  j = cjson.decode(jobs[i])
  if j['id'] == ARGV[4] then
    redis.call('zrem', KEYS[1], jobs[i])
    queue = ARGV[1] .. j['name']
    found = false
    for _,v in pairs(KEYS) do
      if v == queue then
        j['t'] = tonumber(ARGV[2])
        j['fails'] = nil
        j['failed_at'] = nil
        j['err'] = nil
        redis.call('lpush', queue, cjson.encode(j))
        requeuedCount = requeuedCount + 1
        found = true
        break
      end
    end
    if not found then
      j['err'] = 'unknown job when requeueing'
      j['failed_at'] = tonumber(ARGV[2])
      redis.call('zadd', KEYS[1], ARGV[2] + 5, cjson.encode(j))
    end
  end
end
return requeuedCount
`
}

// KEYS[1] = zset of dead jobs, eg work:dead
// KEYS[2...] = known job queues, eg ["work:jobs:create_watch", "work:jobs:send_email", ...]
// ARGV[1] = jobs prefix, eg, "work:jobs:". We'll take that and append the job name from the JSON object in order to queue up a job
// ARGV[2] = current time in epoch seconds
// ARGV[3] = max number of jobs to requeue
// Returns: number of jobs requeued
func (r *RedisDBCommand) RequeueAllDeadCmd() string {
	return `
local jobs, i, j, queue, found, requeuedCount
jobs = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[2], 'LIMIT', 0, ARGV[3])
local jobCount = #jobs
requeuedCount = 0
for i=1,jobCount do
  j = cjson.decode(jobs[i])
  redis.call('zrem', KEYS[1], jobs[i])
  queue = ARGV[1] .. j['name']
  found = false
  for _,v in pairs(KEYS) do
    if v == queue then
      j['t'] = tonumber(ARGV[2])
      j['fails'] = nil
      j['failed_at'] = nil
      j['err'] = nil
      redis.call('lpush', queue, cjson.encode(j))
      requeuedCount = requeuedCount + 1
      found = true
      break
    end
  end
  if not found then
    j['err'] = 'unknown job when requeueing'
    j['failed_at'] = tonumber(ARGV[2])
    redis.call('zadd', KEYS[1], ARGV[2] + 5, cjson.encode(j))
  end
end
return requeuedCount
`
}

// KEYS[1] = job queue to push onto
// KEYS[2] = Unique job's key. Test for existence and set if we push.
// ARGV[1] = job
func (r *RedisDBCommand) EnqueueUnique() string {
	return `
	if redis.call('set', KEYS[2], '1', 'NX', 'EX', '86400') then
		redis.call('lpush', KEYS[1], ARGV[1])
		return 'ok'
	end
	return 'dup'
	`
}

// KEYS[1] = scheduled job queue
// KEYS[2] = Unique job's key. Test for existence and set if we push.
// ARGV[1] = job
// ARGV[2] = epoch seconds for job to be run at
func (r *RedisDBCommand) EnqueueUniqueIn() string {
	return `
	if redis.call('set', KEYS[2], '1', 'NX', 'EX', '86400') then
		redis.call('zadd', KEYS[1], ARGV[2], ARGV[1])
		return 'ok'
	end
	return 'dup'
	`
}
