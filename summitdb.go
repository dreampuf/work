package work

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type SummitDBCommand struct {
}

func (s *SummitDBCommand) NamespacePrefix(namespace string) string {
	l := len(namespace)
	if (l > 0) && (namespace[l-1] != ':') {
		namespace = namespace + ":"
	}
	return namespace
}

func (s *SummitDBCommand) KeyKnownJobs(namespace string) string {
	return s.NamespacePrefix(namespace) + "known_jobs"
}

// returns "<namespace>:jobs:"
// so that we can just append the job name and be good to go
func (s *SummitDBCommand) KeyJobsPrefix(namespace string) string {
	return s.NamespacePrefix(namespace) + "jobs:"
}

func (s *SummitDBCommand) KeyJobs(namespace, jobName string) string {
	return s.KeyJobsPrefix(namespace) + jobName
}

func (s *SummitDBCommand) KeyJobsInProgress(namespace, poolID, jobName string) string {
	return fmt.Sprintf("%s:%s:inprogress", s.KeyJobs(namespace, jobName), poolID)
}

func (s *SummitDBCommand) KeyRetry(namespace string) string {
	return s.NamespacePrefix(namespace) + "retry"
}

func (s *SummitDBCommand) KeyDead(namespace string) string {
	return s.NamespacePrefix(namespace) + "dead"
}

func (s *SummitDBCommand) KeyScheduled(namespace string) string {
	return s.NamespacePrefix(namespace) + "scheduled"
}

func (s *SummitDBCommand) KeyWorkerObservation(namespace, workerID string) string {
	return s.NamespacePrefix(namespace) + "worker:" + workerID
}

func (s *SummitDBCommand) KeyWorkerPools(namespace string) string {
	return s.NamespacePrefix(namespace) + "worker_pools"
}

func (s *SummitDBCommand) KeyHeartbeat(namespace, workerPoolID string) string {
	return s.NamespacePrefix(namespace) + "worker_pools:" + workerPoolID
}

func (s *SummitDBCommand) KeyUniqueJob(namespace, jobName string, args map[string]interface{}) (string, error) {
	var buf bytes.Buffer

	buf.WriteString(s.NamespacePrefix(namespace))
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

func (s *SummitDBCommand) KeyLastPeriodicEnqueue(namespace string) string {
	return s.NamespacePrefix(namespace) + "last_periodic_enqueue"
}

// KEYS[1] = the 1st job queue we want to try, eg, "work:jobs:emails"
// KEYS[2] = the 1st job queue's in prog queue, eg, "work:jobs:emails:97c84119d13cb54119a38743:inprogress"
// KEYS[3] = the 2nd job queue...
// KEYS[4] = the 2nd job queue's in prog queue...
// ...
// KEYS[N] = the last job queue...
// KEYS[N+1] = the last job queue's in prog queue...
func (s *SummitDBCommand) RpoplpushMultiCmd() string {
	return `
//console.log("RpoplpushMultiCmd", KEYS, ARGV);
var res, keylen = KEYS.length;
for (var i=0; i < keylen; i+=2) {
  var res = sdb.call('rpoplpush', KEYS[i], KEYS[i+1]);
  if (res) {
    return [res, KEYS[i], KEYS[i+1]];
	}
}
return null;
`
}

// KEYS[1] = zset of jobs (retry or scheduled), eg work:retry
// KEYS[2] = zset of dead, eg work:dead. If we don't know the jobName of a job, we'll put it in dead.
// KEYS[3...] = known job queues, eg ["work:jobs:create_watch", "work:jobs:send_email", ...]
// ARGV[1] = jobs prefix, eg, "work:jobs:". We'll take that and append the job name from the JSON object in order to queue up a job
// ARGV[2] = current time in epoch seconds
func (s *SummitDBCommand) ZremLpushCmd() string {
	return `
//console.log("ZremLpushCmd", KEYS, ARGV);
	var res, j, queue;
	res = redis.call('zrangebyscore', KEYS[0], '-inf', ARGV[1], 'LIMIT', 0, 1);
	if (res.length > 0) {
		j = JSON.parse(res[1]);
		redis.call('zrem', KEYS[0], res[1]);
		queue = ARGV[0] + j['name'];
		for (var i = 0; i < KEYS.length; i ++) {
	    var v = KEYS[i];
			if (v == queue) {
				j['t'] = parseFloat(ARGV[1]);
				redis.call('lpush', queue, JSON.stringify(j));
				return 'ok';
	    }
		}
		j['err'] = 'unknown job when requeueing';
		j['failed_at'] = parseFloat(ARGV[1]);
		redis.call('zadd', KEYS[1], ARGV[1], JSON.stringify(j));
		return 'dead'; // put on dead queue
	}
	return null;
	`
}

// KEYS[1] = zset of (dead|scheduled|retry), eg, work:dead
// ARGV[1] = died at. The z rank of the job.
// ARGV[2] = job ID to requeue
// Returns:
// - number of jobs deleted (typically 1 or 0)
// - job bytes (last job only)
func (s *SummitDBCommand) DeleteSingleCmd() string {
	return `
console.log("DeleteSingleCmd", KEYS, ARGV);
	var jobs, i, j, deletedCount, jobBytes;
	jobs = redis.call('zrangebyscore', KEYS[0], ARGV[0], ARGV[0]);
	var jobCount = jobs.length;
	jobBytes = '';
	deletedCount = 0;
	for (var i=1; i < jobCount; i++) {
		j = JSON.parse(jobs[i]);
		if (j['id'] == ARGV[1]) {
			redis.call('zrem', KEYS[0], jobs[i]);
		  deletedCount = deletedCount + 1;
		  jobBytes = jobs[i];
	  }
	}
	return [deletedCount, jobBytes];
	`
}

// KEYS[1] = zset of dead jobs, eg, work:dead
// KEYS[2...] = known job queues, eg ["work:jobs:create_watch", "work:jobs:send_email", ...]
// ARGV[1] = jobs prefix, eg, "work:jobs:". We'll take that and append the job name from the JSON object in order to queue up a job
// ARGV[2] = current time in epoch seconds
// ARGV[3] = died at. The z rank of the job.
// ARGV[4] = job ID to requeue
// Returns: number of jobs requeued (typically 1 or 0)
func (s *SummitDBCommand) RequeueSingleDeadCmd() string {
	return `
console.log("RequeueSingleDeadCmd", KEYS, ARGV);
var jobs, i, j, queue, found, requeuedCount;
jobs = redis.call('zrangebyscore', KEYS[0], ARGV[2], ARGV[2]);
var jobCount = jobs.length;
requeuedCount = 0;
for (var i=0; i < jobCount; i++) {
	j = JSON.prase(jobs[i])
	if (j['id'] == ARGV[3]) {
		redis.call('zrem', KEYS[0], jobs[i]);
		queue = ARGV[0] + j['name'];
		found = false;
		for (var i = 0; i < KEYS.length; i ++) {
	    var v = KEYS[i];
			if (v == queue) {
				j['t'] = parseFloat(ARGV[1]);
				j['fails'] = nil;
				j['failed_at'] = nil;
				j['err'] = nil;
				redis.call('lpush', queue, JSON.stringify(j));
				requeuedCount = requeuedCount + 1;
				found = true;
				break;
	    }
	  }
		if (!found) {
			j['err'] = 'unknown job when requeueing';
			j['failed_at'] = parseFloat(ARGV[1]);
			redis.call('zadd', KEYS[0], ARGV[1] + 5, JSON.stringify(j));
	  }
	}
}
return requeuedCount;`
}

// KEYS[1] = zset of dead jobs, eg work:dead
// KEYS[2...] = known job queues, eg ["work:jobs:create_watch", "work:jobs:send_email", ...]
// ARGV[1] = jobs prefix, eg, "work:jobs:". We'll take that and append the job name from the JSON object in order to queue up a job
// ARGV[2] = current time in epoch seconds
// ARGV[3] = max number of jobs to requeue
// Returns: number of jobs requeued
func (s *SummitDBCommand) RequeueAllDeadCmd() string {
	return `
console.log("RequeueAllDeadCmd", KEYS, ARGV);
	var jobs, i, j, queue, found, requeuedCount;
	jobs = redis.call('zrangebyscore', KEYS[0], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2]);
	var jobCount = jobs.length;
	requeuedCount = 0;
	for (var i=0; i < jobCount; i ++) {
		j = JSON.parse(jobs[i]);
		redis.call('zrem', KEYS[0], jobs[i]);
		queue = ARGV[0] + j['name'];
		found = false;
		for (var i = 0; i < KEYS.length; i ++) {
	    var v = KEYS[i];
			if (v == queue) {
				j['t'] = parseFloat(ARGV[1]);
				j['fails'] = null;
				j['failed_at'] = null;
				j['err'] = null;
				redis.call('lpush', queue, JSON.stringify(j));
				requeuedCount = requeuedCount + 1;
				found = true;
				break;
	    }
	  }
		if (!found) {
			j['err'] = 'unknown job when requeueing';
			j['failed_at'] = parseFloat(ARGV[1]);
			redis.call('zadd', KEYS[0], ARGV[1] + 5, JSON.stringify(j));
	  }
	}
	return requeuedCount;
	`
}

// KEYS[1] = job queue to push onto
// KEYS[2] = Unique job's key. Test for existance and set if we push.
// ARGV[1] = job
func (s *SummitDBCommand) EnqueueUnique() string {
	return `
console.log("EnqueueUnique", KEYS, ARGV);
if (redis.call('set', KEYS[1], '1', 'NX', 'EX', '86400')) {
  redis.call('lpush', KEYS[0], ARGV[0]);
  return 'ok';
}
return 'dup';
`
}

// KEYS[1] = scheduled job queue
// KEYS[2] = Unique job's key. Test for existance and set if we push.
// ARGV[1] = job
// ARGV[2] = epoch seconds for job to be run at
func (s *SummitDBCommand) EnqueueUniqueIn() string {
	return `
console.log("EnqueueUniqueIn", KEYS, ARGV);
if (redis.call('set', KEYS[1], '1', 'NX', 'EX', '86400')) {
  redis.call('zadd', KEYS[0], ARGV[1], ARGV[0]);
  return 'ok';
}
return 'dup';
`
}
