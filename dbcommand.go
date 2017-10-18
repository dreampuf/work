package work

type DBCommand interface {
	NamespacePrefix(namespace string) string
	KeyKnownJobs(namespace string) string
	// returns "<namespace>:jobs:"
	// so that we can just append the job name and be good to go
	KeyJobsPrefix(namespace string) string
	KeyJobs(namespace, jobName string) string
	KeyJobsInProgress(namespace, poolID, jobName string) string
	KeyRetry(namespace string) string
	KeyDead(namespace string) string
	KeyScheduled(namespace string) string
	KeyWorkerObservation(namespace, workerID string) string
	KeyWorkerPools(namespace string) string
	KeyHeartbeat(namespace, workerPoolID string) string
	KeyJobsPaused(namespace, jobName string) string
	KeyJobsLock(namespace, jobName string) string
	KeyJobsLockInfo(namespace, jobName string) string
	KeyJobsConcurrency(namespace, jobName string) string
	KeyUniqueJob(namespace, jobName string, args map[string]interface{}) (string, error)
	KeyLastPeriodicEnqueue(namespace string) string
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
	FetchJob() string
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
	ReenqueueJob() string
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
	ReapStaleLocks() string
	// KEYS[1] = zset of jobs (retry or scheduled), eg work:retry
	// KEYS[2] = zset of dead, eg work:dead. If we don't know the jobName of a job, we'll put it in dead.
	// KEYS[3...] = known job queues, eg ["work:jobs:create_watch", "work:jobs:send_email", ...]
	// ARGV[1] = jobs prefix, eg, "work:jobs:". We'll take that and append the job name from the JSON object in order to queue up a job
	// ARGV[2] = current time in epoch seconds
	ZremLpushCmd() string
	// KEYS[1] = zset of (dead|scheduled|retry), eg, work:dead
	// ARGV[1] = died at. The z rank of the job.
	// ARGV[2] = job ID to requeue
	// Returns:
	// - number of jobs deleted (typically 1 or 0)
	// - job bytes (last job only)
	DeleteSingleCmd() string
	// KEYS[1] = zset of dead jobs, eg, work:dead
	// KEYS[2...] = known job queues, eg ["work:jobs:create_watch", "work:jobs:send_email", ...]
	// ARGV[1] = jobs prefix, eg, "work:jobs:". We'll take that and append the job name from the JSON object in order to queue up a job
	// ARGV[2] = current time in epoch seconds
	// ARGV[3] = died at. The z rank of the job.
	// ARGV[4] = job ID to requeue
	// Returns: number of jobs requeued (typically 1 or 0)
	RequeueSingleDeadCmd() string
	// KEYS[1] = zset of dead jobs, eg work:dead
	// KEYS[2...] = known job queues, eg ["work:jobs:create_watch", "work:jobs:send_email", ...]
	// ARGV[1] = jobs prefix, eg, "work:jobs:". We'll take that and append the job name from the JSON object in order to queue up a job
	// ARGV[2] = current time in epoch seconds
	// ARGV[3] = max number of jobs to requeue
	// Returns: number of jobs requeued
	RequeueAllDeadCmd() string
	// KEYS[1] = job queue to push onto
	// KEYS[2] = Unique job's key. Test for existence and set if we push.
	// ARGV[1] = job
	EnqueueUnique() string
	// KEYS[1] = scheduled job queue
	// KEYS[2] = Unique job's key. Test for existence and set if we push.
	// ARGV[1] = job
	// ARGV[2] = epoch seconds for job to be run at
	EnqueueUniqueIn() string
}

