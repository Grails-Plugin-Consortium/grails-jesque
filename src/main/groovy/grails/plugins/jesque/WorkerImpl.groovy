/*
 * Copyright 2011 Greg Haines
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package grails.plugins.jesque

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonMappingException
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import net.greghaines.jesque.Config
import net.greghaines.jesque.Job
import net.greghaines.jesque.JobFailure
import net.greghaines.jesque.WorkerStatus
import net.greghaines.jesque.json.ObjectMapperFactory
import net.greghaines.jesque.utils.JedisUtils
import net.greghaines.jesque.utils.JesqueUtils
import net.greghaines.jesque.utils.ResqueConstants
import net.greghaines.jesque.utils.VersionUtils
import net.greghaines.jesque.worker.*
import redis.clients.jedis.Jedis
import redis.clients.jedis.Transaction
import redis.clients.jedis.Tuple
import redis.clients.jedis.exceptions.JedisException
import redis.clients.util.Pool

import java.lang.management.ManagementFactory
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.concurrent.BlockingDeque
import java.util.concurrent.Callable
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * WorkerImpl is an implementation of the Worker interface. Obeys the contract of a Resque worker in Redis.
 */
@Slf4j
@CompileStatic
public class WorkerImpl implements Worker {

    private static final AtomicLong WORKER_COUNTER = new AtomicLong(0)
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat(ResqueConstants.DATE_FORMAT)
    protected static final long EMPTY_QUEUE_SLEEP_TIME = 500; // 500 ms

    /**
     * Enable/disable worker thread renaming during normal operation. (Disabled by default)<p>
     * <strong>Warning: Enabling this feature is very expensive CPU-wise!</strong><br>
     * This feature is designed to assist in debugging worker state but should
     * be disabled in production environments for performance reasons.</p>
     */
    static volatile boolean threadNameChangingEnabled = false

    protected final Config config
    protected final Pool<Jedis> jedisPool
    protected final String namespace
    protected final BlockingDeque<String> queueNames = new LinkedBlockingDeque<String>()
    protected final WorkerListenerDelegate listenerDelegate = new WorkerListenerDelegate()
    protected final AtomicReference<JobExecutor.State> state = new AtomicReference<JobExecutor.State>(JobExecutor.State.NEW)

    private final String name
    private final AtomicBoolean paused = new AtomicBoolean(false)
    private final AtomicBoolean processingJob = new AtomicBoolean(false)
    private final long workerId = WORKER_COUNTER.getAndIncrement()
    private final String threadNameBase = "Worker-${workerId} Jesque-${VersionUtils.getVersion()}:"
    private final AtomicReference<Thread> threadRef = new AtomicReference<Thread>(null)
    private final AtomicReference<ExceptionHandler> exceptionHandlerRef = new AtomicReference<ExceptionHandler>(new DefaultExceptionHandler())
    private final AtomicReference<FailQueueStrategy> failQueueStrategyRef
    private final JobFactory jobFactory

    /**
     * Creates a new WorkerImpl, with the given connection to Redis.<br>
     * The worker will only listen to the supplied queues and execute jobs that are provided by the given job factory.
     *
     * @param config used to create a connection to Redis and the package prefix for incoming jobs
     * @param queues the list of queues to poll
     * @param jobFactory the job factory that materializes the jobs
     * @param jedisPool the connection to Redis
     * @throws IllegalArgumentException if either config, queues, jobFactory or jedisPool is null
     */
    public WorkerImpl(final Config config, final Collection<String> queues, final JobFactory jobFactory, final Pool<Jedis> jedisPool) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null")
        }
        if (jobFactory == null) {
            throw new IllegalArgumentException("jobFactory must not be null")
        }
        if (jedisPool == null) {
            throw new IllegalArgumentException("jedisPool must not be null")
        }
        checkQueues(queues)
        this.config = config
        this.jobFactory = jobFactory
        this.namespace = config.namespace
        this.jedisPool = jedisPool
        this.failQueueStrategyRef = new AtomicReference<FailQueueStrategy>(new DefaultFailQueueStrategy(this.namespace))
        setQueues(queues)
        this.name = createName()
    }

    /**
     * Verify that the given queues are all valid.
     *
     * @param queues the given queues
     */
    protected static void checkQueues(final Iterable<String> queues) {
        if (queues == null) {
            throw new IllegalArgumentException("queues must not be null")
        }
        if (queues.any { queue -> !queue }) {
            throw new IllegalArgumentException("queues' members must not be null: " + queues)
        }
    }

    /**
     * Starts this worker. Registers the worker in Redis and begins polling the queues for jobs.<br>
     * Stop this worker by calling end() on any thread.
     */
    @Override
    public void run() {
        if (this.state.compareAndSet(JobExecutor.State.NEW, JobExecutor.State.RUNNING)) {
            withJedis { Jedis jedis ->
                try {
                    renameThread("RUNNING")
                    this.threadRef.set(Thread.currentThread())
                    jedis.sadd(key(ResqueConstants.WORKERS), this.name)
                    jedis.set(key(ResqueConstants.WORKER, this.name, ResqueConstants.STARTED), DATE_FORMAT.format(new Date()))
                    this.listenerDelegate.fireEvent(WorkerEvent.WORKER_START, this, null, null, null, null, null)
                    poll()
                } finally {
                    renameThread("STOPPING")
                    this.listenerDelegate.fireEvent(WorkerEvent.WORKER_STOP, this, null, null, null, null, null)
                    jedis.srem(key(ResqueConstants.WORKERS), this.name)
                    jedis.del(key(ResqueConstants.WORKER, this.name), key(ResqueConstants.WORKER, this.name, ResqueConstants.STARTED), key(ResqueConstants.STAT, ResqueConstants.FAILED, this.name),
                            key(ResqueConstants.STAT, ResqueConstants.PROCESSED, this.name))
                    this.returnJedis(jedis)
                    this.threadRef.set(null)
                }
            }
        } else {
            if (this.running) {
                throw new IllegalStateException("This WorkerImpl is already running")
            } else {
                throw new IllegalStateException("This WorkerImpl is shutdown")
            }
        }
    }

    /**
     * Shutdown this Worker.<br>
     * <b>The worker cannot be started again; create a new worker in this case.</b>
     *
     * @param now if true, an effort will be made to stop any job in progress
     */
    @Override
    public void end(final boolean now) {
        if (now) {
            this.state.set(JobExecutor.State.SHUTDOWN_IMMEDIATE)
            final Thread workerThread = this.threadRef.get()
            if (workerThread != null) {
                workerThread.interrupt()
            }
        } else {
            this.state.set(JobExecutor.State.SHUTDOWN)
        }
        togglePause(false); // Release any threads waiting in checkPaused()
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isShutdown() {
        return this.state.get() in [JobExecutor.State.SHUTDOWN, JobExecutor.State.SHUTDOWN_IMMEDIATE]
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isPaused() {
        return this.paused.get()
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isProcessingJob() {
        return this.processingJob.get()
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void togglePause(final boolean paused) {
        this.paused.set(paused)
        synchronized (this.paused) {
            this.paused.notifyAll()
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return this.name
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public WorkerEventEmitter getWorkerEventEmitter() {
        return this.listenerDelegate
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<String> getQueues() {
        return Collections.unmodifiableCollection(this.queueNames)
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addQueue(final String queueName) {
        if (!queueName) {
            throw new IllegalArgumentException("queueName must not be null or empty: " + queueName)
        }
        this.queueNames.add(queueName)
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeQueue(final String queueName, final boolean all) {
        if (!queueName) {
            throw new IllegalArgumentException("queueName must not be null or empty: " + queueName)
        }
        if (all) { // Remove all instances
            boolean tryAgain = true
            while (tryAgain) {
                tryAgain = this.queueNames.remove(queueName)
            }
        } else { // Only remove one instance
            this.queueNames.remove(queueName)
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllQueues() {
        this.queueNames.clear()
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setQueues(final Collection<String> queues) {
        checkQueues(queues)
        this.queueNames.clear()
        withJedis { Jedis jedis ->
            this.queueNames.addAll(queues == ALL_QUEUES ? jedis.smembers(key(ResqueConstants.QUEUES)) : queues)
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobFactory getJobFactory() {
        return this.jobFactory
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExceptionHandler getExceptionHandler() {
        return this.exceptionHandlerRef.get()
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setExceptionHandler(final ExceptionHandler exceptionHandler) {
        if (exceptionHandler == null) {
            throw new IllegalArgumentException("exceptionHandler must not be null")
        }
        this.exceptionHandlerRef.set(exceptionHandler)
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void join(final long millis) throws InterruptedException {
        final Thread workerThread = this.threadRef.get()
        if (workerThread != null && workerThread.isAlive()) {
            workerThread.join(millis)
        }
    }

    /**
     * Polls the queues for jobs and executes them.
     */
    protected void poll() {
        int missCount = 0
        String curQueue = null
        while (this.running) {
            try {
                if (threadNameChangingEnabled) {
                    renameThread("Waiting for " + JesqueUtils.join(",", this.queueNames))
                }
                curQueue = this.queueNames.poll(EMPTY_QUEUE_SLEEP_TIME, TimeUnit.MILLISECONDS)
                if (curQueue) {
                    this.queueNames.add(curQueue); // Rotate the queues
                    checkPaused()
                    // Might have been waiting in poll()/checkPaused() for a while
                    if (this.running) {
                        this.listenerDelegate.fireEvent(WorkerEvent.WORKER_POLL, this, curQueue, null, null, null, null)
                        final String payload = pop(curQueue)
                        if (payload != null) {
                            final Job job = ObjectMapperFactory.get().readValue(payload, Job.class)
                            process(job, curQueue)
                            missCount = 0
                        } else if (++missCount >= this.queueNames.size() && this.running) {
                            // Keeps worker from busy-spinning on empty queues
                            missCount = 0
                            Thread.sleep(EMPTY_QUEUE_SLEEP_TIME)
                        }
                    }
                }
            } catch (InterruptedException ie) {
                if (!isShutdown()) {
                    recoverFromException(curQueue, ie)
                }
            } catch (JsonParseException | JsonMappingException e) {
                // If the job JSON is not deserializable, we never want to submit it again...
                removeInFlight(curQueue)
                recoverFromException(curQueue, e)
            } catch (Exception e) {
                recoverFromException(curQueue, e)
            }
        }
    }

    /**
     * Remove a job from the given queue.
     *
     * @param curQueue the queue to remove a job from
     * @return a JSON string of a job or null if there was nothing to de-queue
     */
    protected String pop(final String curQueue) {
        final String key = this.key(ResqueConstants.QUEUE, curQueue)
        String payload = null
        // If a delayed queue, peek and remove from ZSET
        withJedis { Jedis jedis ->
            if (JedisUtils.isDelayedQueue(jedis, key)) {
                final long now = System.currentTimeMillis()
                // Peek ==> is there any item scheduled to run between -INF and now?
                final Set<Tuple> payloadSet = jedis.zrangeByScoreWithScores(key, -1, now, 0, 1)
                if (payloadSet) {
                    final Tuple tuple = payloadSet.iterator().next()
                    final String tmp = tuple.getElement()
                    final double score = tuple.getScore()
                    // If a recurring job, increment the job score by hash field value
                    String recurringHashKey = JesqueUtils.createRecurringHashKey(key)
                    if (jedis.hexists(recurringHashKey, tmp)) {
                        // Watch the hash to ensure that the job isn't deleted
                        // TODO: Use the ZADD XX feature
                        jedis.watch(recurringHashKey)
                        final Long frequency = Long.valueOf(jedis.hget(recurringHashKey, tmp))
                        final Transaction transaction = jedis.multi()
                        transaction.zadd(key, score + frequency, tmp)
                        if (transaction.exec() != null) {
                            payload = tmp
                        }
                    } else {
                        // Try to acquire this job
                        if (jedis.zrem(key, tmp) == 1L) {
                            payload = tmp
                        }
                    }
                }
            } else if (JedisUtils.isRegularQueue(jedis, key)) { // If a regular queue, pop from it
                payload = lpoplpush(key, this.key(ResqueConstants.INFLIGHT, this.name, curQueue))
            }

        }
        return payload
    }

    /**
     * Handle an exception that was thrown from inside {@link #poll()}.
     *
     * @param curQueue the name of the queue that was being processed when the exception was thrown
     * @param ex the exception that was thrown
     */
    protected void recoverFromException(final String curQueue, final Exception ex) {
        final RecoveryStrategy recoveryStrategy = this.exceptionHandlerRef.get().onException(this, ex, curQueue)
        switch (recoveryStrategy) {
            case RecoveryStrategy.RECONNECT:
                // NOOP no need to recover the RedisPool will handle this for us
                break
            case RecoveryStrategy.TERMINATE:
                log.warn("Terminating in response to exception", ex)
                end(false)
                break
            case RecoveryStrategy.PROCEED:
                this.listenerDelegate.fireEvent(WorkerEvent.WORKER_ERROR, this, curQueue, null, null, null, ex)
                break
            default:
                log.error("Unknown RecoveryStrategy: $recoveryStrategy while attempting to recover from the exception; worker proceeding...", ex)
                break
        }
    }

    /**
     * Checks to see if worker is paused. If so, wait until unpaused.
     *
     * @throws IOException if there was an error creating the pause message
     */
    protected void checkPaused() throws IOException {
        if (this.paused.get()) {
            synchronized (this.paused) {
                if (this.paused.get()) {
                    withJedis { Jedis jedis ->
                        jedis.set(key(ResqueConstants.WORKER, this.name), pauseMsg())
                    }
                }
                while (this.paused.get()) {
                    try {
                        this.paused.wait()
                    } catch (InterruptedException ie) {
                        log.warn("Worker interrupted", ie)
                    }
                }
                withJedis { Jedis jedis ->
                    jedis.del(key(ResqueConstants.WORKER, this.name))
                }
            }
        }
    }

    /**
     * Materializes and executes the given job.
     *
     * @param job the Job to process
     * @param curQueue the queue the payload came from
     */
    protected void process(final Job job, final String curQueue) {
        withJedis { Jedis jedis ->
            try {
                this.processingJob.set(true)
                if (threadNameChangingEnabled) {
                    renameThread("Processing " + curQueue + " since " + System.currentTimeMillis())
                }
                this.listenerDelegate.fireEvent(WorkerEvent.JOB_PROCESS, this, curQueue, job, null, null, null)
                jedis.set(key(ResqueConstants.WORKER, this.name), statusMsg(curQueue, job))
                final Object instance = this.jobFactory.materializeJob(job)
                final Object result = execute(job, curQueue, instance)
                success(job, instance, result, curQueue)
            } catch (Throwable thrwbl) {
                failure(thrwbl, job, curQueue)
            } finally {
                removeInFlight(curQueue)
                jedis.del(key(ResqueConstants.WORKER, this.name))
                this.processingJob.set(false)
            }
        }
    }

    private void removeInFlight(final String curQueue) {
        if (this.state.get() == JobExecutor.State.SHUTDOWN_IMMEDIATE) {
            lpoplpush(key(ResqueConstants.INFLIGHT, this.name, curQueue), key(ResqueConstants.QUEUE, curQueue))
        } else {
            withJedis { Jedis jedis ->
                jedis.lpop(key(ResqueConstants.INFLIGHT, this.name, curQueue))
            }
        }
    }

    /**
     * Executes the given job.
     *
     * @param job the job to execute
     * @param curQueue the queue the job came from
     * @param instance the materialized job
     * @return result of the execution
     * @throws Exception if the instance is a {@link Callable} and throws an exception
     */
    protected Object execute(final Job job, final String curQueue, final Object instance) throws Exception {
        if (instance instanceof WorkerAware) {
            ((WorkerAware) instance).setWorker(this)
        }
        this.listenerDelegate.fireEvent(WorkerEvent.JOB_EXECUTE, this, curQueue, job, instance, null, null)
        final Object result
        if (instance instanceof Callable) {
            result = ((Callable<?>) instance).call(); // The job is executing!
        } else if (instance instanceof Runnable) {
            ((Runnable) instance).run(); // The job is executing!
            result = null
        } else { // Should never happen since we're testing the class earlier
            throw new ClassCastException("Instance must be a Runnable or a Callable: " + instance.getClass().getName()
                    + " - " + instance)
        }
        return result
    }

    /**
     * Update the status in Redis on success.
     *
     * @param job the Job that succeeded
     * @param runner the materialized Job
     * @param result the result of the successful execution of the Job
     * @param curQueue the queue the Job came from
     */
    protected void success(final Job job, final Object runner, final Object result, final String curQueue) {
        // The job may have taken a long time; make an effort to ensure the
        // connection is OK
        try {
            withJedis { Jedis jedis ->
                jedis.incr(key(ResqueConstants.STAT, ResqueConstants.PROCESSED))
                jedis.incr(key(ResqueConstants.STAT, ResqueConstants.PROCESSED, this.name))
            }
        } catch (JedisException je) {
            log.warn("Error updating success stats for job=" + job, je)
        }
        this.listenerDelegate.fireEvent(WorkerEvent.JOB_SUCCESS, this, curQueue, job, runner, result, null)
    }

    /**
     * Update the status in Redis on failure.
     *
     * @param thrwbl the Throwable that occurred
     * @param job the Job that failed
     * @param curQueue the queue the Job came from
     */
    protected void failure(final Throwable thrwbl, final Job job, final String curQueue) {
        // The job may have taken a long time; make an effort to ensure the connection is OK
        try {
            withJedis { Jedis jedis ->
                jedis.incr(key(ResqueConstants.STAT, ResqueConstants.FAILED))
                jedis.incr(key(ResqueConstants.STAT, ResqueConstants.FAILED, this.name))
                final String failQueueKey = this.failQueueStrategyRef.get().getFailQueueKey(thrwbl, job, curQueue)
                if (failQueueKey != null) {
                    jedis.rpush(failQueueKey, failMsg(thrwbl, curQueue, job))
                }
            }
        } catch (JedisException je) {
            log.warn("Error updating failure stats for throwable=" + thrwbl + " job=" + job, je)
        } catch (IOException ioe) {
            log.warn("Error serializing failure payload for throwable=" + thrwbl + " job=" + job, ioe)
        }
        this.listenerDelegate.fireEvent(WorkerEvent.JOB_FAILURE, this, curQueue, job, null, null, thrwbl)
    }

    /**
     * Create and serialize a JobFailure.
     *
     * @param thrwbl the Throwable that occurred
     * @param queue the queue the job came from
     * @param job the Job that failed
     * @return the JSON representation of a new JobFailure
     * @throws IOException if there was an error serializing the JobFailure
     */
    protected String failMsg(final Throwable thrwbl, final String queue, final Job job) throws IOException {
        final JobFailure failure = new JobFailure()
        failure.setFailedAt(new Date())
        failure.setWorker(this.name)
        failure.setQueue(queue)
        failure.setPayload(job)
        failure.setThrowable(thrwbl)
        return ObjectMapperFactory.get().writeValueAsString(failure)
    }

    /**
     * Create and serialize a WorkerStatus.
     *
     * @param queue the queue the Job came from
     * @param job the Job currently being processed
     * @return the JSON representation of a new WorkerStatus
     * @throws IOException if there was an error serializing the WorkerStatus
     */
    protected static String statusMsg(final String queue, final Job job) throws IOException {
        final WorkerStatus status = new WorkerStatus()
        status.setRunAt(new Date())
        status.setQueue(queue)
        status.setPayload(job)
        return ObjectMapperFactory.get().writeValueAsString(status)
    }

    /**
     * Create and serialize a WorkerStatus for a pause event.
     *
     * @return the JSON representation of a new WorkerStatus
     * @throws IOException if there was an error serializing the WorkerStatus
     */
    protected String pauseMsg() throws IOException {
        final WorkerStatus status = new WorkerStatus()
        status.setRunAt(new Date())
        status.setPaused(isPaused())
        return ObjectMapperFactory.get().writeValueAsString(status)
    }

    /**
     * Creates a unique name, suitable for use with Resque.
     *
     * @return a unique name for this worker
     */
    protected String createName() {
        final StringBuilder buf = new StringBuilder(128)
        try {
            buf.append(InetAddress.getLocalHost().getHostName()).append(ResqueConstants.COLON)
                    .append(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]) // PID
                    .append('-').append(this.workerId).append(ResqueConstants.COLON).append(ResqueConstants.JAVA_DYNAMIC_QUEUES)
            for (final String queueName : this.queueNames) {
                buf.append(',').append(queueName)
            }
        } catch (UnknownHostException uhe) {
            throw new RuntimeException(uhe)
        }
        return buf.toString()
    }

    /**
     * Builds a namespaced Redis key with the given arguments.
     *
     * @param parts the key parts to be joined
     * @return an assembled String key
     */
    protected String key(final String... parts) {
        return JesqueUtils.createKey(this.namespace, parts)
    }

    /**
     * Rename the current thread with the given message.
     *
     * @param msg the message to add to the thread name
     */
    protected void renameThread(final String msg) {
        Thread.currentThread().setName(this.threadNameBase + msg)
    }

    protected String lpoplpush(final String from, final String to) {
        String result = null
        withJedis { Jedis jedis ->
            while (JedisUtils.isRegularQueue(jedis, from)) {
                jedis.watch(from)
                // Get the leftmost value of the 'from' list. If it does not exist, there is nothing to pop.
                String val = null
                if (JedisUtils.isRegularQueue(jedis, from)) {
                    val = jedis.lindex(from, 0)
                }
                if (val == null) {
                    jedis.unwatch()
                    result = val
                    break
                }
                final Transaction tx = jedis.multi()
                tx.lpop(from)
                tx.lpush(to, val)
                if (tx.exec() != null) {
                    result = val
                    break
                }
                // If execution of the transaction failed, this means that 'from'
                // was modified while we were watching it and the transaction was
                // not executed. We simply retry the operation.
            }
        }
        return result
    }

    protected withJedis(Closure c) {
        Jedis jedis = getJedisClient()
        try {
            c.call(jedis)
        } finally {
            returnJedis(jedis)
        }
    }

    protected Jedis getJedisClient() {
        Jedis jedis = this.jedisPool.resource
        // authenticate
        if (this.config.getPassword() != null) {
            jedis.auth(this.config.password)
        }

        // select db
        jedis.select(this.config.database)
        return jedis
    }

    protected void returnJedis(Jedis jedis) {
        try {
            this.jedisPool.returnResource(jedis)
        } catch (Exception e) {
            log.error("Could not return resource to jedis pool", e)
            throw e
        }
    }

    /**
     * Returns whether the state of this Worker is RUNNING
     * @return true if this worker is running
     */
    private boolean isRunning() {
        this.state.get() == JobExecutor.State.RUNNING
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return this.namespace + ResqueConstants.COLON + ResqueConstants.WORKER + ResqueConstants.COLON + this.name
    }
}
