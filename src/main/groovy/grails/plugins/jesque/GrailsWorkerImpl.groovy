package grails.plugins.jesque

import grails.core.GrailsApplication
import groovy.util.logging.Slf4j
import net.greghaines.jesque.Config
import net.greghaines.jesque.Job
import net.greghaines.jesque.worker.UnpermittedJobException
import net.greghaines.jesque.worker.WorkerAware
import redis.clients.jedis.Jedis
import redis.clients.util.Pool

import static net.greghaines.jesque.utils.ResqueConstants.WORKER
import static net.greghaines.jesque.worker.WorkerEvent.JOB_EXECUTE
import static net.greghaines.jesque.worker.WorkerEvent.JOB_PROCESS

@Slf4j
class GrailsWorkerImpl extends WorkerImpl {

    GrailsApplication grailsApplication
    JobExceptionHandler jobExceptionHandler

    public GrailsWorkerImpl(GrailsApplication grailsApplication, final Config config, final Pool<Jedis> jedisPool, final Collection<String> queues, final Map<String, ? extends Class> jobTypes) {
        super(config, queues, new GrailsJesqueJobFactory(jobTypes), jedisPool)

        this.grailsApplication = grailsApplication
    }

    protected Object execute(final Job job, final String curQueue, final Object instance) throws Exception {
        log.debug "Executing jog ${job.className}"
        if (instance instanceof WorkerAware) {
            ((WorkerAware) instance).setWorker(this);
        }
        return execute(job, curQueue, instance, job.args)
    }


    protected void process(final Job job, final String curQueue) {
        this.listenerDelegate.fireEvent(JOB_PROCESS, this, curQueue, job, null, null, null)
        if (threadNameChangingEnabled) {
            renameThread("Processing " + curQueue + " since " + System.currentTimeMillis())
        }
        try {
            Class jobClass = ((GrailsJesqueJobFactory) this.jobFactory).getJobTypes()[job.className]
            if (!jobClass) {
                throw new UnpermittedJobException(job.className)
            }
            def instance = createInstance(jobClass.canonicalName)
            execute(job, curQueue, instance, job.args)
        } catch (Exception e) {
            log.error("Failed job execution", e)
            failure(e, job, curQueue)
        }
    }

    protected void failure(final Exception ex, final Job job, final String curQueue) {
        jobExceptionHandler?.onException(ex, job, curQueue)
        super.failure(ex, job, curQueue)
    }

    protected Object createInstance(String fullClassName) {
        grailsApplication.mainContext.getBean(fullClassName)
    }

    protected void execute(final Job job, final String curQueue, final Object instance, final Object[] args) {
        withJedis { Jedis jedis ->
            jedis.set(key(WORKER, this.name), statusMsg(curQueue, job))
            try {
                log.debug "Running perform on instance ${job.className}"
                final Object result
                this.listenerDelegate.fireEvent(JOB_EXECUTE, this, curQueue, job, instance, null, null)
                result = instance.perform(*args)
                success(job, instance, result, curQueue)
            } finally {
                jedis.del(key(WORKER, this.name))
            }
        }
    }

}
