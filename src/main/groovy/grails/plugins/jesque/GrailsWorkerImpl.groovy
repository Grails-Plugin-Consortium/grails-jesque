package grails.plugins.jesque

import grails.core.GrailsApplication
import grails.spring.BeanBuilder
import groovy.util.logging.Slf4j
import net.greghaines.jesque.Config
import net.greghaines.jesque.Job
import net.greghaines.jesque.worker.RecoveryStrategy
import net.greghaines.jesque.worker.UnpermittedJobException
import net.greghaines.jesque.worker.WorkerAware
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.util.Pool

import static net.greghaines.jesque.utils.ResqueConstants.WORKER
import static net.greghaines.jesque.worker.WorkerEvent.*

@Slf4j
class GrailsWorkerImpl extends WorkerImpl {

    BeanBuilder beanBuilder
    GrailsApplication grailsApplication
    JobExceptionHandler jobExceptionHandler

    public GrailsWorkerImpl(
            GrailsApplication grailsApplication,
            final Config config,
            final Collection<String> queues,
            final Map<String, ? extends Class> jobTypes) {
        super(config, queues, new GrailsJesqueJobFactory(jobTypes))

        log.error("This is no longer supported. The underlying connection requires a jedis pool object.")

        this.grailsApplication = grailsApplication
        beanBuilder = new BeanBuilder()
    }

    public GrailsWorkerImpl(
            GrailsApplication grailsApplication,
            final Config config,
            final Pool<Jedis> jedisPool,
            final Collection<String> queues,
            final Map<String, ? extends Class> jobTypes) {
        super(config, queues, new GrailsJesqueJobFactory(jobTypes), jedisPool)

        this.grailsApplication = grailsApplication
        beanBuilder = new BeanBuilder()
    }

    protected Object execute(final Job job, final String curQueue, final Object instance) throws Exception {
        log.info "Executing jog ${job.className}"
        if (instance instanceof WorkerAware) {
            ((WorkerAware) instance).setWorker(this);
        }
        return execute(job, curQueue, instance, job.args)
    }


    protected void process(final Job job, final String curQueue) {
        this.listenerDelegate.fireEvent(JOB_PROCESS, this, curQueue, job, null, null, null)
        renameThread("Processing " + curQueue + " since " + System.currentTimeMillis())
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
        this.jedisClient.set(key(WORKER, this.name), statusMsg(curQueue, job))
        try {
            log.info "Running perform on instance ${job.className}"
            final Object result
            this.listenerDelegate.fireEvent(JOB_EXECUTE, this, curQueue, job, instance, null, null)
            result = instance.perform(*args)
            success(job, instance, result, curQueue)
        } finally {
            this.jedisClient.del(key(WORKER, this.name))
        }
    }

}
