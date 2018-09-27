package grails.plugins.jesque

import grails.core.GrailsApplication
import groovy.util.logging.Slf4j
import net.greghaines.jesque.Config
import net.greghaines.jesque.Job
import net.greghaines.jesque.worker.WorkerAware
import net.greghaines.jesque.worker.WorkerPoolImpl
import redis.clients.jedis.Jedis
import redis.clients.util.Pool

import static net.greghaines.jesque.worker.WorkerEvent.JOB_EXECUTE

@Slf4j
class GrailsWorkerImpl extends WorkerPoolImpl {

    GrailsApplication grailsApplication

    GrailsWorkerImpl(GrailsApplication grailsApplication,
                     final Config config,
                     final Pool<Jedis> jedisPool,
                     final Collection<String> queues, final Map<String, ? extends Class> jobTypes) {
        super(config, queues, new GrailsJesqueJobFactory(jobTypes, grailsApplication), jedisPool)

        this.grailsApplication = grailsApplication
    }

    @Override
    protected Object execute(final Job job, final String curQueue, final Object instance) throws Exception {
        log.debug "Executing job ${job.className} from queue $curQueue"
        if (instance instanceof WorkerAware) {
            ((WorkerAware) instance).setWorker(this)
        }
        this.listenerDelegate.fireEvent(JOB_EXECUTE, this, curQueue, job, instance, null, null)
        instance.perform(*job.args)
    }

}
