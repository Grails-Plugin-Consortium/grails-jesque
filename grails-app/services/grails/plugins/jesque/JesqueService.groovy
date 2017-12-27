package grails.plugins.jesque

import grails.core.GrailsApplication
import grails.persistence.support.PersistenceContextInterceptor
import groovy.util.logging.Slf4j
import net.greghaines.jesque.Job
import net.greghaines.jesque.admin.Admin
import net.greghaines.jesque.admin.AdminClient
import net.greghaines.jesque.admin.AdminPoolImpl
import net.greghaines.jesque.client.Client
import net.greghaines.jesque.meta.WorkerInfo
import net.greghaines.jesque.meta.dao.WorkerInfoDAO
import net.greghaines.jesque.worker.*
import org.joda.time.DateTime
import org.springframework.beans.factory.DisposableBean

@Slf4j
class JesqueService implements DisposableBean {

    static final int DEFAULT_WORKER_POOL_SIZE = 3

    GrailsApplication grailsApplication
    def jesqueConfig
    JesqueDelayedJobService jesqueDelayedJobService
    PersistenceContextInterceptor persistenceInterceptor
    Client jesqueClient
    WorkerInfoDAO workerInfoDao
    List<Worker> workers = Collections.synchronizedList([])
    AdminClient jesqueAdminClient
    def redisPool

    void enqueue(String queueName, Job job) {
        jesqueClient.enqueue(queueName, job)
    }

    void enqueue(String queueName, String jobName, List args) {
        enqueue(queueName, new Job(jobName, args))
    }

    void enqueue(String queueName, Class jobClazz, List args) {
        enqueue(queueName, jobClazz.simpleName, args)
    }

    void enqueue(String queueName, String jobName, Object... args) {
        enqueue(queueName, new Job(jobName, args))
    }

    void enqueue(String queueName, Class jobClazz, Object... args) {
        enqueue(queueName, jobClazz.simpleName, args)
    }

    void priorityEnqueue(String queueName, Job job) {
        jesqueClient.priorityEnqueue(queueName, job)
    }

    void priorityEnqueue(String queueName, String jobName, def args) {
        priorityEnqueue(queueName, new Job(jobName, args))
    }

    void priorityEnqueue(String queueName, Class jobClazz, def args) {
        priorityEnqueue(queueName, jobClazz.simpleName, args)
    }

    void enqueueAt(DateTime dateTime, String queueName, Job job) {
        jesqueDelayedJobService.enqueueAt(dateTime, queueName, job)
    }

    void enqueueAt(DateTime dateTime, String queueName, String jobName, Object... args) {
        enqueueAt(dateTime, queueName, new Job(jobName, args))
    }

    void enqueueAt(DateTime dateTime, String queueName, Class jobClazz, Object... args) {
        enqueueAt(dateTime, queueName, jobClazz.simpleName, args)
    }

    void enqueueAt(DateTime dateTime, String queueName, String jobName, List args) {
        enqueueAt(dateTime, queueName, new Job(jobName, args))
    }

    void enqueueAt(DateTime dateTime, String queueName, Class jobClazz, List args) {
        enqueueAt(dateTime, queueName, jobClazz.simpleName, args)
    }


    void enqueueIn(Integer millisecondDelay, String queueName, Job job) {
        enqueueAt(new DateTime().plusMillis(millisecondDelay), queueName, job)
    }

    void enqueueIn(Integer millisecondDelay, String queueName, String jobName, Object... args) {
        enqueueIn(millisecondDelay, queueName, new Job(jobName, args))
    }

    void enqueueIn(Integer millisecondDelay, String queueName, Class jobClazz, Object... args) {
        enqueueIn(millisecondDelay, queueName, jobClazz.simpleName, args)
    }

    void enqueueIn(Integer millisecondDelay, String queueName, String jobName, List args) {
        enqueueIn(millisecondDelay, queueName, new Job(jobName, args))
    }

    void enqueueIn(Integer millisecondDelay, String queueName, Class jobClazz, List args) {
        enqueueIn(millisecondDelay, queueName, jobClazz.simpleName, args)
    }


    Worker startWorker(String queueName, String jobName, Class jobClass, ExceptionHandler exceptionHandler = null,
                       boolean paused = false) {
        startWorker([queueName], [(jobName): jobClass], exceptionHandler, paused)
    }

    Worker startWorker(List queueName, String jobName, Class jobClass, ExceptionHandler exceptionHandler = null,
                       boolean paused = false) {
        startWorker(queueName, [(jobName): jobClass], exceptionHandler, paused)
    }

    Worker startWorker(String queueName, Map<String, ? extends Class> jobTypes, ExceptionHandler exceptionHandler = null,
                       boolean paused = false) {
        startWorker([queueName], jobTypes, exceptionHandler, paused)
    }

    Worker startWorker(List<String> queues, Map<String, Class> jobTypes, ExceptionHandler exceptionHandler = null,
                       boolean paused = false) {
        log.info "Starting worker processing queueus: ${queues}"

        Class workerClass = GrailsWorkerImpl
        def customWorkerClass = grailsApplication.config.grails.jesque.custom.worker.clazz
        if (customWorkerClass) {
            if (customWorkerClass instanceof String) {
                try {
                    customWorkerClass = Class.forName(customWorkerClass)
                } catch (ClassNotFoundException ignore) {
                    log.error("Custom Worker class not found for name $customWorkerClass")
                    customWorkerClass = null
                }
            }
            if (customWorkerClass && customWorkerClass in GrailsWorkerImpl) {
                workerClass = customWorkerClass as Class
            } else if (customWorkerClass) {
                // the "null" case should only happen at this point, when we could not find the class, so we can safely assume there was a error message already
                log.warn("The specified custom worker class ${customWorkerClass} does not extend GrailsWorkerImpl. Ignoring it")
            }
        }

        Worker worker = (GrailsWorkerImpl) workerClass.newInstance(grailsApplication, jesqueConfig, redisPool, queues, jobTypes)

        def customListenerClass = grailsApplication.config.grails.jesque.custom.listener.clazz
        if (customListenerClass) {
            if (customListenerClass instanceof List) {
                customListenerClass.each {
                    addCustomListenerClass(worker, it)
                }
            } else {
                addCustomListenerClass(worker, customListenerClass)
            }
        }

        if (exceptionHandler)
            worker.exceptionHandler = exceptionHandler

        if (paused) {
            worker.togglePause(paused)
        }

        workers.add(worker)

        // create an Admin for this worker (makes it possible to administer across a cluster)
        Admin admin = new AdminPoolImpl(jesqueConfig, redisPool)
        admin.setWorker(worker)

        def autoFlush = grailsApplication.config.grails.jesque.autoFlush ?: true
        def workerPersistenceListener = new WorkerPersistenceListener(persistenceInterceptor, autoFlush)
        worker.workerEventEmitter.addListener(workerPersistenceListener, WorkerEvent.JOB_EXECUTE, WorkerEvent.JOB_SUCCESS, WorkerEvent.JOB_FAILURE)

        def workerLifeCycleListener = new WorkerLifecycleListener(this)
        worker.workerEventEmitter.addListener(workerLifeCycleListener, WorkerEvent.WORKER_STOP)

        def workerThread = new Thread(worker)
        workerThread.start()

        def adminThread = new Thread(admin)
        adminThread.start()

        worker
    }

    void stopAllWorkers() {
        log.info "Stopping ${workers.size()} jesque workers"

        List<Worker> workersToRemove = workers.collect { it }
        workersToRemove.each { Worker worker ->
            try {
                log.debug "Stopping worker processing queues: ${worker.queues}"
                worker.end(true)
                worker.join(5000)
            } catch (Exception exception) {
                log.error "Exception ending jesque worker", exception
            }
        }
    }

    void withWorker(String queueName, String jobName, Class jobClassName, Closure closure) {
        def worker = startWorker(queueName, jobName, jobClassName)
        try {
            closure()
        } finally {
            worker.end(true)
        }
    }

    void startWorkersFromConfig(ConfigObject jesqueConfigMap) {
        boolean startPaused = jesqueConfigMap.startPaused as boolean ?: false

        jesqueConfigMap?.workers?.keySet()?.each { String workerPoolName ->

            log.info "Starting workers for pool $workerPoolName"
            def value = jesqueConfigMap?.workers[workerPoolName]

            def workers = value.workers ? value.workers.toInteger() : DEFAULT_WORKER_POOL_SIZE
            def queueNames = value.queueNames
            List<String> jobTypes = value.jobTypes

            if (!((queueNames instanceof String) || (queueNames instanceof List<String>)))
                throw new Exception("Invalid queueNames for pool $workerPoolName, expecting must be a String or a List<String>.")

            if (!(jobTypes instanceof List))
                throw new Exception("Invalid jobTypes (${jobTypes}) for pool $workerPoolName, must be a list")

            Map<String, Class> jobNameClass = [:]
            jobTypes?.each { String k ->
                def clazz = grailsApplication.getClassForName(k)
                if (clazz) {
                    jobNameClass.put(clazz.simpleName, clazz)
                } else {
                    log.info "Could not get grails class $k"
                }
            }

            workers.times {
                startWorker(queueNames, jobNameClass, null, startPaused)
            }
        }
    }

    void pruneWorkers() {
        def hostName = InetAddress.localHost.hostName
        workerInfoDao.allWorkers?.each { WorkerInfo workerInfo ->
            if (workerInfo.host == hostName) {
                log.debug "Removing stale worker $workerInfo.name"
                workerInfoDao.removeWorker(workerInfo.name)
            }
        }
    }

    public void removeWorkerFromLifecycleTracking(Worker worker) {
        log.debug "Removing worker ${worker.name} from lifecycle tracking"
        workers.remove(worker)
    }

    void destroy() throws Exception {
        this.stopAllWorkers()
    }

    void pauseAllWorkersOnThisNode() {
        log.info "Pausing all ${workers.size()} jesque workers on this node"

        List<Worker> workersToPause = workers.collect { it }
        workersToPause.each { Worker worker ->
            log.debug "Pausing worker processing queues: ${worker.queues}"
            worker.togglePause(true)
        }
    }

    void resumeAllWorkersOnThisNode() {
        log.info "Resuming all ${workers.size()} jesque workers on this node"

        List<Worker> workersToPause = workers.collect { it }
        workersToPause.each { Worker worker ->
            log.debug "Resuming worker processing queues: ${worker.queues}"
            worker.togglePause(false)
        }
    }

    void pauseAllWorkersInCluster() {
        log.debug "Pausing all workers in the cluster"
        jesqueAdminClient.togglePausedWorkers(true)
    }

    void resumeAllWorkersInCluster() {
        log.debug "Resuming all workers in the cluster"
        jesqueAdminClient.togglePausedWorkers(false)
    }

    void shutdownAllWorkersInCluster() {
        log.debug "Shutting down all workers in the cluster"
        jesqueAdminClient.shutdownWorkers(true)
    }

    boolean areAllWorkersInClusterPaused() {
        return workerInfoDao.getActiveWorkerCount() == 0
    }

    private addCustomListenerClass(Worker worker, customListenerClass) {
        if (customListenerClass instanceof String) {
            try {
                customListenerClass = Class.forName(customListenerClass)
            } catch (ClassNotFoundException ignore) {
                log.error("Custom Job Listener class not found for name $customListenerClass")
                customListenerClass = null
            }
        }
        if (customListenerClass && customListenerClass in WorkerListener) {
            worker.workerEventEmitter.addListener(customListenerClass.newInstance() as WorkerListener)
        } else if (customListenerClass) {
            // the "null" case should only happen at this point, when we could not find the class, so we can safely assume there was a error message already
            log.warn("The specified custom listener class ${customListenerClass} does not implement WorkerListener. Ignoring it")
        }
    }
}
