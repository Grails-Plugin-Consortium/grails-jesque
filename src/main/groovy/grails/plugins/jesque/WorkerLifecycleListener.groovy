package grails.plugins.jesque

import groovy.util.logging.Slf4j
import net.greghaines.jesque.Job
import net.greghaines.jesque.worker.Worker
import net.greghaines.jesque.worker.WorkerEvent
import net.greghaines.jesque.worker.WorkerListener

@Slf4j
class WorkerLifecycleListener implements WorkerListener {

    JesqueService jesqueService

    WorkerLifecycleListener(JesqueService jesqueService) {
        this.jesqueService = jesqueService
    }

    @Override
    void onEvent(WorkerEvent workerEvent, Worker worker, String queue, Job job, Object runner, Object result, Throwable t) {
        log.debug("Processing worker event ${workerEvent.name()}")
        if (workerEvent == WorkerEvent.WORKER_STOP) {
            jesqueService.removeWorkerFromLifecycleTracking(worker)
        }
    }
}
