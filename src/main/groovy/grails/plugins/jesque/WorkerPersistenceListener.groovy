package grails.plugins.jesque

import grails.persistence.support.PersistenceContextInterceptor
import groovy.util.logging.Log
import net.greghaines.jesque.Job
import net.greghaines.jesque.worker.Worker
import net.greghaines.jesque.worker.WorkerEvent
import net.greghaines.jesque.worker.WorkerListener

@Log
class WorkerPersistenceListener implements WorkerListener {

    PersistenceContextInterceptor persistenceInterceptor
    boolean initiated = false
    boolean autoFlush

    WorkerPersistenceListener(PersistenceContextInterceptor persistenceInterceptor, boolean autoFlush) {
        this.persistenceInterceptor = persistenceInterceptor
        this.autoFlush = autoFlush
    }

    private boolean bindSession() {
        if (persistenceInterceptor == null)
            throw new IllegalStateException("No persistenceInterceptor found");

        log.fine("Binding session")

        if (!initiated) {
            persistenceInterceptor.init()
        }
        true
    }

    private void unbindSession() {
        if (initiated) {
            if (autoFlush) {
                persistenceInterceptor.flush()
            }
            persistenceInterceptor.destroy()
            initiated = false
        } else {
            log.fine("persistenceInterceptor has never been initialised")
        }
    }

    @Override
    void onEvent(WorkerEvent workerEvent, Worker worker, String queue, Job job, Object runner, Object result, Throwable t) {
        log.fine("Processing worker event ${workerEvent.name()}")
        if (workerEvent == WorkerEvent.JOB_EXECUTE) {
            initiated = bindSession()
        } else if (workerEvent in [WorkerEvent.JOB_SUCCESS, WorkerEvent.JOB_FAILURE]) {
            unbindSession()
        }
    }
}
