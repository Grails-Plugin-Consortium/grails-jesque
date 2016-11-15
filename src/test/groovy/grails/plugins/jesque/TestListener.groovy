package grails.plugins.jesque

import net.greghaines.jesque.Job
import net.greghaines.jesque.worker.Worker
import net.greghaines.jesque.worker.WorkerEvent
import net.greghaines.jesque.worker.WorkerListener

class TestListener implements WorkerListener {

    @Override
    void onEvent(WorkerEvent event, Worker worker, String queue, Job job, Object runner, Object result, Throwable t) {
        // NO-OP
    }
}