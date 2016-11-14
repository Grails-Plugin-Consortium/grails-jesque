package grails.plugins.jesque

import grails.test.mixin.TestFor
import net.greghaines.jesque.Config
import net.greghaines.jesque.worker.WorkerEvent
import redis.clients.jedis.Jedis
import redis.clients.util.Pool
import spock.lang.Specification

@TestFor(JesqueService)
class JesqueServiceSpec extends Specification {

    def setup() {
        service.grailsApplication = grailsApplication
        service.jesqueConfig = Mock(Config)
        def pool = Mock(Pool)
        pool.getResource() >> Mock(Jedis)
        service.redisPool = pool
    }

    /**
     * This test might throw an exception that can be safely ignored as there really is threads started but we do not have a real redis connection
     */
    def "startWorker is adding a custom worker listener if specified in the config"() {
        given:
        def queues = ["foo", "bar"]
        def jobTypes = ["TestJob": TestJob]

        and:
        grailsApplication.config.grails.jesque.custom.listener.clazz = configuredListener

        when:

        def worker = service.startWorker(queues, jobTypes, null, true)

        then:
        worker.workerEventEmitter.eventListenerMap.each { k, v ->
            expectedListeners.each { expectedListener ->
                assert v.any {
                    it.class == expectedListener
                }, "$expectedListener not in Listener list for $k" // as there is some MetaClass magic happening we have to check it like this
            }
        }

        cleanup:
        worker.end(true)

        where:
        configuredListener                                          | expectedListeners
        "grails.plugins.jesque.TestListener"                        | [TestListener]
        TestListener                                                | [TestListener]
        [TestListener, AnotherTestListener]                         | [TestListener, AnotherTestListener]
        ["grails.plugins.jesque.TestListener", AnotherTestListener] | [TestListener, AnotherTestListener]

    }

    /**
     * This test might throw an exception that can be safely ignored as there really is threads started but we do not have a real redis connection
     */
    def "startWorker is not adding any customer listeners when there is none configured"() {
        given:
        def queues = ["foo", "bar"]
        def jobTypes = ["TestJob": TestJob]

        and:
        grailsApplication.config.grails.jesque.custom.listener.clazz = null

        when:

        def worker = service.startWorker(queues, jobTypes, null, true)

        then:
        worker.workerEventEmitter.eventListenerMap[WorkerEvent.WORKER_START].size() == 0 // we know that there is no default WORKER_START listener
    }

    static class TestJob {
        def perform() {
            true
        }
    }
}
