package grails.jesque

import grails.core.GrailsApplication
import grails.plugins.Plugin
import grails.plugins.jesque.*
import groovy.util.logging.Slf4j
import net.greghaines.jesque.Config
import net.greghaines.jesque.ConfigBuilder
import net.greghaines.jesque.client.ClientPoolImpl
import net.greghaines.jesque.meta.dao.impl.FailureDAORedisImpl
import net.greghaines.jesque.meta.dao.impl.KeysDAORedisImpl
import net.greghaines.jesque.meta.dao.impl.QueueInfoDAORedisImpl
import net.greghaines.jesque.meta.dao.impl.WorkerInfoDAORedisImpl
import org.springframework.beans.factory.config.MethodInvokingFactoryBean
import org.springframework.context.ApplicationContext

import static grails.async.Promises.task

@Slf4j
class JesqueGrailsPlugin extends Plugin {

    // the version or versions of Grails the plugin is designed for
    def grailsVersion = "3.0.0 > *"
    // resources that are excluded from plugin packaging
    def dependsOn = [redis: "2.0.0 > *"]
    def pluginExcludes = [
            "grails-app/views/**",
            "grails-app/domain/**",
            "grails-app/jobs/**",
            "test/**",
    ]

    def title = "Jesque - Redis backed job processing"
    def description = 'Grails Jesque plug-in. Redis backed job processing'

    def author = "Christian Oestreich"
    def authorEmail = "acetrike@gmail.com"

    def license = "APACHE"
    def developers = [
            [name: "Michael Cameron", email: "michael.e.cameron@gmail.com"],
            [name: "Christian Oestreich", email: "acetrike@gmail.com"],
            [name: "Ted Naleid", email: "contact@naleid.com"],
            [name: "Philipp Eschenbach", email: "peh@kunstsysteme.com"],
            [name: "Florian Langenhahn", email: "fln@kunstsysteme.com"]]
    def documentation = "https://github.com/Grails-Plugin-Consortium/grails-jesque"
    def scm = [url: "https://github.com/Grails-Plugin-Consortium/grails-jesque"]

    def loadAfter = ['core', 'hibernate']
    def profiles = ['web']

    Closure doWithSpring() {
        { ->
            log.info "Merging in default jesque config"

            if (!isJesqueEnabled(grailsApplication)) {
                log.info "Jesque Disabled"
                return
            }

            log.info "Creating jesque core beans"
            def redisConfigMap = grailsApplication.config.grails.redis
            def jesqueConfigMap = grailsApplication.config.grails.jesque

            def jesqueConfigBuilder = new ConfigBuilder()
            if (jesqueConfigMap.namespace)
                jesqueConfigBuilder = jesqueConfigBuilder.withNamespace(jesqueConfigMap.namespace)
            if (redisConfigMap.host)
                jesqueConfigBuilder = jesqueConfigBuilder.withHost(redisConfigMap.host)
            if (redisConfigMap.sentinels)
                try {
                    jesqueConfigBuilder = jesqueConfigBuilder.withSentinels(new TreeSet<String>(redisConfigMap.sentinels as List))
                } catch (Exception e) {
                    log.error("Make sure your sentinels are a List")
                }
            if (redisConfigMap.masterName)
                jesqueConfigBuilder = jesqueConfigBuilder.withMasterName(redisConfigMap.masterName)
            if (redisConfigMap.port)
                jesqueConfigBuilder = jesqueConfigBuilder.withPort(redisConfigMap.port as Integer)
            if (redisConfigMap.timeout)
                jesqueConfigBuilder = jesqueConfigBuilder.withTimeout(redisConfigMap.timeout as Integer)
            if (redisConfigMap.password)
                jesqueConfigBuilder = jesqueConfigBuilder.withPassword(redisConfigMap.password)
            if (redisConfigMap.database)
                jesqueConfigBuilder = jesqueConfigBuilder.withDatabase(redisConfigMap.database as Integer)

            def jesqueConfigInstance = jesqueConfigBuilder.build()


            if (jesqueConfigInstance.sentinels && jesqueConfigInstance.masterName) {
                log.info "Using sentinel config sentinels $jesqueConfigInstance.sentinels"
                def sentinels = jesqueConfigInstance.sentinels
                if (sentinels instanceof String) {
                    try {
                        sentinels = Eval.me(sentinels.toString())
                    } catch (Exception e) {
                        log.info("Could not eval sentinels $sentinels", e)
                    }
                }

                if (sentinels instanceof Collection) {
                    jesqueConfig(Config, jesqueConfigInstance.sentinels as Set, jesqueConfigInstance.masterName, jesqueConfigInstance.timeout,
                            jesqueConfigInstance.password, jesqueConfigInstance.namespace, jesqueConfigInstance.database)

                } else {
                    throw new RuntimeException('Redis configuration property [sentinels] does not appear to be a valid collection.')
                }

            } else {
                log.info "Using redis config host $jesqueConfigInstance.host"
                jesqueConfig(Config, jesqueConfigInstance.host, jesqueConfigInstance.port, jesqueConfigInstance.timeout,
                        jesqueConfigInstance.password, jesqueConfigInstance.namespace, jesqueConfigInstance.database)
            }

            jesqueAdminClient(AdminClientImpl, ref('jesqueConfig'), ref('redisPool'))
            jesqueClient(ClientPoolImpl, jesqueConfigInstance, ref('redisPool'))

            failureDao(FailureDAORedisImpl, ref('jesqueConfig'), ref('redisPool'))
            keysDao(KeysDAORedisImpl, ref('jesqueConfig'), ref('redisPool'))
            queueInfoDao(QueueInfoDAORedisImpl, ref('jesqueConfig'), ref('redisPool'))
            workerInfoDao(WorkerInfoDAORedisImpl, ref('jesqueConfig'), ref('redisPool'))

            log.info "Creating jesque job beans"
            grailsApplication.jesqueJobClasses.each { jobClass ->
                configureJobBeans.delegate = delegate
                configureJobBeans(jobClass)
            }
        }
    }

    def configureJobBeans = { GrailsJesqueJobClass jobClass ->
        def fullName = jobClass.fullName

        "${fullName}Class"(MethodInvokingFactoryBean) {
            targetObject = ref("grailsApplication", false)
            targetMethod = "getArtefact"
            arguments = [JesqueJobArtefactHandler.TYPE, jobClass.fullName]
        }

        "${fullName}"(ref("${fullName}Class")) { bean ->
            bean.factoryMethod = "newInstance"
            bean.autowire = "byName"
            bean.scope = "prototype"
        }

        log.info "Wired job beans for $jobClass.fullName"
    }

    void doWithDynamicMethods() {
        // TODO Implement registering dynamic methods to classes (optional)
    }

    void doWithApplicationContext() {
        if (!isJesqueEnabled(grailsApplication))
            return

        task {
            TriggersConfigBuilder.metaClass.getGrailsApplication = { -> grailsApplication }

            JesqueConfigurationService jesqueConfigurationService = applicationContext.jesqueConfigurationService

            log.info "Scheduling Jesque Jobs"
            grailsApplication.jesqueJobClasses.each { GrailsJesqueJobClass jobClass ->
                jesqueConfigurationService.scheduleJob(jobClass)
            }

            def jesqueConfigMap = grailsApplication.config.grails.jesque

            if (jesqueConfigMap.schedulerThreadActive) {
                log.info "Launching jesque scheduler thread"
                JesqueSchedulerThreadService jesqueSchedulerThreadService = applicationContext.jesqueSchedulerThreadService
                jesqueSchedulerThreadService.startSchedulerThread()
            }
            if (jesqueConfigMap.delayedJobThreadActive) {
                log.info "Launching delayed job thread"
                JesqueDelayedJobThreadService jesqueDelayedJobThreadService = applicationContext.jesqueDelayedJobThreadService
                jesqueDelayedJobThreadService.startThread()
            }

            log.info "Starting jesque workers"
            JesqueService jesqueService = applicationContext.jesqueService

            jesqueConfigurationService.validateConfig(jesqueConfigMap as ConfigObject)

            log.info "Found ${jesqueConfigMap.size()} workers"
            if (jesqueConfigMap.pruneWorkersOnStartup) {
                log.info "Pruning workers"
                jesqueService.pruneWorkers()
            }

            jesqueConfigurationService.mergeClassConfigurationIntoConfigMap(jesqueConfigMap as ConfigObject)
            if (jesqueConfigMap.createWorkersOnStartup) {
                log.info "Creating workers"

                jesqueService.startWorkersFromConfig(jesqueConfigMap as ConfigObject)
            }
        }

        applicationContext
    }

    void onChange(Map<String, Object> event) {
        if (!isJesqueEnabled(grailsApplication))
            return

        Class source = event.source as Class
        if (!grailsApplication.isArtefactOfType(JesqueJobArtefactHandler.TYPE, source)) {
            return
        }

        log.debug("Job ${source} changed. Reloading...")

        ApplicationContext context = event.ctx as ApplicationContext
        JesqueConfigurationService jesqueConfigurationService = context?.jesqueConfigurationService

        if (context && jesqueConfigurationService) {
            GrailsJesqueJobClass jobClass = grailsApplication.getJobClass(source.name)
            if (jobClass)
                jesqueConfigurationService.deleteScheduleJob(jobClass)

            jobClass = (GrailsJesqueJobClass) grailsApplication.addArtefact(JesqueJobArtefactHandler.TYPE, source)

            beans {
                configureJobBeans.delegate = delegate
                configureJobBeans(jobClass)
            }

            jesqueConfigurationService.scheduleJob(jobClass)
        } else {
            log.error("Application context or Jesque Scheduler not found. Can't reload Jesque plugin.")
        }
    }

    void onConfigChange(Map<String, Object> event) {
        // TODO Implement code that is executed when the project configuration changes.
        // The event is the same as for 'onChange'.
    }

    void onShutdown(Map<String, Object> event) {
        // TODO Implement code that is executed when the application shuts down (optional)
    }

    private static Boolean isJesqueEnabled(GrailsApplication application) {
        def jesqueConfigMap = application.config?.grails?.jesque

        Boolean isJesqueEnabled = true

        def enabled = jesqueConfigMap.enabled
        if (enabled != null) {
            if (enabled instanceof String) {
                isJesqueEnabled = Boolean.parseBoolean(enabled)
            } else if (enabled instanceof Boolean) {
                isJesqueEnabled = enabled
            }
        }

        return isJesqueEnabled
    }

}
