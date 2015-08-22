package grails.jesque

import grails.plugins.jesque.GrailsJesqueJobClass
import grails.plugins.jesque.JesqueConfigurationService
import grails.plugins.jesque.JesqueDelayedJobThreadService
import grails.plugins.jesque.JesqueJobArtefactHandler
import grails.plugins.jesque.JesqueSchedulerThreadService
import grails.plugins.jesque.JesqueService
import grails.plugins.jesque.TriggersConfigBuilder
import grails.plugins.*
import grails.util.GrailsUtil
import net.greghaines.jesque.ConfigBuilder
import org.codehaus.groovy.grails.commons.GrailsApplication
import org.springframework.beans.factory.config.MethodInvokingFactoryBean
import org.springframework.context.ApplicationContext

class GrailsJesqueGrailsPlugin extends Plugin {

    // the version or versions of Grails the plugin is designed for
    def grailsVersion = "3.0.4 > *"
    // resources that are excluded from plugin packaging
    def pluginExcludes = [
        "grails-app/views/error.gsp"
    ]

    // TODO Fill in these fields
    def title = "Grails Jesque" // Headline display name of the plugin
    def author = "Your name"
    def authorEmail = ""
    def description = '''\
Brief summary/description of the plugin.
'''
    def profiles = ['web']

    // URL to the plugin's documentation
    def documentation = "http://grails.org/plugin/grails-jesque"

    // Extra (optional) plugin metadata

    // License: one of 'APACHE', 'GPL2', 'GPL3'
//    def license = "APACHE"

    // Details of company behind the plugin (if there is one)
//    def organization = [ name: "My Company", url: "http://www.my-company.com/" ]

    // Any additional developers beyond the author specified above.
//    def developers = [ [ name: "Joe Bloggs", email: "joe@bloggs.net" ]]

    // Location of the plugin's issue tracker.
//    def issueManagement = [ system: "JIRA", url: "http://jira.grails.org/browse/GPMYPLUGIN" ]

    // Online location of the plugin's browseable source code.
//    def scm = [ url: "http://svn.codehaus.org/grails-plugins/" ]

    Closure doWithSpring() { {->
        log.info "Merging in default jesque config"
        loadJesqueConfig(application.config.grails.jesque)

        if(!isJesqueEnabled(application)) {
            log.info "Jesque Disabled"
            return
        }

        log.info "Creating jesque core beans"
        def redisConfigMap = application.config.grails.redis
        def jesqueConfigMap = application.config.grails.jesque

        def jesqueConfigBuilder = new ConfigBuilder()
        if(jesqueConfigMap.namespace)
            jesqueConfigBuilder = jesqueConfigBuilder.withNamespace(jesqueConfigMap.namespace)
        if(redisConfigMap.host)
            jesqueConfigBuilder = jesqueConfigBuilder.withHost(redisConfigMap.host)
        if(redisConfigMap.port)
            jesqueConfigBuilder = jesqueConfigBuilder.withPort(redisConfigMap.port as Integer)
        if(redisConfigMap.timeout)
            jesqueConfigBuilder = jesqueConfigBuilder.withTimeout(redisConfigMap.timeout as Integer)
        if(redisConfigMap.password)
            jesqueConfigBuilder = jesqueConfigBuilder.withPassword(redisConfigMap.password)
        if(redisConfigMap.database)
            jesqueConfigBuilder = jesqueConfigBuilder.withDatabase(redisConfigMap.database as Integer)

        def jesqueConfigInstance = jesqueConfigBuilder.build()

        jesqueAdminClient(AdminClientImpl, ref('jesqueConfig'))
        jesqueConfig(Config, jesqueConfigInstance.host, jesqueConfigInstance.port, jesqueConfigInstance.timeout,
                jesqueConfigInstance.password, jesqueConfigInstance.namespace, jesqueConfigInstance.database)
        jesqueClient(ClientPoolImpl, jesqueConfigInstance, ref('redisPool'))

        failureDao(FailureDAORedisImpl, ref('jesqueConfig'), ref('redisPool'))
        keysDao(KeysDAORedisImpl, ref('jesqueConfig'), ref('redisPool'))
        queueInfoDao(QueueInfoDAORedisImpl, ref('jesqueConfig'), ref('redisPool'))
        workerInfoDao(WorkerInfoDAORedisImpl, ref('jesqueConfig'), ref('redisPool'))

        log.info "Creating jesque job beans"
        application.jesqueJobClasses.each {jobClass ->
            configureJobBeans.delegate = delegate
            configureJobBeans(jobClass)
        }
        } 
    }

    def configureJobBeans = {GrailsJesqueJobClass jobClass ->
        def fullName = jobClass.fullName

        "${fullName}Class"(MethodInvokingFactoryBean) {
            targetObject = ref("grailsApplication", true)
            targetMethod = "getArtefact"
            arguments = [JesqueJobArtefactHandler.TYPE, jobClass.fullName]
        }

        "${fullName}"(ref("${fullName}Class")) {bean ->
            bean.factoryMethod = "newInstance"
            bean.autowire = "byName"
            bean.scope = "prototype"
        }
    }

    void doWithDynamicMethods() {
        // TODO Implement registering dynamic methods to classes (optional)
    }

    void doWithApplicationContext() {
        if(!isJesqueEnabled(application))
            return

        TriggersConfigBuilder.metaClass.getGrailsApplication = { -> application }

        JesqueConfigurationService jesqueConfigurationService = applicationContext.jesqueConfigurationService

        log.info "Scheduling Jesque Jobs"
        application.jesqueJobClasses.each{ GrailsJesqueJobClass jobClass ->
            jesqueConfigurationService.scheduleJob(jobClass)
        }

        def jesqueConfigMap = application.config.grails.jesque

        if( jesqueConfigMap.schedulerThreadActive ) {
            log.info "Launching jesque scheduler thread"
            JesqueSchedulerThreadService jesqueSchedulerThreadService = applicationContext.jesqueSchedulerThreadService
            jesqueSchedulerThreadService.startSchedulerThread()
        }
        if( jesqueConfigMap.delayedJobThreadActive ) {
            log.info "Launching delayed job thread"
            JesqueDelayedJobThreadService jesqueDelayedJobThreadService = applicationContext.jesqueDelayedJobThreadService
            jesqueDelayedJobThreadService.startThread()
        }

        log.info "Starting jesque workers"
        JesqueService jesqueService = applicationContext.jesqueService

        jesqueConfigurationService.validateConfig(jesqueConfigMap)

        log.info "Found ${jesqueConfigMap.size()} workers"
        if(jesqueConfigMap.pruneWorkersOnStartup) {
            log.info "Pruning workers"
            jesqueService.pruneWorkers()
        }

        jesqueConfigurationService.mergeClassConfigurationIntoConfigMap(jesqueConfigMap)
        if(jesqueConfigMap.createWorkersOnStartup) {
            log.info "Creating workers"
            jesqueService.startWorkersFromConfig(jesqueConfigMap)
        }

        applicationContext
    }

    void onChange(Map<String, Object> event) {
        if(!isJesqueEnabled(application))
            return

        Class source = event.source
        if(!application.isArtefactOfType(JesqueJobArtefactHandler.TYPE, source)) {
            return
        }

        log.debug("Job ${source} changed. Reloading...")

        ApplicationContext context = event.ctx
        JesqueConfigurationService jesqueConfigurationService = context?.jesqueConfigurationService

        if(context && jesqueConfigurationService) {
            GrailsJesqueJobClass jobClass = application.getJobClass(source.name)
            if(jobClass)
                jesqueConfigurationService.deleteScheduleJob(jobClass)

            jobClass = (GrailsJesqueJobClass)application.addArtefact(JesqueJobArtefactHandler.TYPE, source)

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

    private ConfigObject loadJesqueConfig(ConfigObject jesqueConfig) {
        GroovyClassLoader classLoader = new GroovyClassLoader(getClass().classLoader)

        // merging default jesque config into main application config
        def defaultConfig = new ConfigSlurper(GrailsUtil.environment).parse(classLoader.loadClass('DefaultJesqueConfig'))

        //may look weird, but we must merge the user config into default first so the user overrides default,
        // then merge back into the main to bring default values in that were not overridden
        def mergedConfig = defaultConfig.grails.jesque.merge(jesqueConfig)
        jesqueConfig.merge( mergedConfig )

        return jesqueConfig
    }

    private Boolean isJesqueEnabled(GrailsApplication application) {
        def jesqueConfigMap = application.config.grails.jesque

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
