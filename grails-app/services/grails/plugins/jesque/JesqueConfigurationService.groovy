package grails.plugins.jesque

import grails.core.GrailsApplication
import groovy.util.logging.Slf4j
import org.joda.time.DateTimeZone

@Slf4j
class JesqueConfigurationService {
	GrailsApplication grailsApplication
	def jesqueSchedulerService

	Boolean validateConfig(ConfigObject jesqueConfigMap) {
		jesqueConfigMap?.workers?.keySet()?.each { String workerPoolName ->
			log.info "Running validation on $workerPoolName"
			def value = jesqueConfigMap?.workers[workerPoolName]
			if (value.workers && !(value.workers instanceof Integer))
				throw new Exception("Invalid worker count ${value.workers} for pool $workerPoolName, expecting Integer")

			def queueNames = value.queueNames
			if (queueNames && !((queueNames instanceof String) || (queueNames instanceof List<String>)))
				throw new Exception("Invalid queueNames ($queueNames) for pool $workerPoolName, must be a String or a List<String>")

			def jobTypes = value.jobTypes
			if (jobTypes && !(jobTypes instanceof List))
				throw new Exception("Invalid jobTypes ($jobTypes) for pool $workerPoolName, must be a list")

			jobTypes?.each { String jobClass ->
				Class jobClassClass = grailsApplication.getClassForName(jobClass)
				if (!(jobClassClass instanceof Class))
					throw new Exception("Invalid jobClass for jobName ($jobClass) for pool $workerPoolName, the value must be a Class with full path ex. com.foo.SomeJob")
			}
		}

		return true
	}

	void mergeClassConfigurationIntoConfigMap(ConfigObject jesqueConfigMap) {
		grailsApplication.jesqueJobClasses?.each { GrailsJesqueJobClass jobArtefact ->
			def alreadyConfiguredPool = jesqueConfigMap?.workers?.keySet()?.find { workerName ->
				jesqueConfigMap?.workers[workerName]?.jobTypes?.any { String jobName ->
					grailsApplication.getClassForName(jobName) == jobArtefact.clazz
				}
			}

			if (alreadyConfiguredPool) {
				def configuredPool = jesqueConfigMap?.workers[alreadyConfiguredPool]
				//already configured, make sure pool name matches, and queue is listed, otherwise error, do nothing else
				if (jobArtefact.workerPool != GrailsJesqueJobClassProperty.DEFAULT_WORKER_POOL && jobArtefact.workerPool != alreadyConfiguredPool)
					throw new Exception("Class ${jobArtefact.fullName} specifies worker pool ${jobArtefact.workerPool} but configuration file has ${alreadyConfiguredPool}")

				if (configuredPool.queueNames instanceof String)
					configuredPool.queueNames = configuredPool.queueNames

				if (jobArtefact.queue != GrailsJesqueJobClassProperty.DEFAULT_QUEUE && !(jobArtefact.queue in configuredPool.queueNames))
					throw new Exception("Class ${jobArtefact.fullName} specifies queue name ${jobArtefact.queue} but worker pool ${alreadyConfiguredPool} has ${configuredPool.queueNames}")

				return
			}

			def workerPoolConfig = jesqueConfigMap?.workers?."${jobArtefact.workerPool}"
			if (workerPoolConfig) {
				if (!workerPoolConfig.queueNames)
					workerPoolConfig.queueNames = []
				if (!workerPoolConfig.jobTypes)
					workerPoolConfig.jobTypes = []

				if (workerPoolConfig.queueNames instanceof String)
					workerPoolConfig.queueNames = workerPoolConfig.queueNames

				jobArtefact.jobNames.each { jobName ->
					log.info "Adding job ${jobArtefact.clazz.toString()} to workerPoolConfig"
					workerPoolConfig.jobTypes << jobArtefact.clazz.toString()
				}

				if (!(jobArtefact.queue in workerPoolConfig.queueNames))
					workerPoolConfig.queueNames += jobArtefact.queue
			}
		}
	}

	void scheduleJob(GrailsJesqueJobClass jobClass) {
		log.info("Scheduling ${jobClass.fullName}")

		jobClass.triggers.each { key, trigger ->
			String name = trigger.triggerAttributes[GrailsJesqueJobClassProperty.NAME]
			String cronExpression = trigger.triggerAttributes[GrailsJesqueJobClassProperty.CRON_EXPRESSION]
			DateTimeZone timeZone = DateTimeZone.forID(trigger.triggerAttributes[GrailsJesqueJobClassProperty.TIMEZONE])
			String queue = trigger.triggerAttributes[GrailsJesqueJobClassProperty.JESQUE_QUEUE]
			String jesqueJobName = trigger.triggerAttributes[GrailsJesqueJobClassProperty.JESQUE_JOB_NAME]
			List jesqueJobArguments = trigger.triggerAttributes[GrailsJesqueJobClassProperty.JESQUE_JOB_ARGUMENTS] ?: []

			jesqueSchedulerService.schedule(name, cronExpression, timeZone, queue, jesqueJobName, jesqueJobArguments)
		}
	}

	void deleteScheduleJob(GrailsJesqueJobClass jobClass) {
		log.info("Remove schedule for ${jobClass.fullName}")

		jobClass.triggers.each { key, trigger ->
			String name = trigger.triggerAttributes[GrailsJesqueJobClassProperty.NAME]

			jesqueSchedulerService.deleteSchedule(name)
		}
	}
}
