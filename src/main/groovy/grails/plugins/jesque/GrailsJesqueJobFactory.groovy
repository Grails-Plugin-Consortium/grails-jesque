package grails.plugins.jesque

import grails.core.GrailsApplication
import net.greghaines.jesque.Job
import net.greghaines.jesque.utils.ReflectionUtils
import net.greghaines.jesque.worker.MapBasedJobFactory
import net.greghaines.jesque.worker.UnpermittedJobException

/**
 * Job Factory that knows how to materialize grails jobs.
 */
class GrailsJesqueJobFactory extends MapBasedJobFactory {

    GrailsApplication grailsApplication

    GrailsJesqueJobFactory(Map<String, ? extends Class<?>> jobTypes, final GrailsApplication grailsApplication) {
        super(jobTypes)
        this.grailsApplication = grailsApplication
    }

    @Override
    protected void checkJobType(final String jobName, final Class<?> jobType) {
        if (jobName == null) {
            throw new IllegalArgumentException("jobName must not be null")
        }
        if (jobType == null) {
            throw new IllegalArgumentException("jobType must not be null")
        }
    }

    @Override
    Object materializeJob(final Job job) throws Exception {
        Class jobClass = jobTypes[job.className]
        if (!jobClass) {
            throw new UnpermittedJobException(job.className)
        }

        def instance = grailsApplication.mainContext.getBean(jobClass.canonicalName)
        if (job.vars && !job.vars.isEmpty()) {
            ReflectionUtils.invokeSetters(instance, job.vars)
        }
        return instance
    }

}