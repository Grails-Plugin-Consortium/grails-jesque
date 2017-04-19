package grails.plugins.jesque

import net.greghaines.jesque.worker.MapBasedJobFactory

import java.util.Map.Entry

/**
 * MapBasedJobFactory uses a map of job names and types to materialize jobs.
 */
public class GrailsJesqueJobFactory extends MapBasedJobFactory {

    /**
     * Constructor.
     * @param jobTypes the map of job names and types to execute
     */
    GrailsJesqueJobFactory(Map<String, ? extends Class<?>> jobTypes) {
        super(jobTypes)
    }

    protected void checkJobTypes(final Map<String, ? extends Class<?>> jobTypes) {
        if (jobTypes == null) {
            throw new IllegalArgumentException("jobTypes must not be null");
        }
        for (final Entry<String, ? extends Class<?>> entry : jobTypes.entrySet()) {
            try {
                checkJobType(entry.getKey(), entry.getValue());
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("jobTypes contained invalid value", iae);
            }
        }
    }

    /**
     * Determine if a job name and job type are valid.
     * @param jobName the name of the job
     * @param jobType the class of the job
     * @throws IllegalArgumentException if the name or type are invalid
     */
    protected void checkJobType(final String jobName, final Class<?> jobType) {
        if (jobName == null) {
            throw new IllegalArgumentException("jobName must not be null");
        }
        if (jobType == null) {
            throw new IllegalArgumentException("jobType must not be null");
        }
    }
}
