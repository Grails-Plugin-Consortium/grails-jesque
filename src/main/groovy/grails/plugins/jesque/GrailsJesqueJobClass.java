package grails.plugins.jesque;

import grails.core.InjectableGrailsClass;

import java.util.List;
import java.util.Map;

public interface GrailsJesqueJobClass extends InjectableGrailsClass {

    public Map getTriggers();
    
    public String getQueue();

    public String getWorkerPool();

    public List getJobNames();
}
