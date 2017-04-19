package grails.plugins.jesque;

import grails.core.InjectableGrailsClass;

import java.util.List;
import java.util.Map;

public interface GrailsJesqueJobClass extends InjectableGrailsClass {

    Map getTriggers();

    String getQueue();

    String getWorkerPool();

    List getJobNames();
}
