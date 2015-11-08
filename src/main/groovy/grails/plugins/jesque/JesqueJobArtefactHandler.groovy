package grails.plugins.jesque

import grails.core.ArtefactHandlerAdapter
import org.codehaus.groovy.ast.ClassNode
import org.grails.compiler.injection.GrailsASTUtils
import org.springframework.util.ReflectionUtils

import java.lang.reflect.Method
import java.util.regex.Pattern

import static org.grails.io.support.GrailsResourceUtils.GRAILS_APP_DIR
import static org.grails.io.support.GrailsResourceUtils.REGEX_FILE_SEPARATOR

public class JesqueJobArtefactHandler extends ArtefactHandlerAdapter {

    public static final String TYPE = "JesqueJob";
    public static final String PERFORM = "perform";

    static final String JOB = "Job";
    static Pattern JOB_PATH_PATTERN = Pattern.compile(".+" + REGEX_FILE_SEPARATOR + GRAILS_APP_DIR + REGEX_FILE_SEPARATOR + "jobs" + REGEX_FILE_SEPARATOR + "(.+)\\.(groovy)");

    public JesqueJobArtefactHandler() {
        super(TYPE, GrailsJesqueJobClass.class, DefaultGrailsJesqueJobClass.class, TYPE)
    }

    boolean isArtefact(ClassNode classNode) {
        if (classNode == null ||
                !isValidArtefactClassNode(classNode, classNode.getModifiers()) ||
                !classNode.getName().endsWith(JOB) ||
                !classNode.getMethods(PERFORM)) {
            return false
        }

        URL url = GrailsASTUtils.getSourceUrl(classNode)

        url && JOB_PATH_PATTERN.matcher(url.getFile()).find()
    }

    boolean isArtefactClass(Class clazz) {
        // class shouldn't be null and should ends with Job suffix
        if (clazz == null || !clazz.getName().endsWith(JOB)) return false
        // and should have one of execute() or execute(JobExecutionContext) methods defined
        Method method = ReflectionUtils.findMethod(clazz, PERFORM)
        if (method == null) {
            // we're using Object as a param here to allow groovy-style 'def execute(param)' method
            method = ReflectionUtils.findMethod(clazz, PERFORM, [Object] as Class[])
        }
        method != null
    }
}
