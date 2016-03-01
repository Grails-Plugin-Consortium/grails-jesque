package grails.plugins.jesque

import spock.lang.Specification

class JobFieldSpec extends Specification {
    def "ensure all enums are valid"() {
        expect:
            JobField.Args.name == 'args'
            JobField.CronExpression.name == 'cronExpression'
            JobField.JesqueJobName.name == 'jesqueJobName'
            JobField.JesqueJobQueue.name == 'jesqueJobQueue'
    }
}
