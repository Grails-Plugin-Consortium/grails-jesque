package grails.plugins.jesque

enum JobField {
    CronExpression('cronExpression'),
    Args('args'),
    JesqueJobName('jesqueJobName'),
    JesqueJobQueue('jesqueJobQueue')

    String name

    JobField(String name) {
        this.name = name
    }
}
