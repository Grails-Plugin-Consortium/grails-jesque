package grails.plugins.jesque

enum TriggerField {
    NextFireTime('nextFireTime'),
    State('state'),
    AcquiredBy('acquiredBy')

    String name

    TriggerField(String name) {
        this.name = name
    }
}