package grails.plugins.jesque

enum TriggerState {
    Waiting('WAITING'),
    Acquired('ACQUIRED')

    String name

    TriggerState(String name) {
        this.name = name
    }

    static TriggerState findByName(String name) {
        values().find{ it.name == name }
    }
}
