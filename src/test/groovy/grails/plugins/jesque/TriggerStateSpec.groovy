package grails.plugins.jesque

import spock.lang.Specification
import spock.lang.Unroll

class TriggerStateSpec extends Specification {
    def "ensure all enums are valid"() {
        expect:
        TriggerState.Waiting.name == 'WAITING'
        TriggerState.Acquired.name == 'ACQUIRED'
    }

    @Unroll
    def "find all should return the value #name equals #expectedValue"() {
        when:
        TriggerState triggerState = TriggerState.findByName(name)

        then:
        triggerState == expectedValue

        where:
        name       | expectedValue
        'WAITING'  | TriggerState.Waiting
        'ACQUIRED' | TriggerState.Acquired
        ''         | null
        'asdf'     | null
        'WAITING1' | null
        null       | null
    }
}
