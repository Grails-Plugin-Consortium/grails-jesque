package ${packageName}

class ${className}Job {
    static triggers = {
        simple repeatInterval: 5000l // execute job once in 5 seconds
    }

    def perform() {
        // execute job
    }
}
