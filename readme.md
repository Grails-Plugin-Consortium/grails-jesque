Grails Jesque
=============

[![Build Status](https://travis-ci.org/Grails-Plugin-Consortium/grails-jesque.svg?branch=master)](https://travis-ci.org/Grails-Plugin-Consortium/grails-jesque)

Jesque is an implementation of [Resque](https://github.com/resque/resque) in [Java](http://www.oracle.com/technetwork/java/index.html).
It is fully-interoperable with the [Ruby](http://www.ruby-lang.org/en/) and [Node.js](http://nodejs.org/) ([Coffee-Resque](https://github.com/technoweenie/coffee-resque)) implementations.

The grails jesque plugin uses [jesque](https://github.com/gresrun/jesque) and the grails redis plugin as a dependency.
While it uses jesque for the core functionality it makes it groovier to use in grails.

There is also a grails [jesque-web](https://github.com/michaelcameron/grails-jesque-web) plugin initially ported from the [jesque-web](https://github.com/gresrun/jesque-web) spring-mvc app, which itself was based on the Sinatra web app resque-web in [resque](https://github.com/resque/resque).
Either UI will allow you to see what's in your queues and view and re-process failed messages.

A scheduler (a la Quartz) has been added to support scheduled injection of jobs. The syntax is very similar to the grails Quartz plugin. 


Demo Project
-------------
There is a demo project located [in github](https://github.com/Grails-Plugin-Consortium/grails-jesque-demo).


How do I use it?
----------------
Add the jesque plugin to grails, it will automatically pull in jesque with it's dependencies, and the grails redis plugin.

```bash
dependencies {
    compile('org.grails.plugins:jesque:1.0.9')
}
```

You must also have [redis](http://redis.io) installed in your environment.


Example to enqueue

```groovy

class BackgroundJob {
    def someOtherService //auto-wiring supported

    def perform( arg1, arg2 ) {
        def domainObject = DomainClass.get(arg1) //GORM supported
        domainObject.message = arg2
        domainObject.save()
    }
}

class SomeOtherClass {
    def jesqueService

    def doWorkAsync() {
        jesqueService.enqueue( 'myQueueName', BackgroundJob.simpleName, 1, 'hi there')
    }
}
```

Workers can be started manually by calling

```groovy
    jesqueService.startWorker( 'DemoJesqueJobQueue', DemoJesqueJob.simpleName, DemoJesqueJob )
```

or automatically upon start-up with the following config

```yaml
---
grails:
    redis:
        port: 6379
        host: localhost
    jesque:
        enabled: true
        pruneWorkersOnStartup: true
        createWorkersOnStartup: true
        schedulerThreadActive: true
        delayedJobThreadActive: true
        startPaused: false
        autoFlush: true
        workers:
            DemoJesqueJobPool:
                queueNames: DemoJesqueJobQueue
                jobTypes:
                    - org.grails.jesque.demo.DemoJesqueJob
                    - org.grails.jesque.demo.DemoTwoJesqueJob
```

The redis pool used is configured in the [redis](https://github.com/grails-plugins/grails-redis) plugin:

```yaml
grails:
    redis:
        host: localhost
        port: 6379
```

Or using sentinels

```yaml
grails:
    redis:
        sentinels:
            - 10.0.0.1:26379
            - 10.0.0.2:26379
        masterName: foobar        
```

Jobs
----
Jobs should be placed in grails-app/jobs similar to the [Quartz](http://grails.org/plugin/quartz) plugin.
However, to not clash with quartz, and to retain similarties with resque, the method to execute must be called perform.

You can run the script create-jesque-job to create a shell of a job for you automatically.  The
following will create a BackgroundJob in the grails-app/jobs folder.

```bash
grails create-jesque-job org.grails.jesque.demo.DemoJesqueJob
```

```groovy
package org.grails.jesque.demo

import groovy.util.logging.Slf4j

@Slf4j
class DemoJesqueJob {

    static queue = 'DemoJesqueJobQueue'
    static workerPool = 'DemoJesqueJobPool'

    static triggers = {
        cron name: 'DemoJesqueJobTrigger', cronExpression: '0/15 * * * * ? *'
    }

    def perform() {
        log.info "Executing Job"
    }
}
```

Roadmap
----
* Upgrade custom Listener and Worker to grails 3 support
* Ability to execute methods on services without creating a job object
* Wrap above ability automatically with annotation and dynamically creating a method with the same name + "Async" suffix
* Create grails/groovy docs (gdoc?) to extensively document options
* Dynamic wake time of delayed jobs thread to reduce polling

Release Notes
=============

* 0.2.0 - released 2011-10-17
    * First publicly announced version
* 0.3.0 - released 2012-02-03
    * First implementation of scheduler
* 0.4.0 - released 2012-06-06
    * Gracefully shutdown threads
    * Handle changes to scheduled jobs during development
    * Upgrade to Jedis 2.1.0 (Note, this is binary incompatible with Jedis 2.0.0, Grails Jesque < 0.4.0 will not run with Jedis 2.1.0 and >= 0.4.0 must run with Jedis >= 2.1.0)
    * Change artefact name from "Job" to "JesqueJob" to not clash with other grails plugins (e.g. quartz) that use an artefact name of "Job" (issue #14)
* 0.5.0 - released 2012-11-20
    * Add delayed job queue implementation
    * Ability to use grailsConfiguration in triggers closure
* 0.5.1 - released 2012-11-27
    * Add some logging to the exception handler to track down shutdown issues
    * Add ability to prevent jesque from starting via config ([issue #22](https://github.com/michaelcameron/grails-jesque/issues/23))
* 0.6.0 - released 2013-03-05
    * Upgrade to Jesque 1.3.1
    * Fix edge case with errors after calling jedis.multi() but before calling trans.exec() ([issue #26](https://github.com/michaelcameron/grails-jesque/issues/26))
* 0.6.1 - release 2013-03-22
    * Use Jesque's admin functionality to allow start/stop/pause of workers in a cluster
* 0.6.2 - release 2013-06-13
    * Add ability to specify Redis DB
* 0.8.0 - TBD - 0.8.0-SNAPSHOT version available now
    * Added priorityEnqueue methods to JesqueService
    * Added ability to define a custom WorkerListener
    * Added ability to define a custom WorkerImpl
    * Added ability to specify a custom job exception handler
    * Updated to grails version 2.3.8
    * Move tests outside of main project
    * Remove test and hibernate dependencies
    * Updated dependencies
* 1.0.0 - Grails 3.x
    * Some features have not been ported over yet such as custom Worker and Listener
    * Design was changed to use list based jobTypes instead of Map for simplicity in YAML
* 1.0.1 - Bug fix for NPE
* 1.0.6 - Background the starting of the jobs to speed up booting
* 1.1.0 - Adding sentinel support
* 1.1.2 - Bug fix for wiring jobs
* 1.1.3 - Bug fix for worker persistence context


License
-------
Copyright 2011 Michael Cameron

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   <http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
