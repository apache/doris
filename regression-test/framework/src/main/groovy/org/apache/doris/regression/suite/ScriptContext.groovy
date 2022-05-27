// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.regression.suite

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.doris.regression.Config
import org.apache.doris.regression.suite.event.EventListener

import java.lang.reflect.UndeclaredThrowableException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Phaser
import java.util.function.Function

@Slf4j
@CompileStatic
class ScriptContext implements Closeable {
    public final File file
    public final Config config
    public final File dataPath
    public final File outputFile
    public final String name
    public final String flowName
    public final String flowId
    public final List<EventListener> eventListeners
    public final ExecutorService suiteExecutors
    public final ExecutorService actionExecutors
    public final Closure<Boolean> suiteFilter
    private long startTime
    private long finishTime
    private final Thread createContextThread
    private final Phaser phaser = new Phaser(1) // register main script thread

    ScriptContext(File file, ExecutorService suiteExecutors, ExecutorService actionExecutors, Config config,
                  List<EventListener> eventListeners, Closure<Boolean> suiteFilter) {
        this.file = file
        this.config = config
        this.name = this.flowId = new File(config.suitePath).relativePath(file)
        this.flowName = '' // force set to empty string, for teamcity
        this.eventListeners = eventListeners
        this.suiteExecutors = suiteExecutors
        this.actionExecutors = actionExecutors
        this.suiteFilter = suiteFilter
        this.createContextThread = Thread.currentThread()

        def path = new File(config.suitePath).relativePath(file)
        def outputRelativePath = path.substring(0, path.lastIndexOf(".")) + ".out"
        this.outputFile = new File(new File(config.dataPath), outputRelativePath)
        this.dataPath = this.outputFile.getParentFile().getCanonicalFile()
    }

    private final synchronized Suite newSuite(String suiteName, String group) {
        if (Thread.currentThread() != createContextThread) {
            throw new IllegalStateException("Can not create suite in another thread")
        }
        // if close scriptContext, the phaser will become to 1
        if (phaser.getPhase() > 0) {
            throw new IllegalStateException("Can not create suite after close scriptContext")
        }

        SuiteContext suiteContext = new SuiteContext(file, suiteName, group, this,
                suiteExecutors, actionExecutors, config) {
            @Override
            void close() {
                try {
                    super.close()
                } finally {
                    // count down
                    phaser.arriveAndDeregister()
                }
            }
        }
        Suite suite = new Suite(suiteName, group, suiteContext)
        // count up, register suite thread
        phaser.register()
        return suite
    }

    synchronized final Suite createAndRunSuite(String suiteName, String group, Closure suiteBody) {
        Suite suite = newSuite(suiteName, group)

        suiteExecutors.submit {
            suite.context.start {
                log.info("Run ${suiteName} in ${file}".toString())

                try {
                    // delegate closure
                    suiteBody.setResolveStrategy(Closure.DELEGATE_FIRST)
                    suiteBody.setDelegate(suite)

                    // execute body
                    suiteBody.call(suite)

                    // check
                    suite.doLazyCheck()

                    // success
                    try {
                        suite.successCallbacks.each { it(suite) }
                    } catch (Throwable t) {
                        throw new IllegalStateException("Run suite success callbacks failed", t)
                    }

                    log.info("Run ${suiteName} in ${file.absolutePath} succeed".toString())
                } catch (Throwable t) {
                    log.error("Run ${suiteName} in ${file.absolutePath} failed".toString(), t)
                    try {
                        // fail
                        if (suite != null) {
                            suite.failCallbacks.each { it(suite) }
                        }
                    } catch (Throwable ex) {
                        log.error("Run suite fail callbacks failed", ex)
                        t.addSuppressed(ex)
                    }
                    throw t
                } finally {
                    try {
                        // finish
                        if (suite != null) {
                            suite.finishCallbacks.each { it(suite) }
                        }
                    } catch (Throwable t) {
                        log.error("Run suite finish callbacks failed", t)
                    }
                }
            }
        }
        return suite
    }

    public <T> T start(Function<ScriptContext, T> action) {
        this.startTime = System.currentTimeMillis()
        eventListeners.each {it.onScriptStarted(this) }
        this.withCloseable { scriptContext ->
            try {
                action.apply(scriptContext)
            } catch (Throwable t) {
                if (t instanceof UndeclaredThrowableException) {
                    t = ((UndeclaredThrowableException) t).undeclaredThrowable
                }
                log.error("Fatal: run SuiteScript in ${file} failed".toString(), t)
                eventListeners.each { it.onScriptFailed(scriptContext, t) }
                null
            }
        }
    }

    synchronized void close() {
        if (phaser.getPhase() > 0) {
            throw new IllegalStateException("Can not close scriptContext twice")
        }
        // wait for all suite finished, phase += 1
        phaser.arriveAndAwaitAdvance()

        this.finishTime = System.currentTimeMillis()
        long elapsed = finishTime - startTime
        eventListeners.each {it.onScriptFinished(this, elapsed) }
    }
}
