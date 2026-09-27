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

/**
 * The uncaught exceptions of the threads a suite started, for that suite's verdict.
 *
 * A suite that starts a Thread itself and join()s it gets nothing back from a failure on it: join() waits,
 * it does not rethrow, and the framework runs no code on such a thread (Suite.thread() reports through its
 * future; a raw Thread has none). Until this class, Awaitility filled that gap by accident: every await()
 * installs itself as the JVM's default uncaught-exception handler while it waits and rethrows, from the
 * awaiting thread, what any thread of the JVM threw meanwhile - so a thread's failure failed whichever
 * suite happened to be awaiting, its own only by luck, and no suite at all when none was
 * (RegressionTest.initGroovyEnv turns that off). The JVM's default handler is now uncaughtException():
 * the dying thread carries in OWNER - inherited at construction from the suite's thread, through any depth
 * of threads started from it - the failures of the suite that started it, and its failure is recorded
 * there; Suite.doLazyCheck() throws the first one once the suite's body has returned, that is after every
 * thread the suite joined has ended. A failure after that (a thread the suite left running past its end,
 * like test_active_queries's poller), or on a thread no suite started, fails nothing; every one of them
 * is logged.
 */
@Slf4j
@CompileStatic
class UncaughtThreadFailures {
    // The failures of the suite running on this thread, or that started it, transitively: set on the
    // suite's own thread for the suite's run (ScriptContext.createAndRunSuite), copied into every thread
    // constructed on it.
    static final InheritableThreadLocal<UncaughtThreadFailures> OWNER = new InheritableThreadLocal<>()

    private final String suiteName
    private final List<Throwable> failures = []
    private boolean accepting = true

    UncaughtThreadFailures(String suiteName) {
        this.suiteName = suiteName
    }

    // Records the failure of thread as the suite's. False once takeAll() ran: the suite's verdict is taken.
    synchronized boolean add(Thread thread, Throwable throwable) {
        if (!accepting) {
            return false
        }
        failures.add(new IllegalStateException(
                "Thread ${thread.name} of suite ${suiteName} died with an uncaught exception".toString(), throwable))
        return true
    }

    // Takes what was recorded, and refuses everything after: the suite's verdict is being taken.
    synchronized List<Throwable> takeAll() {
        accepting = false
        List<Throwable> taken = new ArrayList<>(failures)
        failures.clear()
        return taken
    }

    static void installAsDefaultHandler() {
        Thread.setDefaultUncaughtExceptionHandler(
                { Thread thread, Throwable throwable -> uncaughtException(thread, throwable) }
                        as Thread.UncaughtExceptionHandler)
    }

    // Runs on the dying thread, as the JVM's default handler.
    static void uncaughtException(Thread thread, Throwable throwable) {
        UncaughtThreadFailures owner = OWNER.get()
        if (owner == null) {
            log.error("Uncaught exception in thread ${thread.name}, which no suite started; it fails no suite"
                    .toString(), throwable)
        } else if (owner.add(thread, throwable)) {
            log.error(("Uncaught exception in thread ${thread.name}, started by suite ${owner.suiteName}, "
                    + "which fails the suite").toString(), throwable)
        } else {
            log.error(("Uncaught exception in thread ${thread.name}, started by suite ${owner.suiteName} "
                    + "whose verdict is already taken; it fails no suite").toString(), throwable)
        }
    }
}
