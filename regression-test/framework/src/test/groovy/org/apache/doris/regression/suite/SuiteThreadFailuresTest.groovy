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

import org.apache.doris.regression.Config
import org.apache.doris.regression.suite.event.EventListener
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.nio.file.Path
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertNull
import static org.junit.jupiter.api.Assertions.assertTrue

// A suite is failed by the uncaught exception of a thread it started, however the thread came to be - on the
// suite's thread or in a task of Suite.thread() - and a suite failing on something else carries it along. The
// suites run the way RegressionTest runs them, with the handler it installs as the JVM's default.
class SuiteThreadFailuresTest {
    @TempDir
    Path tempDir

    private final ExecutorService suiteExecutors = Executors.newSingleThreadExecutor()
    // One worker, as the run-wide pool of Suite.thread() has a few for every suite of the run.
    private final ExecutorService actionExecutors = Executors.newFixedThreadPool(1)
    private Thread.UncaughtExceptionHandler previousHandler

    @BeforeEach
    void installHandler() {
        previousHandler = Thread.getDefaultUncaughtExceptionHandler()
        UncaughtThreadFailures.installAsDefaultHandler()
    }

    @AfterEach
    void restoreHandler() {
        Thread.setDefaultUncaughtExceptionHandler(previousHandler)
        suiteExecutors.shutdownNow()
        actionExecutors.shutdownNow()
    }

    // Records how each suite ended.
    private static class SuiteVerdicts implements EventListener {
        final List<Throwable> failures = Collections.synchronizedList([])
        final List<Boolean> successes = Collections.synchronizedList([])

        void onScriptStarted(ScriptContext scriptContext) {}
        void onScriptFailed(ScriptContext scriptContext, Throwable t) {}
        void onScriptFinished(ScriptContext scriptContext, long elapsed) {}
        void onSuiteStarted(SuiteContext suiteContext) {}
        void onSuiteFailed(SuiteContext suiteContext, Throwable t) { failures.add(t) }
        void onSuiteFinished(SuiteContext suiteContext, boolean success, long elapsed) { successes.add(success) }
        void onThreadStarted(SuiteContext suiteContext) {}
        void onThreadFailed(SuiteContext suiteContext, Throwable t) {}
        void onThreadFinished(SuiteContext suiteContext, long elapsed) {}
    }

    // Runs body as the suite name of a suite file of its own and returns what the suite failed with, null if it
    // passed.
    private Throwable runSuite(String name, Closure body) {
        File suiteDir = tempDir.resolve("suites").toFile()
        suiteDir.mkdirs()
        File suiteFile = new File(suiteDir, "${name}.groovy")
        assertTrue(suiteFile.createNewFile())
        Config config = new Config()
        config.suitePath = suiteDir.toString()
        config.dataPath = tempDir.resolve("data").toString()
        config.realDataPath = tempDir.resolve("real-data").toString()
        config.defaultDb = "regression_test"
        SuiteVerdicts verdicts = new SuiteVerdicts()
        ScriptContext scriptContext = new ScriptContext(
                suiteFile, suiteExecutors, actionExecutors, config, [verdicts], { true })
        scriptContext.createAndRunSuite(name, "p0", body)
        // Waits for the suite to be over.
        scriptContext.close()
        assertEquals([verdicts.failures.isEmpty()], verdicts.successes)
        return verdicts.failures.isEmpty() ? null : verdicts.failures[0]
    }

    @Test
    void aSuiteWhoseJoinedThreadDiesFails() {
        Throwable failure = runSuite("joined_thread_dies") {
            Thread worker = new Thread({ throw new IllegalStateException("statement failed") } as Runnable, "worker")
            worker.start()
            worker.join()
        }

        assertNotNull(failure)
        assertTrue(failure.message.contains("Thread worker of suite joined_thread_dies died"), failure.message)
        assertEquals("statement failed", failure.cause.message)
    }

    @Test
    void aSuiteWhoseThreadsAllEndWellPasses() {
        assertNull(runSuite("joined_thread_ends_well") {
            Thread worker = new Thread({ } as Runnable, "worker")
            worker.start()
            worker.join()
        })
    }

    @Test
    void aThreadATaskOfSuiteThreadStartsCountsForTheSuiteThatSubmittedTheTask() {
        // The worker of the pool is constructed while another suite submits to it, and keeps that suite's
        // collector for good.
        UncaughtThreadFailures anotherSuite = new UncaughtThreadFailures("another_suite")
        UncaughtThreadFailures.OWNER.set(anotherSuite)
        try {
            actionExecutors.submit({ } as Runnable).get()
        } finally {
            UncaughtThreadFailures.OWNER.remove()
        }

        Throwable failure = runSuite("task_starts_a_thread") {
            thread {
                Thread started = new Thread(
                        { throw new IllegalStateException("statement failed") } as Runnable, "started_by_task")
                started.start()
                started.join()
            }.get()
        }

        assertNotNull(failure)
        assertTrue(failure.message.contains("Thread started_by_task of suite task_starts_a_thread died"),
                failure.message)
        assertTrue(anotherSuite.takeAll().isEmpty())
    }

    @Test
    void aThreadALazyCheckLeftRunningCountsOnceTheChecksAreOver() {
        // The body returns at once; the thread its lazy check starts dies afterwards.
        Throwable failure = runSuite("lazy_check_starts_a_thread") {
            lazyCheckThread {
                Thread.sleep(300)
                Thread started = new Thread(
                        { throw new IllegalStateException("statement failed") } as Runnable, "started_late")
                started.start()
                started.join()
            }
        }

        assertNotNull(failure)
        assertTrue(failure.message.contains("Thread started_late of suite lazy_check_starts_a_thread died"),
                failure.message)
    }

    @Test
    void aSuiteFailingOnSomethingElseCarriesWhatItsThreadsDiedOf() {
        // A thread whose statement failed leaves the body no rows, and the body then fails on that.
        Throwable failure = runSuite("fails_on_what_its_thread_left") {
            List<Object> rows = null
            Thread worker = new Thread({ throw new IllegalStateException("statement failed") } as Runnable, "worker")
            worker.start()
            worker.join()
            rows.size()
        }

        assertTrue(failure instanceof NullPointerException, String.valueOf(failure))
        assertEquals(1, failure.suppressed.length)
        assertTrue(failure.suppressed[0].message.contains("Thread worker of suite fails_on_what_its_thread_left"))
        assertEquals("statement failed", failure.suppressed[0].cause.message)
    }
}
