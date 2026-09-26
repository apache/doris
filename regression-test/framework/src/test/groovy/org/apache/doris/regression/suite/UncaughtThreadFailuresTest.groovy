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

import org.junit.jupiter.api.Test

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertSame
import static org.junit.jupiter.api.Assertions.assertTrue

class UncaughtThreadFailuresTest {
    // Runs body to its end on a thread constructed while owner is the OWNER - as a suite's thread starts
    // a Thread of its own - with the handler this class installs as the JVM's default.
    private static void onAThreadStartedUnder(UncaughtThreadFailures owner, Closure body) {
        Thread.UncaughtExceptionHandler previous = Thread.getDefaultUncaughtExceptionHandler()
        UncaughtThreadFailures.installAsDefaultHandler()
        UncaughtThreadFailures.OWNER.set(owner)
        try {
            Thread thread = new Thread(body as Runnable, "worker")
            thread.start()
            thread.join()
        } finally {
            UncaughtThreadFailures.OWNER.remove()
            Thread.setDefaultUncaughtExceptionHandler(previous)
        }
    }

    @Test
    void aFailureOnAThreadTheSuiteStartedIsTheSuites() {
        UncaughtThreadFailures failures = new UncaughtThreadFailures("the_suite")
        AssertionError failed = new AssertionError("expected 1 row")

        onAThreadStartedUnder(failures) { throw failed }

        List<Throwable> taken = failures.takeAll()
        assertEquals(1, taken.size())
        assertSame(failed, taken[0].cause)
        assertTrue(taken[0].message.contains("worker"))
        assertTrue(taken[0].message.contains("the_suite"))
    }

    @Test
    void aThreadStartedFromAThreadOfTheSuiteCountsToo() {
        UncaughtThreadFailures failures = new UncaughtThreadFailures("the_suite")
        RuntimeException failed = new RuntimeException("deep")

        onAThreadStartedUnder(failures) {
            Thread grandchild = new Thread({ throw failed } as Runnable, "grandchild")
            grandchild.start()
            grandchild.join()
        }

        List<Throwable> taken = failures.takeAll()
        assertEquals(1, taken.size())
        assertSame(failed, taken[0].cause)
        assertTrue(taken[0].message.contains("grandchild"))
    }

    @Test
    void aFailureAfterTheVerdictIsTakenIsNotRecorded() {
        UncaughtThreadFailures failures = new UncaughtThreadFailures("the_suite")
        assertTrue(failures.takeAll().isEmpty())

        onAThreadStartedUnder(failures) { throw new RuntimeException("late") }

        assertTrue(failures.takeAll().isEmpty())
        assertFalse(failures.add(Thread.currentThread(), new RuntimeException("later")))
    }

    @Test
    void aThreadNoSuiteStartedFailsNothing() {
        onAThreadStartedUnder(null) { throw new RuntimeException("stray") }
    }
}
