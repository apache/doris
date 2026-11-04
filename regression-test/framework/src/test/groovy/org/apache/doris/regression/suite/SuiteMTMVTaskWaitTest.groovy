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

class SuiteMTMVTaskWaitTest {
    private static final String SHOW_TASKS = "select TaskId, Status from tasks('type'='mv')"
    private static final Closure<String> IGNORE_LOG = { String message -> }

    private static List<Object> task(String taskId, String status) {
        return [taskId, status] as List<Object>
    }

    private static Closure<List<List<Object>>> polls(List<List<List<Object>>> script) {
        Deque<List<List<Object>>> queue = new ArrayDeque<>(script)
        return { ->
            if (queue.isEmpty()) {
                throw new IllegalStateException("poll script exhausted, the wait did not converge")
            }
            return queue.poll()
        }
    }

    private static String waitFor(Set<String> finished, List<List<List<Object>>> script) {
        return Suite.pollMTMVTaskTerminal(SHOW_TASKS, "test", finished, polls(script), IGNORE_LOG, 1, 5)
                .get(0).toString()
    }

    @Test
    void followsATaskThatWasSeenRunning() {
        assertEquals("X", waitFor(new HashSet<>(), [[task("X", "RUNNING")], [task("X", "SUCCESS")]]))
    }

    @Test
    void confirmsATerminalRowThatWasNeverSeenRunning() {
        assertEquals("X", waitFor(new HashSet<>(), [[task("X", "SUCCESS")], [task("X", "SUCCESS")]]))
    }

    @Test
    void ignoresThePreviousTaskWhileTheNewTaskIsMissing() {
        Set<String> finished = new HashSet<>()
        assertEquals("P", waitFor(finished, [[task("P", "RUNNING")], [task("P", "SUCCESS")]]))

        // X runs and finishes; while it moves from the running list to the MV history the
        // newest row of the MV is P again, and P must not be returned a second time.
        assertEquals("X", waitFor(finished, [[task("X", "RUNNING")], [task("P", "SUCCESS")],
                [task("X", "SUCCESS")]]))
    }

    @Test
    void fallsBackToTheSameTaskWhenNothingNewIsSubmitted() {
        Set<String> finished = new HashSet<>()
        assertEquals("P", waitFor(finished, [[task("P", "RUNNING")], [task("P", "SUCCESS")]]))

        // Nothing new was submitted: the wait must return P instead of polling to the timeout.
        assertEquals("P", waitFor(finished, (1..100).collect { [task("P", "SUCCESS")] }))
    }
}
