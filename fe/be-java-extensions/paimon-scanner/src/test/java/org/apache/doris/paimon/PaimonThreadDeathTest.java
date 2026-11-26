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

package org.apache.doris.paimon;

import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.FatalExitExceptionHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A paimon pool thread that dies must not take the process with it. Paimon's own handler for these
 * threads calls {@code System.exit}, and inside BE that runs the C++ global destructors under a BE
 * that is still serving; the plugin ships a handler of the same name that only logs.
 *
 * <p>Should paimon's handler come back - the patched class dropped, or found after paimon-common's -
 * these tests do not fail with an assertion: the JVM running them exits, and surefire reports a
 * forked VM that terminated without saying goodbye.
 */
public class PaimonThreadDeathTest {

    @Test
    public void threadThatDiesOfAnUncaughtErrorLeavesTheProcessRunning() throws Exception {
        // The factory behind the pools of AsyncRecordReader and ParallelExecution.
        Thread thread = new ExecutorThreadFactory("doris-fatal-exit-test").newThread(() -> {
            throw new OutOfMemoryError("simulated: Java heap space");
        });
        Assertions.assertSame(FatalExitExceptionHandler.INSTANCE, thread.getUncaughtExceptionHandler(),
                "paimon no longer hands its threads this handler; the patch may not be needed any more");

        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(60));
        Assertions.assertFalse(thread.isAlive(), "the thread was meant to die");
    }

    @Test
    public void poolKeepsServingAfterOneOfItsThreadsDies() throws Exception {
        ExecutorService pool = Executors.newCachedThreadPool(new ExecutorThreadFactory("doris-fatal-exit-pool"));
        try {
            // Thrown outside any future, the way an OutOfMemoryError hits a worker waiting for work:
            // it reaches the thread's uncaught exception handler and ends that worker.
            pool.execute(() -> {
                throw new OutOfMemoryError("simulated: Java heap space");
            });
            Assertions.assertEquals("served", pool.submit(() -> "served").get(60, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
    }
}
