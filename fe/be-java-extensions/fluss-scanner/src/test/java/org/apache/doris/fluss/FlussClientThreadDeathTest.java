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

package org.apache.doris.fluss;

import org.apache.fluss.utils.FatalExitExceptionHandler;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A fluss client thread that dies must not take the process with it. Fluss's own handler for these
 * threads calls {@code System.exit}, and inside BE that runs the C++ global destructors under a BE
 * that is still serving; the plugin ships a handler of the same name that only logs.
 *
 * <p>Should fluss's handler come back - the patched class dropped, or found after fluss-client's -
 * these tests do not fail with an assertion: the JVM running them exits, and surefire reports a
 * forked VM that terminated without saying goodbye.
 */
public class FlussClientThreadDeathTest {

    @Test
    public void clientThreadThatDiesOfAnUncaughtErrorLeavesTheProcessRunning() throws Exception {
        // The factory behind fluss's admin refresh, remote file download and token renewal threads.
        Thread thread = new ExecutorThreadFactory("doris-fatal-exit-test").newThread(() -> {
            throw new OutOfMemoryError("simulated: Java heap space");
        });
        Assertions.assertSame(FatalExitExceptionHandler.INSTANCE, thread.getUncaughtExceptionHandler(),
                "fluss no longer hands its threads this handler; the patch may not be needed any more");

        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(60));
        Assertions.assertFalse(thread.isAlive(), "the thread was meant to die");
    }

    @Test
    public void failedFutureFlussAssertsNeverFailsLeavesTheProcessRunning() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        FutureUtils.assertNoException(future);
        future.completeExceptionally(new IllegalStateException("simulated"));
        Assertions.assertTrue(future.isCompletedExceptionally());
    }
}
