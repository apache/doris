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

package org.apache.doris.qe;

import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.split.SplitAssignment;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class QueryScanTaskScopeTest {
    @Test
    void registrationRacingCancellationCannotLoseTheStopSignal() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int attempt = 0; attempt < 50; attempt++) {
                QueryScanTaskScope scope = new QueryScanTaskScope();
                SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
                CyclicBarrier ready = new CyclicBarrier(2);
                Future<?> register = executor.submit(() -> {
                    ready.await(5, TimeUnit.SECONDS);
                    scope.register(assignment, () -> { });
                    return null;
                });
                Future<?> cancel = executor.submit(() -> {
                    ready.await(5, TimeUnit.SECONDS);
                    scope.cancel(new Status(TStatusCode.CANCELLED, "query cancelled"));
                    return null;
                });
                register.get(5, TimeUnit.SECONDS);
                cancel.get(5, TimeUnit.SECONDS);
                Mockito.verify(assignment).setException(Mockito.argThat(
                        failure -> failure.getMessage().contains("query cancelled")));
                scope.close();
            }
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void cancellationBeforeRegistrationStopsTheLateScanWithTheOriginalReason() {
        QueryScanTaskScope scope = new QueryScanTaskScope();
        scope.cancel(new Status(TStatusCode.CANCELLED, "cancelled before coordinator publication"));
        scope.cancel(new Status(TStatusCode.TIMEOUT, "later timeout"));
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        AtomicInteger unregisters = new AtomicInteger();
        scope.register(assignment, unregisters::incrementAndGet);
        Mockito.verify(assignment).setException(Mockito.argThat(
                failure -> failure.getMessage().contains("cancelled before coordinator publication")));
        Assertions.assertEquals(0, unregisters.get(), "late BE fetches retain a terminal endpoint until cleanup");
        scope.close();
        scope.close();
        Assertions.assertEquals(1, unregisters.get());
        Mockito.verify(assignment).stop();
    }

    @Test
    void closedScopeCannotLeaveALateScanRegistered() {
        QueryScanTaskScope scope = new QueryScanTaskScope();
        scope.close();
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        AtomicInteger unregisters = new AtomicInteger();
        scope.register(assignment, unregisters::incrementAndGet);
        Mockito.verify(assignment).stop();
        Assertions.assertEquals(1, unregisters.get());
    }

    @Test
    void freshAttemptDoesNotInheritThePreviousAttemptsCancellation() {
        QueryScanTaskScope first = new QueryScanTaskScope();
        first.cancel(new Status(TStatusCode.CANCELLED, "first attempt"));
        first.close();
        QueryScanTaskScope next = new QueryScanTaskScope();
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        next.register(assignment, () -> { });
        Mockito.verify(assignment, Mockito.never()).setException(Mockito.any(UserException.class));
        Mockito.verify(assignment, Mockito.never()).stop();
        next.close();
        Mockito.verify(assignment).stop();
    }
}
