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

package org.apache.doris.connector.spi.scan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectorScanContextTest {
    @Test
    public void notificationIsOnceAndLateRegistrationObservesCancellation() {
        ConnectorScanContext context = new ConnectorScanContext();
        AtomicInteger notifications = new AtomicInteger();
        CompletableFuture<Void> callback = context.whenCancelled(() -> {
            Assertions.assertTrue(context.isCancelled());
            notifications.incrementAndGet();
        }).toCompletableFuture();
        context.cancel();
        context.cancel();
        callback.join();
        context.whenCancelled(notifications::incrementAndGet).toCompletableFuture().join();
        Assertions.assertEquals(2, notifications.get());
    }

    @Test
    public void oneCallbackFailureDoesNotPoisonOtherNotifications() {
        ConnectorScanContext context = new ConnectorScanContext();
        AtomicInteger notifications = new AtomicInteger();
        CompletableFuture<Void> failed = context.whenCancelled(() -> {
            throw new IllegalStateException("controlled callback failure");
        }).toCompletableFuture();
        CompletableFuture<Void> healthy = context.whenCancelled(notifications::incrementAndGet).toCompletableFuture();
        context.cancel();
        healthy.join();
        Assertions.assertInstanceOf(IllegalStateException.class,
                Assertions.assertThrows(CompletionException.class, failed::join).getCause());
        Assertions.assertEquals(1, notifications.get());
    }

    @Test
    public void registrationRacingCancellationNeverLosesNotification() throws Exception {
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            for (int i = 0; i < 100; i++) {
                ConnectorScanContext context = new ConnectorScanContext();
                AtomicInteger notifications = new AtomicInteger();
                CountDownLatch start = new CountDownLatch(1);
                Future<?> cancellation = caller.submit(() -> {
                    start.await();
                    context.cancel();
                    return null;
                });
                start.countDown();
                CompletableFuture<Void> notification = context.whenCancelled(notifications::incrementAndGet)
                        .toCompletableFuture();
                cancellation.get(10, TimeUnit.SECONDS);
                notification.get(10, TimeUnit.SECONDS);
                Assertions.assertEquals(1, notifications.get());
            }
        } finally {
            caller.shutdownNow();
            Assertions.assertTrue(caller.awaitTermination(10, TimeUnit.SECONDS));
        }
    }
}
