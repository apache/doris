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

package org.apache.doris.catalog.authorizer.ranger.hive;

import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RangerHiveAuditLogFlusherTest {

    @Test
    public void testRunFlushesOnceAndReturns() {
        RangerHiveAuditHandler auditHandler = Mockito.mock(RangerHiveAuditHandler.class);

        new RangerHiveAuditLogFlusher(auditHandler).run();

        Mockito.verify(auditHandler).flushAudit();
    }

    @Test
    public void testRunContainsProviderFailureSoLaterTicksContinue() {
        RangerHiveAuditHandler auditHandler = Mockito.mock(RangerHiveAuditHandler.class);
        Mockito.doThrow(new RuntimeException("provider unavailable"))
                .doNothing().when(auditHandler).flushAudit();
        RangerHiveAuditLogFlusher flusher = new RangerHiveAuditLogFlusher(auditHandler);

        flusher.run();
        flusher.run();

        Mockito.verify(auditHandler, Mockito.times(2)).flushAudit();
    }

    @Test
    public void testProducerCanEnqueueWhileProviderDeliveryIsBlocked() throws Exception {
        RecordingAuditHandler auditHandler = new RecordingAuditHandler(false, true);
        AuthzAuditEvent first = allowedEvent();
        AuthzAuditEvent second = allowedEvent();
        auditHandler.addAuthzAuditEvent(first);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<?> flush = executor.submit(auditHandler::flushAudit);
            Assert.assertTrue(auditHandler.deliveryStarted.await(10, TimeUnit.SECONDS));

            Future<?> producer = executor.submit(() -> auditHandler.addAuthzAuditEvent(second));
            producer.get(2, TimeUnit.SECONDS);

            auditHandler.allowDelivery.countDown();
            flush.get(10, TimeUnit.SECONDS);
            auditHandler.flushAudit();

            Assert.assertEquals(List.of(first, second), auditHandler.delivered);
        } finally {
            auditHandler.allowDelivery.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testFailedDeliveryIsRetriedBeforeLaterEvents() {
        RecordingAuditHandler auditHandler = new RecordingAuditHandler(true, false);
        AuthzAuditEvent first = allowedEvent();
        AuthzAuditEvent second = allowedEvent();
        auditHandler.addAuthzAuditEvent(first);

        Assert.assertThrows(RuntimeException.class, auditHandler::flushAudit);
        Assert.assertEquals(1, auditHandler.getPendingAuditEventCountForTest());

        auditHandler.addAuthzAuditEvent(second);
        auditHandler.flushAudit();

        Assert.assertEquals(List.of(first, second), auditHandler.delivered);
        Assert.assertEquals(0, auditHandler.getPendingAuditEventCountForTest());
    }

    private static AuthzAuditEvent allowedEvent() {
        AuthzAuditEvent event = new AuthzAuditEvent();
        event.setAccessResult((short) 1);
        return event;
    }

    private static class RecordingAuditHandler extends RangerHiveAuditHandler {
        private final AtomicBoolean failNext;
        private final boolean blockFirstDelivery;
        private final AtomicBoolean firstDelivery = new AtomicBoolean(true);
        private final CountDownLatch deliveryStarted = new CountDownLatch(1);
        private final CountDownLatch allowDelivery = new CountDownLatch(1);
        private final List<AuthzAuditEvent> delivered = new CopyOnWriteArrayList<>();

        RecordingAuditHandler(boolean failFirstDelivery, boolean blockFirstDelivery) {
            this.failNext = new AtomicBoolean(failFirstDelivery);
            this.blockFirstDelivery = blockFirstDelivery;
        }

        @Override
        protected void logAuditEvent(AuthzAuditEvent auditEvent) {
            if (blockFirstDelivery && firstDelivery.compareAndSet(true, false)) {
                deliveryStarted.countDown();
                try {
                    if (!allowDelivery.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("timed out waiting to release audit provider");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            if (failNext.compareAndSet(true, false)) {
                throw new RuntimeException("provider unavailable");
            }
            delivered.add(auditEvent);
        }
    }
}
