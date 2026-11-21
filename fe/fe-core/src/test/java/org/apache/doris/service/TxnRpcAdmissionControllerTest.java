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

package org.apache.doris.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TxnRpcAdmissionControllerTest {
    @Test
    public void testIdleShutdownFencesNewTransactionRpcs() {
        TxnRpcAdmissionController controller = new TxnRpcAdmissionController();
        TxnRpcAdmissionController.Guard guard = controller.tryEnter();
        Assertions.assertNotNull(guard);
        Assertions.assertFalse(controller.stopAcceptingIfIdle());
        guard.close();
        Assertions.assertTrue(controller.stopAcceptingIfIdle());
        Assertions.assertNull(controller.tryEnter());
    }

    @Test
    public void testAdmittedHandlerSurvivesClosingAdmission() {
        TxnRpcAdmissionController controller = new TxnRpcAdmissionController();
        TxnRpcAdmissionController.Guard guard = controller.tryEnter();
        Assertions.assertNotNull(guard);
        controller.stopAccepting();
        Assertions.assertNull(controller.tryEnter());
        Assertions.assertEquals(1, controller.getActiveRequests());
        Assertions.assertFalse(Thread.currentThread().isInterrupted());
        guard.close();
        guard.close();
        Assertions.assertEquals(0, controller.getActiveRequests());
        Assertions.assertNull(controller.tryEnter());
    }

    @Test
    public void testExceptionReleasesHandler() {
        TxnRpcAdmissionController controller = new TxnRpcAdmissionController();
        Assertions.assertThrows(IllegalStateException.class, () -> {
            try (TxnRpcAdmissionController.Guard guard = controller.tryEnter()) {
                Assertions.assertNotNull(guard);
                throw new IllegalStateException("failed transaction");
            }
        });
        Assertions.assertEquals(0, controller.getActiveRequests());
    }

    @Test
    public void testClosingCannotMissAnAdmittedHandler() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            for (int i = 0; i < 200; i++) {
                TxnRpcAdmissionController controller = new TxnRpcAdmissionController();
                CountDownLatch start = new CountDownLatch(1);
                Future<TxnRpcAdmissionController.Guard> request = executor.submit(() -> {
                    start.await();
                    return controller.tryEnter();
                });
                start.countDown();
                controller.stopAccepting();
                TxnRpcAdmissionController.Guard guard = request.get(5, TimeUnit.SECONDS);
                Assertions.assertEquals(guard == null ? 0 : 1, controller.getActiveRequests());
                Assertions.assertNull(controller.tryEnter());
                if (guard != null) {
                    guard.close();
                }
                Assertions.assertEquals(0, controller.getActiveRequests());
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
