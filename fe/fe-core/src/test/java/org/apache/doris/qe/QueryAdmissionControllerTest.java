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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class QueryAdmissionControllerTest {
    @Test
    public void testDrainIncludesPlanningAndAllowsResultRetrieval() {
        QueryAdmissionController controller = new QueryAdmissionController();
        Assertions.assertTrue(controller.tryAcquire());
        controller.stopAccepting();
        Assertions.assertFalse(controller.tryAcquire());
        Assertions.assertEquals(1, controller.getActiveRequests());
        controller.acquireContinuation();
        controller.release();
        Assertions.assertEquals(1, controller.getActiveRequests());
        controller.release();
        Assertions.assertEquals(0, controller.getActiveRequests());
        Assertions.assertFalse(controller.tryAcquire());
    }

    @Test
    public void testAdmissionRacesShutdown() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            for (int i = 0; i < 100; i++) {
                QueryAdmissionController controller = new QueryAdmissionController();
                CountDownLatch start = new CountDownLatch(1);
                Future<Boolean> request = executor.submit(() -> {
                    start.await();
                    return controller.tryAcquire();
                });
                start.countDown();
                controller.stopAccepting();
                boolean admitted = request.get(10, TimeUnit.SECONDS);
                // Every request winning admission must remain visible to the drain loop.
                Assertions.assertEquals(admitted ? 1 : 0, controller.getActiveRequests());
                Assertions.assertFalse(controller.tryAcquire());
                if (admitted) {
                    controller.release();
                }
                Assertions.assertEquals(0, controller.getActiveRequests());
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
