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

package org.apache.doris.cloud.catalog;

import org.apache.doris.metric.MetricRepo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public class CloudTabletRebalancerMetricsTest {

    @Test
    public void testCreateUsesCurrentJvmThreadAllocationSupport() {
        CloudTabletRebalancerMetrics metrics = CloudTabletRebalancerMetrics.create();
        Assertions.assertNotNull(metrics.startRound());
    }

    @Test
    public void testAllocatedBytesSupplierHandlesJvmCapabilities() {
        com.sun.management.ThreadMXBean unsupported = Mockito.mock(com.sun.management.ThreadMXBean.class);
        Mockito.when(unsupported.isThreadAllocatedMemorySupported()).thenReturn(false);
        LongSupplier unsupportedSupplier = CloudTabletRebalancerMetrics.createAllocatedBytesSupplier(unsupported);
        Assertions.assertEquals(-1L, unsupportedSupplier.getAsLong());

        com.sun.management.ThreadMXBean denied = Mockito.mock(com.sun.management.ThreadMXBean.class);
        Mockito.when(denied.isThreadAllocatedMemorySupported()).thenReturn(true);
        Mockito.when(denied.isThreadAllocatedMemoryEnabled()).thenReturn(false);
        Mockito.doThrow(new SecurityException()).when(denied).setThreadAllocatedMemoryEnabled(true);
        LongSupplier deniedSupplier = CloudTabletRebalancerMetrics.createAllocatedBytesSupplier(denied);
        Assertions.assertEquals(-1L, deniedSupplier.getAsLong());

        com.sun.management.ThreadMXBean enabled = Mockito.mock(com.sun.management.ThreadMXBean.class);
        Mockito.when(enabled.isThreadAllocatedMemorySupported()).thenReturn(true);
        Mockito.when(enabled.isThreadAllocatedMemoryEnabled()).thenReturn(true);
        Mockito.when(enabled.getCurrentThreadAllocatedBytes()).thenReturn(1234L);
        LongSupplier enabledSupplier = CloudTabletRebalancerMetrics.createAllocatedBytesSupplier(enabled);
        Assertions.assertEquals(1234L, enabledSupplier.getAsLong());
    }

    @Test
    public void testFinishRoundRecordsDurationAllocationAndWork() {
        AtomicLong nanoTime = new AtomicLong(1_000_000_000L);
        AtomicLong allocatedBytes = new AtomicLong(10_000L);
        CloudTabletRebalancerMetrics metrics =
                new CloudTabletRebalancerMetrics(nanoTime::get, allocatedBytes::get);

        CloudTabletRebalancerMetrics.Round round = metrics.startRound();
        nanoTime.set(1_012_000_000L);
        allocatedBytes.set(10_777L);

        try (MockedStatic<MetricRepo> metricRepo = Mockito.mockStatic(MetricRepo.class)) {
            metrics.finishRound(round, 42L);
            metricRepo.verify(() -> MetricRepo.updateCloudTabletRebalancerMetrics(12L, 777L, 42L));
        }
    }

    @Test
    public void testFinishRoundMarksAllocationUnavailable() {
        AtomicLong nanoTime = new AtomicLong(2_000_000_000L);
        CloudTabletRebalancerMetrics metrics = new CloudTabletRebalancerMetrics(nanoTime::get, () -> -1L);

        CloudTabletRebalancerMetrics.Round round = metrics.startRound();
        nanoTime.set(2_003_000_000L);

        try (MockedStatic<MetricRepo> metricRepo = Mockito.mockStatic(MetricRepo.class)) {
            metrics.finishRound(round, 7L);
            metricRepo.verify(() -> MetricRepo.updateCloudTabletRebalancerMetrics(3L, -1L, 7L));
        }
    }
}
