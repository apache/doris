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

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

final class CloudTabletRebalancerMetrics {
    private static final long ALLOCATED_BYTES_UNAVAILABLE = -1L;

    private final LongSupplier nanoTimeSupplier;
    private final LongSupplier allocatedBytesSupplier;

    CloudTabletRebalancerMetrics(LongSupplier nanoTimeSupplier, LongSupplier allocatedBytesSupplier) {
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.allocatedBytesSupplier = allocatedBytesSupplier;
    }

    static CloudTabletRebalancerMetrics create() {
        com.sun.management.ThreadMXBean threadMxBean =
                ManagementFactory.getPlatformMXBean(com.sun.management.ThreadMXBean.class);
        return new CloudTabletRebalancerMetrics(System::nanoTime, createAllocatedBytesSupplier(threadMxBean));
    }

    Round startRound() {
        return new Round(nanoTimeSupplier.getAsLong(), allocatedBytesSupplier.getAsLong());
    }

    void finishRound(Round round, long tabletScanCount) {
        long durationMs = TimeUnit.NANOSECONDS.toMillis(nanoTimeSupplier.getAsLong() - round.startNanos);
        long currentAllocatedBytes = allocatedBytesSupplier.getAsLong();
        long allocatedBytes = round.startAllocatedBytes < 0L || currentAllocatedBytes < 0L
                ? ALLOCATED_BYTES_UNAVAILABLE : currentAllocatedBytes - round.startAllocatedBytes;
        MetricRepo.updateCloudTabletRebalancerMetrics(durationMs, allocatedBytes, tabletScanCount);
    }

    static LongSupplier createAllocatedBytesSupplier(com.sun.management.ThreadMXBean threadMxBean) {
        if (threadMxBean == null || !threadMxBean.isThreadAllocatedMemorySupported()) {
            return () -> ALLOCATED_BYTES_UNAVAILABLE;
        }
        if (!threadMxBean.isThreadAllocatedMemoryEnabled()) {
            try {
                threadMxBean.setThreadAllocatedMemoryEnabled(true);
            } catch (SecurityException | UnsupportedOperationException e) {
                return () -> ALLOCATED_BYTES_UNAVAILABLE;
            }
        }
        return () -> threadMxBean.getThreadAllocatedBytes(Thread.currentThread().getId());
    }

    static final class Round {
        private final long startNanos;
        private final long startAllocatedBytes;

        private Round(long startNanos, long startAllocatedBytes) {
            this.startNanos = startNanos;
            this.startAllocatedBytes = startAllocatedBytes;
        }
    }
}
