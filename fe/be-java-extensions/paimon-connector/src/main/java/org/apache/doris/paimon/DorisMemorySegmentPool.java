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

import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentPool;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;

/**
 * Paimon write-buffer pool backed by memory allocated and tracked by Doris BE.
 *
 * <p>This class only adapts the Paimon page interface to the BE allocator. The native memory
 * manager owns every returned page and releases them after the Java writer has closed.
 */
final class DorisMemorySegmentPool implements MemorySegmentPool {
    @FunctionalInterface
    interface PageAllocator {
        ByteBuffer allocate(long nativeMemoryManager, int pageSize, boolean waitForMemory);
    }

    private final long nativeMemoryManager;
    private final int pageSize;
    private final int maxPages;
    private final PageAllocator pageAllocator;
    private final ArrayDeque<MemorySegment> availableSegments = new ArrayDeque<>();
    private int allocatedPages;
    private boolean nativeMemoryPressure;

    DorisMemorySegmentPool(long maxMemory, int pageSize, long nativeMemoryManager) {
        this(maxMemory, pageSize, nativeMemoryManager,
                PaimonJniWriter::allocatePaimonMemoryPage);
    }

    DorisMemorySegmentPool(long maxMemory, int pageSize, long nativeMemoryManager,
            PageAllocator pageAllocator) {
        if (nativeMemoryManager == 0) {
            throw new IllegalArgumentException("Doris native memory manager must not be null");
        }
        if (pageSize <= 0) {
            throw new IllegalArgumentException(
                    "Doris-managed Paimon memory page size must be positive: " + pageSize);
        }
        if (maxMemory < pageSize) {
            throw new IllegalArgumentException(
                    "Doris-managed Paimon memory pool must contain at least one page: maxMemory="
                            + maxMemory + ", pageSize=" + pageSize);
        }
        long pages = maxMemory / pageSize;
        if (pages > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Doris-managed Paimon memory pool has too many pages: " + pages);
        }
        this.nativeMemoryManager = nativeMemoryManager;
        this.pageSize = pageSize;
        this.maxPages = (int) pages;
        this.pageAllocator = Objects.requireNonNull(pageAllocator, "pageAllocator");
    }

    @Override
    public MemorySegment nextSegment() {
        synchronized (this) {
            MemorySegment available = availableSegments.pollFirst();
            if (available != null) {
                nativeMemoryPressure = false;
                return available;
            }
            if (allocatedPages >= maxPages) {
                return null;
            }
            ++allocatedPages;
        }

        final ByteBuffer buffer;
        try {
            buffer = pageAllocator.allocate(nativeMemoryManager, pageSize, false);
        } catch (Throwable t) {
            rollbackAllocation();
            throw t;
        }

        if (buffer == null) {
            recordNativeMemoryPressure();
            return null;
        }
        clearNativeMemoryPressure();
        return MemorySegment.wrapOffHeapMemory(buffer);
    }

    /**
     * Wait for one native page only after a Paimon write operation has reached a safe boundary.
     *
     * <p>{@link #nextSegment()} deliberately returns {@code null} on Doris memory pressure so the
     * standard Paimon memory pool can first preempt another owner and let the requesting owner run
     * its own spill path. If neither path produced a reusable page, this method allocates one page
     * in blocking mode and leaves it in the pool for the next Paimon operation.
     */
    void waitForMemoryIfNeeded() {
        synchronized (this) {
            if (!nativeMemoryPressure) {
                return;
            }
            if (!availableSegments.isEmpty() || allocatedPages >= maxPages) {
                nativeMemoryPressure = false;
                return;
            }
            ++allocatedPages;
            nativeMemoryPressure = false;
        }

        final ByteBuffer buffer;
        try {
            buffer = pageAllocator.allocate(nativeMemoryManager, pageSize, true);
        } catch (Throwable t) {
            rollbackAllocation();
            throw t;
        }

        if (buffer == null) {
            rollbackAllocation();
            throw new OutOfMemoryError(
                    "Doris failed to allocate a native Paimon memory page of "
                            + pageSize + " bytes while waiting at a safe write boundary");
        }

        synchronized (this) {
            availableSegments.addFirst(MemorySegment.wrapOffHeapMemory(buffer));
        }
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    @Override
    public synchronized void returnAll(List<MemorySegment> memory) {
        availableSegments.addAll(memory);
    }

    @Override
    public synchronized int freePages() {
        return availableSegments.size() + maxPages - allocatedPages;
    }

    private synchronized void recordNativeMemoryPressure() {
        --allocatedPages;
        nativeMemoryPressure = true;
    }

    private synchronized void clearNativeMemoryPressure() {
        nativeMemoryPressure = false;
    }

    private synchronized void rollbackAllocation() {
        --allocatedPages;
    }
}
