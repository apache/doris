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

import org.apache.paimon.memory.AbstractMemorySegmentPool;
import org.apache.paimon.memory.MemorySegment;

import java.nio.ByteBuffer;

/** Paimon write-buffer pool backed by memory allocated and tracked by Doris BE. */
final class DorisMemorySegmentPool extends AbstractMemorySegmentPool {
    static final int MIN_REQUIRED_PAGES = 3;

    private final long nativeMemoryManager;

    DorisMemorySegmentPool(long maxMemory, int pageSize, long nativeMemoryManager) {
        super(maxMemory, pageSize);
        if (nativeMemoryManager == 0) {
            throw new IllegalArgumentException("Doris native memory manager must not be null");
        }
        if (pageSize <= 0 || maxMemory / pageSize < MIN_REQUIRED_PAGES) {
            throw new IllegalArgumentException(
                    "Doris-managed Paimon memory pool must contain at least "
                            + MIN_REQUIRED_PAGES + " pages: maxMemory=" + maxMemory
                            + ", pageSize=" + pageSize);
        }
        this.nativeMemoryManager = nativeMemoryManager;
    }

    @Override
    protected MemorySegment allocateMemory() {
        ByteBuffer buffer =
                PaimonJniWriter.allocatePaimonMemoryPage(nativeMemoryManager, pageSize);
        if (buffer == null) {
            throw new OutOfMemoryError(
                    "Doris failed to allocate a native Paimon memory page of "
                            + pageSize + " bytes");
        }
        return MemorySegment.wrapOffHeapMemory(buffer);
    }
}
