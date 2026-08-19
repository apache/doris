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

package org.apache.doris.datasource.hudi;

import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reference-counted wrapper around a shared {@link HoodieTableFileSystemView}.
 *
 * <p>The underlying fs view is cached per table and shared by concurrent scan nodes. Closing it while
 * another thread is still planning splits is unsafe, so the cache only closes the view after the entry has
 * been evicted AND all acquired references have been released.
 */
public class HudiFsViewCacheValue {
    private final HoodieTableFileSystemView fsView;
    private final AtomicInteger refCount = new AtomicInteger(0);
    private volatile boolean evicted = false;
    private volatile boolean closed = false;

    public HudiFsViewCacheValue(HoodieTableFileSystemView fsView) {
        this.fsView = fsView;
    }

    public HoodieTableFileSystemView acquire() {
        refCount.incrementAndGet();
        return fsView;
    }

    public void evict() {
        evicted = true;
        maybeClose();
    }

    public void release() {
        refCount.decrementAndGet();
        maybeClose();
    }

    private synchronized void maybeClose() {
        if (evicted && !closed && refCount.get() == 0) {
            closed = true;
            fsView.close();
        }
    }
}
