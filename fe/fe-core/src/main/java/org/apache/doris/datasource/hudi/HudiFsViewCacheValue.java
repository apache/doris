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

/**
 * Reference-counted wrapper around a shared {@link HoodieTableFileSystemView}.
 *
 * <p>The underlying fs view is cached per table and shared by concurrent scan nodes. Closing it while
 * another thread is still planning splits is unsafe, so the cache only closes the view after the entry has
 * been evicted AND all acquired references have been released.
 */
public class HudiFsViewCacheValue {
    private final HoodieTableFileSystemView fsView;
    // The loader owns one transferable reference until getFsView hands this exact generation to its first caller.
    private int refCount = 1;
    private boolean loaderReferenceAvailable = true;
    private boolean evicted = false;
    private boolean closed = false;

    public HudiFsViewCacheValue(HoodieTableFileSystemView fsView) {
        this.fsView = fsView;
    }

    public synchronized Lease tryAcquire() {
        Lease lease;
        if (loaderReferenceAvailable) {
            loaderReferenceAvailable = false;
            lease = new Lease(this, fsView);
        } else if (evicted) {
            return null;
        } else {
            refCount++;
            lease = new Lease(this, fsView);
        }
        try {
            // The cache uses expire-after-access without detached refresh. Sync every foreground generation handoff
            // so a continuously hot key still observes newly completed commits.
            fsView.sync();
            return lease;
        } catch (RuntimeException e) {
            lease.close();
            throw e;
        }
    }

    public synchronized void evict() {
        evicted = true;
        maybeClose();
    }

    private synchronized void release() {
        if (refCount <= 0) {
            throw new IllegalStateException("Hudi fs view released without a matching acquisition");
        }
        refCount--;
        maybeClose();
    }

    private void maybeClose() {
        if (evicted && !closed && refCount == 0) {
            closed = true;
            fsView.close();
        }
    }

    /** A lease pins the exact cache generation until split planning has finished using it. */
    public static class Lease implements AutoCloseable {
        private HudiFsViewCacheValue owner;
        private final HoodieTableFileSystemView fsView;

        private Lease(HudiFsViewCacheValue owner, HoodieTableFileSystemView fsView) {
            this.owner = owner;
            this.fsView = fsView;
        }

        public HoodieTableFileSystemView get() {
            return fsView;
        }

        @Override
        public synchronized void close() {
            if (owner != null) {
                owner.release();
                owner = null;
            }
        }
    }
}
