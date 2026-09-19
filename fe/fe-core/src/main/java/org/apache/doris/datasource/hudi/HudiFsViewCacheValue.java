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
 * another thread is still planning splits is unsafe, so the wrapper only closes the view after its cache
 * and loader owners have retired and all acquired leases have been released.
 */
public class HudiFsViewCacheValue {
    private final HoodieTableFileSystemView fsView;
    // Cache and loader ownership are independent so an asynchronous removal callback cannot close
    // a rejected value before getFsView has handed it to the caller that performed the load.
    private int refCount = 2;
    private boolean cacheReferenceReleased;
    private boolean loaderReferenceReleased;
    private boolean closed = false;

    public HudiFsViewCacheValue(HoodieTableFileSystemView fsView) {
        this.fsView = fsView;
    }

    public Lease tryAcquire() {
        synchronized (this) {
            if (cacheReferenceReleased && loaderReferenceReleased) {
                return null;
            }
            refCount++;
            return new Lease(this, fsView);
        }
    }

    public synchronized void releaseCacheReference() {
        if (!cacheReferenceReleased) {
            cacheReferenceReleased = true;
            releaseReference();
        }
    }

    public synchronized void releaseLoaderReference() {
        if (!loaderReferenceReleased) {
            loaderReferenceReleased = true;
            releaseReference();
        }
    }

    public synchronized void retire() {
        releaseCacheReference();
        releaseLoaderReference();
    }

    private synchronized void release() {
        releaseReference();
    }

    private void releaseReference() {
        if (refCount <= 0) {
            throw new IllegalStateException("Hudi fs view released without a matching acquisition");
        }
        refCount--;
        maybeClose();
    }

    private void maybeClose() {
        if (!closed && refCount == 0) {
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
