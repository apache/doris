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

package org.apache.doris.datasource;

import org.apache.doris.common.util.MasterDaemon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * `SplitSource` is obtained by RPC call of `FrontendServiceImpl#fetchSplitBatch`.
 * Each `SplitSource` is reference by its unique ID. `SplitSourceManager` provides the register, get, and remove
 * function to manage the split sources. In order to clean the split source when the query finished,
 * `SplitSource` is stored as a weak reference, and use `ReferenceQueue` to remove split source when GC.
 */
public class SplitSourceManager extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(SplitSourceManager.class);

    public static class SplitSourceReference extends WeakReference<SplitSource> {
        private final long uniqueId;

        public SplitSourceReference(SplitSource splitSource, ReferenceQueue<? super SplitSource> queue) {
            super(splitSource, queue);
            uniqueId = splitSource.getUniqueId();
        }

        public long getUniqueId() {
            return uniqueId;
        }
    }

    private final ReferenceQueue<SplitSource> splitsRefQueue = new ReferenceQueue<>();
    private final Map<Long, WeakReference<SplitSource>> splits = new ConcurrentHashMap<>();

    public void registerSplitSource(SplitSource splitSource) {
        splits.put(splitSource.getUniqueId(), new SplitSourceReference(splitSource, splitsRefQueue));
    }

    public void removeSplitSource(long uniqueId) {
        splits.remove(uniqueId);
    }

    public SplitSource getSplitSource(long uniqueId) {
        WeakReference<SplitSource> ref = splits.get(uniqueId);
        if (ref == null) {
            return null;
        } else {
            return ref.get();
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        while (true) {
            try {
                SplitSourceReference reference = (SplitSourceReference) splitsRefQueue.remove();
                removeSplitSource(reference.getUniqueId());
            } catch (Exception e) {
                LOG.warn("Failed to clean split source", e);
            }
        }
    }
}
