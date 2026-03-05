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

package org.apache.doris.datasource.paimon;

import org.apache.paimon.table.Table;

import java.util.function.Supplier;

/**
 * Cache value for Paimon table metadata.
 * Encapsulates the Paimon Table object and provides lazy loading for snapshot cache.
 */
public class PaimonTableCacheValue {
    private final Table paimonTable;

    // Lazy-loaded snapshot cache
    private volatile boolean snapshotCacheLoaded;
    private volatile PaimonSnapshotCacheValue snapshotCacheValue;

    public PaimonTableCacheValue(Table paimonTable) {
        this.paimonTable = paimonTable;
    }

    public Table getPaimonTable() {
        return paimonTable;
    }

    /**
     * Get snapshot cache value with lazy loading.
     * Uses double-checked locking to ensure thread-safe initialization.
     *
     * @param loader Supplier to load snapshot cache value when needed
     * @return The cached or newly loaded snapshot cache value
     */
    public PaimonSnapshotCacheValue getSnapshotCacheValue(Supplier<PaimonSnapshotCacheValue> loader) {
        if (!snapshotCacheLoaded) {
            synchronized (this) {
                if (!snapshotCacheLoaded) {
                    snapshotCacheValue = loader.get();
                    snapshotCacheLoaded = true;
                }
            }
        }
        return snapshotCacheValue;
    }
}
