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

import org.apache.doris.common.DdlException;
import org.apache.doris.mtmv.BaseTableInfo;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class PaimonSnapshotCache {
    // snapshotId ==> SchemaCacheValue
    private static Map<SnapshotCacheKey, PaimonSchemaCacheValue> snapshotCache = Maps.newConcurrentMap();
    // snapshotId ==> refNum
    private static Map<SnapshotCacheKey, AtomicInteger> snapshotIdRefs = Maps.newConcurrentMap();

    public static PaimonSchemaCacheValue getSchemaCacheBySnapshotId(PaimonExternalTable paimonExternalTable,
            long snapshotId) throws DdlException {
        Preconditions.checkNotNull(paimonExternalTable);
        BaseTableInfo baseTableInfo = new BaseTableInfo(paimonExternalTable);
        SnapshotCacheKey key = new SnapshotCacheKey(baseTableInfo, snapshotId);
        try {
            paimonExternalTable.readSnapshotLock();
            if (snapshotCache.containsKey(key)) {
                return snapshotCache.get(key);
            }
        } finally {
            paimonExternalTable.readSnapshotUnlock();
        }
        return load(paimonExternalTable, snapshotId, key);
    }

    private static PaimonSchemaCacheValue load(PaimonExternalTable paimonExternalTable, long snapshotId,
            SnapshotCacheKey key) throws DdlException {
        // may be slow, not in lock
        Optional<PaimonSchemaCacheValue> latestSchemaCache = paimonExternalTable.getLatestSchemaCache();
        try {
            paimonExternalTable.writeSnapshotLock();
            if (snapshotCache.containsKey(key)) {
                return snapshotCache.get(key);
            }
            if (latestSchemaCache.isPresent() && latestSchemaCache.get().getSnapshootId() == snapshotId) {
                snapshotCache.put(key, latestSchemaCache.get());
                return latestSchemaCache.get();
            }
        } finally {
            paimonExternalTable.writeSnapshotUnlock();
        }
        throw new DdlException("schema can not find by: " + key);
    }

    public static void ref(PaimonExternalTable paimonExternalTable, long snapshotId) {
        Preconditions.checkNotNull(paimonExternalTable);
        BaseTableInfo baseTableInfo = new BaseTableInfo(paimonExternalTable);
        SnapshotCacheKey key = new SnapshotCacheKey(baseTableInfo, snapshotId);
        try {
            paimonExternalTable.writeSnapshotLock();
            if (snapshotIdRefs.containsKey(key)) {
                snapshotIdRefs.get(key).getAndIncrement();
            } else {
                snapshotIdRefs.put(key, new AtomicInteger(1));
            }
        } finally {
            paimonExternalTable.writeSnapshotUnlock();
        }

    }

    public static void unref(PaimonExternalTable paimonExternalTable, long snapshotId) {
        Preconditions.checkNotNull(paimonExternalTable);
        BaseTableInfo baseTableInfo = new BaseTableInfo(paimonExternalTable);
        SnapshotCacheKey key = new SnapshotCacheKey(baseTableInfo, snapshotId);
        try {
            paimonExternalTable.writeSnapshotLock();
            int i = snapshotIdRefs.get(key).decrementAndGet();
            if (i == 0) {
                snapshotIdRefs.remove(key);
                snapshotCache.remove(key);
            }
        } finally {
            paimonExternalTable.writeSnapshotUnlock();
        }
    }

    private static class SnapshotCacheKey {
        public BaseTableInfo baseTableInfo;
        public long snapshotId;

        public SnapshotCacheKey(BaseTableInfo baseTableInfo, long snapshotId) {
            this.baseTableInfo = baseTableInfo;
            this.snapshotId = snapshotId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SnapshotCacheKey that = (SnapshotCacheKey) o;
            return snapshotId == that.snapshotId && Objects.equal(baseTableInfo, that.baseTableInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(baseTableInfo, snapshotId);
        }

        @Override
        public String toString() {
            return "SnapshotCacheKey{"
                    + "baseTableInfo=" + baseTableInfo
                    + ", snapshotId=" + snapshotId
                    + '}';
        }
    }
}
