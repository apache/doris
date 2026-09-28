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

import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimator;

import org.apache.paimon.table.Table;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

/**
 * Cache value for a Paimon table handle. Snapshot projections use a separate cache entry; the
 * only post-admission growth of this value is the lazily built store graph and RowType lookup
 * maps of the table itself, which the publication estimate reserves up front.
 */
public class PaimonTableCacheValue {
    private static final AtomicLong NEXT_GENERATION = new AtomicLong();

    private final Table paimonTable;
    private final long generation;
    // The execution authenticator active when this generation was loaded. Later fence/schema
    // loads on the generation reuse it so a concurrent catalog reset (property ALTER) can
    // neither fail an in-flight lookup nor pair replacement credentials with this handle.
    @Nullable
    private final org.apache.doris.common.security.authentication.ExecutionAuthenticator authenticator;
    private volatile long retainedTablePayloadBytes;
    private volatile MetaCacheSizeEstimate sizeEstimate;

    public PaimonTableCacheValue(Table paimonTable) {
        this(paimonTable,
                (org.apache.doris.common.security.authentication.ExecutionAuthenticator) null);
    }

    public PaimonTableCacheValue(Table paimonTable,
            @Nullable org.apache.doris.common.security.authentication.ExecutionAuthenticator authenticator) {
        this.paimonTable = paimonTable;
        this.authenticator = authenticator;
        this.generation = NEXT_GENERATION.incrementAndGet();
    }

    @Nullable
    org.apache.doris.common.security.authentication.ExecutionAuthenticator getAuthenticator() {
        return authenticator;
    }

    public PaimonTableCacheValue(Table paimonTable, PaimonSnapshotCacheValue ignoredFence) {
        this(paimonTable);
        Objects.requireNonNull(ignoredFence, "latestSnapshotFence can not be null");
    }

    public Table getPaimonTable() {
        return paimonTable;
    }

    public long getGeneration() {
        return generation;
    }

    long getRetainedTablePayloadBytes() {
        return retainedTablePayloadBytes;
    }

    /**
     * Compute the retained weight once before the value is published to a weight-bounded cache.
     * Never opens the table store; failures fail closed as an incomplete estimate.
     */
    synchronized MetaCacheSizeEstimate prepareForCachePublication(NameMapping key) {
        if (sizeEstimate == null) {
            sizeEstimate = MetaCacheSizeEstimator.estimateSafely("paimon_table_preparation_failed",
                    () -> {
                        retainedTablePayloadBytes =
                                PaimonCacheSizeEstimator.retainedTablePayloadBytes(paimonTable);
                        return PaimonCacheSizeEstimator.estimateTableEntry(key, this);
                    });
        }
        return sizeEstimate;
    }

    public MetaCacheSizeEstimate getSizeEstimate() {
        return sizeEstimate == null
                ? MetaCacheSizeEstimate.incomplete("not_prepared") : sizeEstimate;
    }
}
