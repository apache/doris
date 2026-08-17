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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cache value for a Paimon table handle. Snapshot projections use a separate cache entry so this
 * value cannot grow after admission.
 */
public class PaimonTableCacheValue {
    private static final AtomicLong NEXT_GENERATION = new AtomicLong();

    private final Table paimonTable;
    private final long generation;

    public PaimonTableCacheValue(Table paimonTable) {
        this.paimonTable = paimonTable;
        this.generation = NEXT_GENERATION.incrementAndGet();
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

}
