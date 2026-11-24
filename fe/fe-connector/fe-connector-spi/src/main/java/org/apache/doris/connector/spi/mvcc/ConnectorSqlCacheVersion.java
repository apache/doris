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

package org.apache.doris.connector.spi.mvcc;

import java.util.Objects;

/**
 * Immutable whole-table SQL result-cache identity and its wall-clock update time.
 * The identity is compared for equality, never ordered. It must cover data, schema and table recreation,
 * and describe the same pinned generation as the scan. Reading it must not enumerate files or partitions.
 * The timestamp is only for the cache's quiet-window eligibility check, never for identity comparisons.
 */
public final class ConnectorSqlCacheVersion {
    private final String version;
    private final long updateTimeMillis;

    public ConnectorSqlCacheVersion(String version, long updateTimeMillis) {
        this.version = Objects.requireNonNull(version, "version");
        this.updateTimeMillis = updateTimeMillis;
    }

    public String getVersion() {
        return version;
    }

    public long getUpdateTimeMillis() {
        return updateTimeMillis;
    }
}
