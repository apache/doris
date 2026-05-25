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

package org.apache.doris.connector;

import org.apache.doris.catalog.Env;
import org.apache.doris.connector.spi.ConnectorMetaInvalidator;
import org.apache.doris.datasource.ExternalMetaCacheMgr;

import java.util.List;
import java.util.Objects;

/**
 * fe-core side bridge from the connector SPI {@link ConnectorMetaInvalidator} to the
 * engine's {@link ExternalMetaCacheMgr}. Returned by
 * {@link DefaultConnectorContext#getMetaInvalidator()} so connectors that receive
 * external change notifications (e.g. HMS notification events) can drop the right
 * cache entries without depending on fe-core internals directly.
 */
public final class ExternalMetaCacheInvalidator implements ConnectorMetaInvalidator {

    private final long catalogId;

    public ExternalMetaCacheInvalidator(long catalogId) {
        this.catalogId = catalogId;
    }

    @Override
    public void invalidateAll() {
        mgr().invalidateCatalog(catalogId);
    }

    @Override
    public void invalidateDatabase(String dbName) {
        mgr().invalidateDb(catalogId, Objects.requireNonNull(dbName, "dbName"));
    }

    @Override
    public void invalidateTable(String dbName, String tableName) {
        mgr().invalidateTable(catalogId,
                Objects.requireNonNull(dbName, "dbName"),
                Objects.requireNonNull(tableName, "tableName"));
    }

    @Override
    public void invalidatePartition(String dbName, String tableName, List<String> partitionValues) {
        // The SPI carries partition column VALUES (e.g. ["2024", "01"]) but the engine's
        // partition cache is keyed by partition NAMES (e.g. "year=2024/month=01").
        // Reconstructing the name requires partition column names which are not carried by
        // the SPI today. Until the SPI grows that metadata, fall back to table-level
        // invalidation — correct but over-broad.
        mgr().invalidateTable(catalogId,
                Objects.requireNonNull(dbName, "dbName"),
                Objects.requireNonNull(tableName, "tableName"));
    }

    @Override
    public void invalidateStatistics(String dbName, String tableName) {
        // ExternalMetaCacheMgr exposes no per-table statistics-only invalidation today
        // (the row count cache is keyed by id, not name). Calling invalidateTable here
        // would violate the SPI contract ("without dropping schema cache"), so leave as
        // a no-op until a stats-only entry point exists.
    }

    private static ExternalMetaCacheMgr mgr() {
        return Env.getCurrentEnv().getExtMetaCacheMgr();
    }
}
