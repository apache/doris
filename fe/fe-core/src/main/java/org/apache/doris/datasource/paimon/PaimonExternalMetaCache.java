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

import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.metacache.AbstractExternalMetaCache;
import org.apache.doris.datasource.metacache.MetaCacheEntryDef;
import org.apache.doris.datasource.metacache.MetaCacheEntryInvalidation;
import org.apache.doris.datasource.metacache.paimon.PaimonLatestSnapshotProjectionLoader;
import org.apache.doris.datasource.metacache.paimon.PaimonPartitionInfoLoader;
import org.apache.doris.datasource.metacache.paimon.PaimonTableLoader;

import org.apache.paimon.table.Table;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Paimon engine implementation of {@link AbstractExternalMetaCache}.
 *
 * <p>Registered entries:
 * <ul>
 *   <li>{@code table}: loaded Paimon table handle per table mapping</li>
 *   <li>{@code schema}: schema cache keyed by table identity + schema id</li>
 * </ul>
 *
 * <p>Latest snapshot metadata is modeled as a runtime projection memoized inside the table cache
 * value instead of as an independent cache entry.
 *
 * <p>Invalidation behavior:
 * <ul>
 *   <li>db/table invalidation clears table and schema entries by matching local names</li>
 *   <li>partition-level invalidation falls back to table-level invalidation</li>
 * </ul>
 */
public class PaimonExternalMetaCache extends AbstractExternalMetaCache {
    public static final String ENGINE = "paimon";
    public static final String ENTRY_TABLE = "table";
    public static final String ENTRY_SCHEMA = "schema";

    private final EntryHandle<NameMapping, PaimonTableCacheValue> tableEntry;
    private final EntryHandle<PaimonSchemaCacheKey, SchemaCacheValue> schemaEntry;
    private final PaimonTableLoader tableLoader;
    private final PaimonLatestSnapshotProjectionLoader latestSnapshotProjectionLoader;

    public PaimonExternalMetaCache(ExecutorService refreshExecutor) {
        super(ENGINE, refreshExecutor);
        tableLoader = new PaimonTableLoader();
        latestSnapshotProjectionLoader = new PaimonLatestSnapshotProjectionLoader(
                new PaimonPartitionInfoLoader(tableLoader), this::getPaimonSchemaCacheValue);
        tableEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_TABLE, NameMapping.class, PaimonTableCacheValue.class,
                this::loadTableCacheValue, defaultEntryCacheSpec(),
                MetaCacheEntryInvalidation.forNameMapping(nameMapping -> nameMapping)));
        schemaEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_SCHEMA, PaimonSchemaCacheKey.class,
                SchemaCacheValue.class, this::loadSchemaCacheValue, defaultSchemaCacheSpec(),
                MetaCacheEntryInvalidation.forNameMapping(PaimonSchemaCacheKey::getNameMapping)));
    }

    public Table getPaimonTable(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        return tableEntry.get(nameMapping.getCtlId()).get(nameMapping).getPaimonTable();
    }

    public Table getPaimonTable(NameMapping nameMapping) {
        return tableEntry.get(nameMapping.getCtlId()).get(nameMapping).getPaimonTable();
    }

    public PaimonSnapshotCacheValue getSnapshotCache(ExternalTable dorisTable) {
        NameMapping nameMapping = dorisTable.getOrBuildNameMapping();
        return tableEntry.get(nameMapping.getCtlId()).get(nameMapping).getLatestSnapshotCacheValue();
    }

    public PaimonSchemaCacheValue getPaimonSchemaCacheValue(NameMapping nameMapping, long schemaId) {
        SchemaCacheValue schemaCacheValue = schemaEntry.get(nameMapping.getCtlId())
                .get(new PaimonSchemaCacheKey(nameMapping, schemaId));
        return (PaimonSchemaCacheValue) schemaCacheValue;
    }

    private PaimonTableCacheValue loadTableCacheValue(NameMapping nameMapping) {
        Table paimonTable = tableLoader.load(nameMapping);
        return new PaimonTableCacheValue(paimonTable,
                () -> latestSnapshotProjectionLoader.load(nameMapping, paimonTable));
    }

    private SchemaCacheValue loadSchemaCacheValue(PaimonSchemaCacheKey key) {
        ExternalTable dorisTable = findExternalTable(key.getNameMapping(), ENGINE);
        return dorisTable.initSchemaAndUpdateTime(key).orElseThrow(() ->
                new CacheException("failed to load paimon schema cache value for: %s.%s.%s, schemaId: %s",
                        null, key.getNameMapping().getCtlId(), key.getNameMapping().getLocalDbName(),
                        key.getNameMapping().getLocalTblName(), key.getSchemaId()));
    }

    @Override
    protected Map<String, String> catalogPropertyCompatibilityMap() {
        return singleCompatibilityMap(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND, ENTRY_SCHEMA);
    }
}
