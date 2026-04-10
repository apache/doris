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

package org.apache.doris.datasource.maxcompute;

import org.apache.doris.common.Config;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.metacache.AbstractExternalMetaCache;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.MetaCacheEntryDef;
import org.apache.doris.datasource.metacache.MetaCacheEntryInvalidation;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * MaxCompute engine implementation of {@link AbstractExternalMetaCache}.
 *
 * <p>Registered entries:
 * <ul>
 *   <li>{@code partition_values}: partition value/index structures per table</li>
 *   <li>{@code schema}: schema cache keyed by {@link SchemaCacheKey}</li>
 * </ul>
 */
public class MaxComputeExternalMetaCache extends AbstractExternalMetaCache {
    public static final String ENGINE = "maxcompute";
    public static final String ENTRY_PARTITION_VALUES = "partition_values";
    public static final String ENTRY_SCHEMA = "schema";
    private final EntryHandle<NameMapping, TablePartitionValues> partitionValuesEntry;
    private final EntryHandle<SchemaCacheKey, SchemaCacheValue> schemaEntry;

    public MaxComputeExternalMetaCache(ExecutorService refreshExecutor) {
        super(ENGINE, refreshExecutor);
        partitionValuesEntry = registerEntry(MetaCacheEntryDef.contextualOnly(
                ENTRY_PARTITION_VALUES,
                NameMapping.class,
                TablePartitionValues.class,
                CacheSpec.of(
                        true,
                        Config.external_cache_refresh_time_minutes * 60L,
                        Config.max_hive_partition_cache_num),
                MetaCacheEntryInvalidation.forNameMapping(nameMapping -> nameMapping)));
        schemaEntry = registerEntry(MetaCacheEntryDef.of(
                ENTRY_SCHEMA,
                SchemaCacheKey.class,
                SchemaCacheValue.class,
                this::loadSchemaCacheValue,
                defaultSchemaCacheSpec(),
                MetaCacheEntryInvalidation.forNameMapping(SchemaCacheKey::getNameMapping)));
    }

    @Override
    public Collection<String> aliases() {
        return Collections.singleton("max_compute");
    }

    public TablePartitionValues getPartitionValues(NameMapping nameMapping) {
        return partitionValuesEntry.get(nameMapping.getCtlId()).get(nameMapping, this::loadPartitionValues);
    }

    public MaxComputeSchemaCacheValue getMaxComputeSchemaCacheValue(long catalogId, SchemaCacheKey key) {
        SchemaCacheValue schemaCacheValue = schemaEntry.get(catalogId).get(key);
        return (MaxComputeSchemaCacheValue) schemaCacheValue;
    }

    private SchemaCacheValue loadSchemaCacheValue(SchemaCacheKey key) {
        ExternalTable dorisTable = findExternalTable(key.getNameMapping(), ENGINE);
        return dorisTable.initSchemaAndUpdateTime(key).orElseThrow(() ->
                new CacheException("failed to load maxcompute schema cache value for: %s.%s.%s",
                        null, key.getNameMapping().getCtlId(), key.getNameMapping().getLocalDbName(),
                        key.getNameMapping().getLocalTblName()));
    }

    private TablePartitionValues loadPartitionValues(NameMapping nameMapping) {
        MaxComputeSchemaCacheValue schemaCacheValue =
                getMaxComputeSchemaCacheValue(nameMapping.getCtlId(), new SchemaCacheKey(nameMapping));
        TablePartitionValues partitionValues = new TablePartitionValues();
        partitionValues.addPartitions(
                schemaCacheValue.getPartitionSpecs(),
                schemaCacheValue.getPartitionSpecs().stream()
                        .map(spec -> MaxComputeExternalTable.parsePartitionValues(
                                schemaCacheValue.getPartitionColumnNames(), spec))
                        .collect(java.util.stream.Collectors.toList()),
                schemaCacheValue.getPartitionTypes(),
                Collections.nCopies(schemaCacheValue.getPartitionSpecs().size(), 0L));
        return partitionValues;
    }

    @Override
    protected Map<String, String> catalogPropertyCompatibilityMap() {
        return singleCompatibilityMap(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND, ENTRY_SCHEMA);
    }
}
