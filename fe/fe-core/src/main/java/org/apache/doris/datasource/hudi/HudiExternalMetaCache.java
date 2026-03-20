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

import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.datasource.metacache.AbstractExternalMetaCache;
import org.apache.doris.datasource.metacache.MetaCacheEntryDef;
import org.apache.doris.datasource.metacache.MetaCacheEntryInvalidation;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Hudi engine implementation of {@link AbstractExternalMetaCache}.
 *
 * <p>Registered entries:
 * <ul>
 *   <li>{@code partition}: partition metadata keyed by table identity + snapshot timestamp + mode</li>
 *   <li>{@code fs_view}: {@link HoodieTableFileSystemView} keyed by {@link NameMapping}</li>
 *   <li>{@code meta_client}: {@link HoodieTableMetaClient} keyed by {@link NameMapping}</li>
 *   <li>{@code schema}: Hudi schema cache keyed by table identity + timestamp</li>
 * </ul>
 *
 * <p>Invalidation behavior:
 * <ul>
 *   <li>db/table invalidation clears all four entries for matching keys</li>
 *   <li>partition-level invalidation currently falls back to table-level invalidation</li>
 * </ul>
 */
public class HudiExternalMetaCache extends AbstractExternalMetaCache {
    private static final Logger LOG = LogManager.getLogger(HudiExternalMetaCache.class);

    public static final String ENGINE = "hudi";
    public static final String ENTRY_PARTITION = "partition";
    public static final String ENTRY_FS_VIEW = "fs_view";
    public static final String ENTRY_META_CLIENT = "meta_client";
    public static final String ENTRY_SCHEMA = "schema";

    private final EntryHandle<HudiPartitionCacheKey, TablePartitionValues> partitionEntry;
    private final EntryHandle<HudiFsViewCacheKey, HoodieTableFileSystemView> fsViewEntry;
    private final EntryHandle<HudiMetaClientCacheKey, HoodieTableMetaClient> metaClientEntry;
    private final EntryHandle<HudiSchemaCacheKey, SchemaCacheValue> schemaEntry;

    public HudiExternalMetaCache(ExecutorService refreshExecutor) {
        super(ENGINE, refreshExecutor);
        partitionEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_PARTITION, HudiPartitionCacheKey.class,
                TablePartitionValues.class, this::loadPartitionValuesCacheValue, defaultEntryCacheSpec(),
                MetaCacheEntryInvalidation.forNameMapping(HudiPartitionCacheKey::getNameMapping)));
        fsViewEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_FS_VIEW, HudiFsViewCacheKey.class,
                HoodieTableFileSystemView.class, this::createFsView, defaultEntryCacheSpec(),
                MetaCacheEntryInvalidation.forNameMapping(HudiFsViewCacheKey::getNameMapping)));
        metaClientEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_META_CLIENT, HudiMetaClientCacheKey.class,
                HoodieTableMetaClient.class, this::createHoodieTableMetaClient, defaultEntryCacheSpec(),
                MetaCacheEntryInvalidation.forNameMapping(HudiMetaClientCacheKey::getNameMapping)));
        schemaEntry = registerEntry(MetaCacheEntryDef.of(ENTRY_SCHEMA, HudiSchemaCacheKey.class,
                SchemaCacheValue.class, this::loadSchemaCacheValue, defaultSchemaCacheSpec(),
                MetaCacheEntryInvalidation.forNameMapping(HudiSchemaCacheKey::getNameMapping)));
    }

    public HoodieTableMetaClient getHoodieTableMetaClient(NameMapping nameMapping) {
        return metaClientEntry.get(nameMapping.getCtlId()).get(HudiMetaClientCacheKey.of(nameMapping));
    }

    public HoodieTableFileSystemView getFsView(NameMapping nameMapping) {
        return fsViewEntry.get(nameMapping.getCtlId()).get(HudiFsViewCacheKey.of(nameMapping));
    }

    public HudiSchemaCacheValue getHudiSchemaCacheValue(NameMapping nameMapping, long timestamp) {
        SchemaCacheValue schemaCacheValue = schemaEntry.get(nameMapping.getCtlId())
                .get(new HudiSchemaCacheKey(nameMapping, timestamp));
        return (HudiSchemaCacheValue) schemaCacheValue;
    }

    public TablePartitionValues getSnapshotPartitionValues(HMSExternalTable table,
            String timestamp, boolean useHiveSyncPartition) {
        return partitionEntry.get(table.getCatalog().getId()).get(
                HudiPartitionCacheKey.of(table.getOrBuildNameMapping(), Long.parseLong(timestamp),
                        useHiveSyncPartition));
    }

    public TablePartitionValues getPartitionValues(HMSExternalTable table, boolean useHiveSyncPartition)
            throws CacheException {
        HoodieTableMetaClient tableMetaClient = getHoodieTableMetaClient(table.getOrBuildNameMapping());
        TablePartitionValues emptyPartitionValues = new TablePartitionValues();
        Option<String[]> partitionColumns = tableMetaClient.getTableConfig().getPartitionFields();
        if (!partitionColumns.isPresent() || partitionColumns.get().length == 0) {
            return emptyPartitionValues;
        }
        HoodieTimeline timeline = tableMetaClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
        Option<HoodieInstant> lastInstant = timeline.lastInstant();
        if (!lastInstant.isPresent()) {
            return emptyPartitionValues;
        }
        long lastTimestamp = Long.parseLong(lastInstant.get().requestedTime());
        return partitionEntry.get(table.getCatalog().getId()).get(
                HudiPartitionCacheKey.of(table.getOrBuildNameMapping(), lastTimestamp, useHiveSyncPartition));
    }

    private HoodieTableFileSystemView createFsView(HudiFsViewCacheKey key) {
        HoodieTableMetaClient tableMetaClient = metaClientEntry.get(key.getNameMapping().getCtlId())
                .get(HudiMetaClientCacheKey.of(key.getNameMapping()));
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().build();
        HoodieLocalEngineContext ctx = new HoodieLocalEngineContext(tableMetaClient.getStorageConf());
        return FileSystemViewManager.createInMemoryFileSystemView(ctx, tableMetaClient, metadataConfig);
    }

    private HoodieTableMetaClient createHoodieTableMetaClient(HudiMetaClientCacheKey key) {
        LOG.debug("create hudi table meta client for {}", key.getNameMapping().getFullLocalName());
        HMSExternalTable hudiTable = findHudiTable(key.getNameMapping());
        HadoopStorageConfiguration hadoopStorageConfiguration =
                new HadoopStorageConfiguration(hudiTable.getCatalog().getConfiguration());
        return HiveMetaStoreClientHelper.ugiDoAs(
                hudiTable.getCatalog().getConfiguration(),
                () -> HoodieTableMetaClient.builder()
                        .setConf(hadoopStorageConfiguration)
                        .setBasePath(hudiTable.getRemoteTable().getSd().getLocation())
                        .build());
    }

    private TablePartitionValues loadPartitionValuesCacheValue(HudiPartitionCacheKey key) {
        HMSExternalTable hudiTable = findHudiTable(key.getNameMapping());
        HoodieTableMetaClient tableMetaClient = getHoodieTableMetaClient(key.getNameMapping());
        return loadPartitionValues(hudiTable, tableMetaClient, key.getTimestamp(), key.isUseHiveSyncPartition());
    }

    private TablePartitionValues loadPartitionValues(HMSExternalTable table, HoodieTableMetaClient tableMetaClient,
            long timestamp, boolean useHiveSyncPartition) {
        try {
            TablePartitionValues partitionValues = new TablePartitionValues();
            Option<String[]> partitionColumns = tableMetaClient.getTableConfig().getPartitionFields();
            if (!partitionColumns.isPresent() || partitionColumns.get().length == 0) {
                return partitionValues;
            }
            HoodieTimeline timeline = tableMetaClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
            List<String> partitionNames = loadPartitionNames(table, tableMetaClient, timeline, timestamp,
                    useHiveSyncPartition);
            List<String> partitionColumnsList = Arrays.asList(partitionColumns.get());
            partitionValues.addPartitions(partitionNames,
                    partitionNames.stream()
                            .map(partition -> HudiPartitionUtils.parsePartitionValues(partitionColumnsList, partition))
                            .collect(Collectors.toList()),
                    table.getHudiPartitionColumnTypes(timestamp),
                    Collections.nCopies(partitionNames.size(), 0L));
            partitionValues.setLastUpdateTimestamp(timestamp);
            return partitionValues;
        } catch (Exception e) {
            LOG.warn("Failed to get hudi partitions", e);
            throw new CacheException("Failed to get hudi partitions: " + Util.getRootCauseMessage(e), e);
        }
    }

    private List<String> loadPartitionNames(HMSExternalTable table, HoodieTableMetaClient tableMetaClient,
            HoodieTimeline timeline, long timestamp, boolean useHiveSyncPartition) throws Exception {
        Option<HoodieInstant> lastInstant = timeline.lastInstant();
        if (!lastInstant.isPresent()) {
            return Collections.emptyList();
        }
        long lastTimestamp = Long.parseLong(lastInstant.get().requestedTime());
        if (timestamp != lastTimestamp) {
            return HudiPartitionUtils.getPartitionNamesBeforeOrEquals(timeline, String.valueOf(timestamp));
        }
        if (!useHiveSyncPartition) {
            return HudiPartitionUtils.getAllPartitionNames(tableMetaClient);
        }
        HMSExternalCatalog catalog = (HMSExternalCatalog) table.getCatalog();
        List<String> partitionNames = catalog.getClient()
                .listPartitionNames(table.getRemoteDbName(), table.getRemoteName());
        partitionNames = partitionNames.stream().map(FileUtils::unescapePathName).collect(Collectors.toList());
        if (partitionNames.isEmpty()) {
            LOG.warn("Failed to get partitions from hms api, switch it from hudi api.");
            return HudiPartitionUtils.getAllPartitionNames(tableMetaClient);
        }
        return partitionNames;
    }

    private HMSExternalTable findHudiTable(NameMapping nameMapping) {
        ExternalTable dorisTable = findExternalTable(nameMapping, ENGINE);
        if (!(dorisTable instanceof HMSExternalTable)) {
            throw new CacheException("table %s.%s.%s is not hms external table when loading hudi cache",
                    null, nameMapping.getCtlId(), nameMapping.getLocalDbName(), nameMapping.getLocalTblName());
        }
        return (HMSExternalTable) dorisTable;
    }

    private SchemaCacheValue loadSchemaCacheValue(HudiSchemaCacheKey key) {
        ExternalTable dorisTable = findExternalTable(key.getNameMapping(), ENGINE);
        return dorisTable.initSchemaAndUpdateTime(key).orElseThrow(() ->
                new CacheException("failed to load hudi schema cache value for: %s.%s.%s, timestamp: %s",
                        null, key.getNameMapping().getCtlId(), key.getNameMapping().getLocalDbName(),
                        key.getNameMapping().getLocalTblName(), key.getTimestamp()));
    }

    @Override
    protected Map<String, String> catalogPropertyCompatibilityMap() {
        return singleCompatibilityMap(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND, ENTRY_SCHEMA);
    }
}
