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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.connector.spi.ConnectorMetadataAccessSource;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccUtil;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class MTMVRefreshContext {
    private MTMV mtmv;
    private Map<String, Map<MTMVRelatedTableIf, Set<String>>> partitionMappings;
    private MTMVBaseVersions baseVersions;
    private Set<BaseTableInfo> baseTables;
    private Set<MTMVRelatedTableIf> pctTables;
    private MTMVPartitionInfo.MTMVPartitionType partitionType;
    private Map<List<String>, Set<String>> queryUsedPartitions;
    private Map<MTMVRelatedTableIf, Map<String, PartitionItem>> partitionItems;
    private ConnectorMetadataAccessSource metadataAccessSource = ConnectorMetadataAccessSource.MTMV;
    // Within the same context, repeated fetches of the same table's snapshot must return consistent values.
    // Hence, the results are cached at this stage.
    // The value is loaded/cached on the first fetch
    private Map<BaseTableInfo, MTMVSnapshotIf> baseTableSnapshotCache = Maps.newHashMap();
    private Map<MTMVRelatedTableIf, Map<String, MTMVSnapshotIf>> partitionSnapshotCache = Maps.newHashMap();
    // Resolve each persisted base-table identity once per operation. User-session catalogs deliberately bypass
    // shared object caches, so resolving again inside an MTMV-partition loop would become one remote lookup per
    // MV partition even though freshness itself was already batch-preloaded.
    private Map<BaseTableInfo, TableIf> resolvedBaseTables = Maps.newHashMap();
    private Map<BaseTableInfo, AnalysisException> baseTableResolutionFailures = Maps.newHashMap();
    private Map<MTMVRelatedTableIf, BaseTableInfo> pctTableInfos = Maps.newHashMap();

    public MTMVRefreshContext(MTMV mtmv) {
        this.mtmv = mtmv;
    }

    public MTMV getMtmv() {
        return mtmv;
    }

    public Map<String, Map<MTMVRelatedTableIf, Set<String>>> getPartitionMappings() {
        return partitionMappings;
    }

    public Map<MTMVRelatedTableIf, Set<String>> getByPartitionName(String partitionName) {
        return partitionMappings.getOrDefault(partitionName, Maps.newHashMap());
    }

    public MTMVBaseVersions getBaseVersions() {
        return baseVersions;
    }

    public Set<BaseTableInfo> getBaseTables() {
        return baseTables;
    }

    public Set<MTMVRelatedTableIf> getPctTables() {
        return pctTables;
    }

    public MTMVPartitionInfo.MTMVPartitionType getPartitionType() {
        return partitionType;
    }

    public ConnectorMetadataAccessSource getMetadataAccessSource() {
        return metadataAccessSource;
    }

    public Map<BaseTableInfo, MTMVSnapshotIf> getBaseTableSnapshotCache() {
        return baseTableSnapshotCache;
    }

    public TableIf getBaseTable(BaseTableInfo baseTableInfo) throws AnalysisException {
        AnalysisException previousFailure = baseTableResolutionFailures.get(baseTableInfo);
        if (previousFailure != null) {
            throw previousFailure;
        }
        TableIf table = resolvedBaseTables.get(baseTableInfo);
        if (table != null) {
            return table;
        }
        try {
            table = MTMVUtil.getTable(baseTableInfo);
            resolvedBaseTables.put(baseTableInfo, table);
            return table;
        } catch (AnalysisException e) {
            baseTableResolutionFailures.put(baseTableInfo, e);
            throw e;
        }
    }

    public Map<String, MTMVSnapshotIf> getPartitionSnapshots(MTMVRelatedTableIf table,
            Set<String> partitionNames, Optional<MvccSnapshot> snapshot) throws AnalysisException {
        Map<String, MTMVSnapshotIf> cached = partitionSnapshotCache.computeIfAbsent(
                table, ignored -> Maps.newHashMap());
        Set<String> missing = Sets.difference(partitionNames, cached.keySet()).copyInto(Sets.newLinkedHashSet());
        if (!missing.isEmpty()) {
            Map<String, MTMVSnapshotIf> loaded = table.getPartitionSnapshots(
                    new ArrayList<>(missing), this, snapshot);
            if (!loaded.keySet().containsAll(missing)) {
                Set<String> absent = Sets.difference(missing, loaded.keySet());
                throw new AnalysisException("can not find partitions: " + absent);
            }
            cached.putAll(loaded);
        }
        Map<String, MTMVSnapshotIf> result = new LinkedHashMap<>();
        for (String partitionName : partitionNames) {
            result.put(partitionName, cached.get(partitionName));
        }
        return Collections.unmodifiableMap(result);
    }

    public MTMVSnapshotIf getCachedPartitionSnapshot(MTMVRelatedTableIf table, String partitionName) {
        Map<String, MTMVSnapshotIf> cached = partitionSnapshotCache.get(table);
        return cached == null ? null : cached.get(partitionName);
    }

    boolean hasPersistedPartitionSet(String mtmvPartitionName, MTMVRelatedTableIf table,
            Set<String> currentPartitions) {
        BaseTableInfo tableInfo = pctTableInfos.computeIfAbsent(table, BaseTableInfo::new);
        return Objects.equals(currentPartitions,
                mtmv.getRefreshSnapshot().getPctSnapshots(mtmvPartitionName, tableInfo));
    }

    boolean persistedPartitionSetsMatch(String mtmvPartitionName) {
        if (partitionType == MTMVPartitionInfo.MTMVPartitionType.SELF_MANAGE) {
            return true;
        }
        Map<MTMVRelatedTableIf, Set<String>> mapping = getByPartitionName(mtmvPartitionName);
        if (mapping == null || mapping.isEmpty()) {
            return false;
        }
        for (MTMVRelatedTableIf table : pctTables) {
            if (table.needAutoRefresh() && !hasPersistedPartitionSet(
                    mtmvPartitionName, table, mapping.getOrDefault(table, Collections.emptySet()))) {
                return false;
            }
        }
        return true;
    }

    boolean persistedPartitionSetsMatch(Set<String> mtmvPartitionNames) {
        for (String mtmvPartitionName : mtmvPartitionNames) {
            if (!persistedPartitionSetsMatch(mtmvPartitionName)) {
                return false;
            }
        }
        return true;
    }

    /** Preloads every external PCT partition after mapping capture and before refresh calculations take locks. */
    public void preloadPartitionSnapshots() throws AnalysisException {
        preloadPartitionSnapshots(partitionMappings.keySet());
    }

    /** Preloads the external PCT partition union required by the selected MTMV partitions. */
    public void preloadPartitionSnapshots(Set<String> mtmvPartitionNames) throws AnalysisException {
        preloadPartitionSnapshots(mtmvPartitionNames, true);
    }

    private void preloadPartitionSnapshots(Set<String> mtmvPartitionNames,
            boolean requirePersistedPartitionSet) throws AnalysisException {
        Map<MTMVRelatedTableIf, Set<String>> namesByTable = Maps.newHashMap();
        for (String mtmvPartitionName : mtmvPartitionNames) {
            Map<MTMVRelatedTableIf, Set<String>> mapping = getByPartitionName(mtmvPartitionName);
            for (Map.Entry<MTMVRelatedTableIf, Set<String>> entry : mapping.entrySet()) {
                if (entry.getKey().needAutoRefresh()
                        && entry.getKey().supportsPartitionSnapshotBatchLoading()
                        && (!requirePersistedPartitionSet
                                || hasPersistedPartitionSet(mtmvPartitionName, entry.getKey(), entry.getValue()))) {
                    namesByTable.computeIfAbsent(entry.getKey(), ignored -> Sets.newLinkedHashSet())
                            .addAll(entry.getValue());
                }
            }
        }
        for (Map.Entry<MTMVRelatedTableIf, Set<String>> entry : namesByTable.entrySet()) {
            getPartitionSnapshots(entry.getKey(), entry.getValue(),
                    MvccUtil.getSnapshotFromContext(entry.getKey()));
        }
    }

    /** Preloads every non-PCT table snapshot used by the context. */
    public void preloadTableSnapshots() throws AnalysisException {
        preloadTableSnapshots(baseTables, Collections.emptySet());
    }

    /** Preloads only the non-PCT table snapshots that the following freshness comparison can observe. */
    public void preloadTableSnapshots(Set<BaseTableInfo> tables, Set<TableNameInfo> excludeTables)
            throws AnalysisException {
        for (BaseTableInfo baseTableInfo : tables) {
            if (MTMVPartitionUtil.isTableExcluded(excludeTables, baseTableInfo)) {
                continue;
            }
            TableIf table = getBaseTable(baseTableInfo);
            if (!(table instanceof MTMVRelatedTableIf)) {
                continue;
            }
            MTMVRelatedTableIf relatedTable = (MTMVRelatedTableIf) table;
            if (!relatedTable.needAutoRefresh()) {
                continue;
            }
            if (partitionType != MTMVPartitionInfo.MTMVPartitionType.SELF_MANAGE
                    && pctTables.contains(relatedTable)) {
                continue;
            }
            MTMVPartitionUtil.getTableSnapshotFromContext(relatedTable, this);
        }
    }

    public void preloadSnapshots() throws AnalysisException {
        preloadPartitionSnapshots();
        preloadTableSnapshots();
    }

    public void preloadSnapshots(Set<String> mtmvPartitionNames) throws AnalysisException {
        preloadPartitionSnapshots(mtmvPartitionNames);
        preloadTableSnapshots();
    }

    /** Preloads the exact partition/table snapshot set used by one freshness comparison. */
    public void preloadSnapshots(Set<String> mtmvPartitionNames, Set<BaseTableInfo> tables,
            Set<TableNameInfo> excludeTables) throws AnalysisException {
        preloadPartitionSnapshots(mtmvPartitionNames);
        preloadTableSnapshots(tables, excludeTables);
    }

    /** Preloads snapshots that will be persisted after refresh, including first or mapping-changed baselines. */
    public void preloadSnapshotsForPersistence(Set<String> mtmvPartitionNames, Set<BaseTableInfo> tables)
            throws AnalysisException {
        preloadPartitionSnapshots(mtmvPartitionNames, false);
        preloadTableSnapshots(tables, Collections.emptySet());
    }

    public void refreshLocalState() throws AnalysisException {
        refreshLocalState(false);
    }

    /** Revalidates locked local structure using versions fetched and cached before the locks were acquired. */
    public void refreshLocalStateFromCachedVersions() throws AnalysisException {
        refreshLocalState(true);
    }

    /** Revalidates a rewrite context with its final query partition selection without remote version calls. */
    public void refreshLocalStateFromCachedVersions(Map<List<String>, Set<String>> queryUsedPartitions)
            throws AnalysisException {
        this.queryUsedPartitions = Collections.unmodifiableMap(new LinkedHashMap<>(queryUsedPartitions));
        refreshLocalState(true);
    }

    private void refreshLocalState(boolean cachedVersionsOnly) throws AnalysisException {
        for (MTMVRelatedTableIf pctTable : pctTables) {
            if (pctTable instanceof OlapTable) {
                partitionItems.put(pctTable,
                        pctTable.getAndCopyPartitionItems(MvccUtil.getSnapshotFromContext(pctTable)));
            }
        }
        partitionMappings = partitionItems.isEmpty()
                ? mtmv.calculatePartitionMappings(queryUsedPartitions)
                : mtmv.calculatePartitionMappings(queryUsedPartitions, partitionItems);
        baseVersions = cachedVersionsOnly
                ? MTMVPartitionUtil.getCachedBaseVersions(mtmv, partitionMappings, baseTables)
                : MTMVPartitionUtil.getBaseVersions(mtmv, partitionMappings, baseTables);
        baseTableSnapshotCache.keySet().removeIf(BaseTableInfo::isInternalTable);
        partitionSnapshotCache.keySet().removeIf(OlapTable.class::isInstance);
        resolvedBaseTables.keySet().removeIf(BaseTableInfo::isInternalTable);
        baseTableResolutionFailures.keySet().removeIf(BaseTableInfo::isInternalTable);
    }

    public static MTMVRefreshContext buildContext(MTMV mtmv) throws AnalysisException {
        return buildContext(mtmv, Maps.newHashMap());
    }

    public static MTMVRefreshContext buildContextForDisplay(MTMV mtmv) throws AnalysisException {
        MTMVRefreshContext context = buildContext(mtmv);
        context.metadataAccessSource = ConnectorMetadataAccessSource.DISPLAY;
        return context;
    }

    /** Keeps only the state needed to rebuild this cloud context from statement-local pins and version caches. */
    public MTMVRefreshContext compactCloudPreload() {
        MTMVRefreshContext compact = new MTMVRefreshContext(mtmv);
        compact.baseTables = baseTables;
        compact.pctTables = pctTables;
        compact.partitionType = partitionType;
        compact.queryUsedPartitions = Collections.emptyMap();
        compact.partitionItems = Collections.emptyMap();
        compact.partitionMappings = Collections.emptyMap();
        compact.metadataAccessSource = metadataAccessSource;
        return compact;
    }

    /** Rebuilds heavyweight mapping state without remote version access. */
    public MTMVRefreshContext rebuildFromCachedVersions(
            Map<List<String>, Set<String>> queryUsedPartitions) throws AnalysisException {
        return buildContext(mtmv, queryUsedPartitions, baseTables, true);
    }

    public static MTMVRefreshContext buildContext(MTMV mtmv,
            Map<List<String>, Set<String>> queryUsedPartitions) throws AnalysisException {
        MTMVRelation relation = mtmv.getRelation();
        Set<BaseTableInfo> baseTables = relation == null || relation.getBaseTablesOneLevelAndFromView() == null
                ? Collections.emptySet() : relation.getBaseTablesOneLevelAndFromView();
        return buildContext(mtmv, queryUsedPartitions, baseTables);
    }

    /** Builds a context against the relation resolved for the current operation, not a persisted stale relation. */
    public static MTMVRefreshContext buildContext(MTMV mtmv, Set<BaseTableInfo> baseTables)
            throws AnalysisException {
        return buildContext(mtmv, Maps.newHashMap(), baseTables);
    }

    /** Builds a context using both the current query partition filter and the relation resolved for this operation. */
    public static MTMVRefreshContext buildContext(MTMV mtmv,
            Map<List<String>, Set<String>> queryUsedPartitions, Set<BaseTableInfo> baseTables)
            throws AnalysisException {
        return buildContext(mtmv, queryUsedPartitions, baseTables, false);
    }

    private static MTMVRefreshContext buildContext(MTMV mtmv,
            Map<List<String>, Set<String>> queryUsedPartitions, Set<BaseTableInfo> baseTables,
            boolean cachedVersionsOnly) throws AnalysisException {
        MTMVRefreshContext context = new MTMVRefreshContext(mtmv);
        context.baseTables = Collections.unmodifiableSet(new LinkedHashSet<>(baseTables));
        context.pctTables = Collections.unmodifiableSet(new LinkedHashSet<>(
                mtmv.getMvPartitionInfo().getPctTables()));
        context.partitionType = mtmv.getMvPartitionInfo().getPartitionType();
        context.queryUsedPartitions = Collections.unmodifiableMap(new LinkedHashMap<>(queryUsedPartitions));
        context.partitionItems = Maps.newHashMap();
        for (MTMVRelatedTableIf pctTable : context.pctTables) {
            if (!(pctTable instanceof OlapTable)) {
                context.partitionItems.put(pctTable,
                        pctTable.getAndCopyPartitionItems(MvccUtil.getSnapshotFromContext(pctTable)));
            }
        }
        context.refreshLocalState(cachedVersionsOnly);
        return context;
    }

}
