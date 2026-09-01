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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.datasource.mvcc.MvccUtil;

import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class MTMVRefreshContext {
    private MTMV mtmv;
    private Map<String, Map<MTMVRelatedTableIf, Set<String>>> partitionMappings;
    private MTMVBaseVersions baseVersions;
    // Within the same context, repeated fetches of the same table's snapshot must return consistent values.
    // Hence, the results are cached at this stage.
    // The value is loaded/cached on the first fetch
    private Map<BaseTableInfo, MTMVSnapshotIf> baseTableSnapshotCache = Maps.newHashMap();
    private final Map<MTMVRelatedTableIf, Map<String, MTMVSnapshotIf>> partitionSnapshotCache = Maps.newHashMap();
    private final Map<MTMVRelatedTableIf, Set<String>> missingPartitionSnapshotCache = Maps.newHashMap();
    private final Map<MTMVRelatedTableIf, Map<String, AnalysisException>> partitionSnapshotFailureCache =
            Maps.newHashMap();
    private final Function<MTMVRelatedTableIf, Optional<MvccSnapshot>> snapshotResolver;

    public MTMVRefreshContext(MTMV mtmv) {
        this(mtmv, MvccUtil::getSnapshotFromContext);
    }

    private MTMVRefreshContext(MTMV mtmv,
            Function<MTMVRelatedTableIf, Optional<MvccSnapshot>> snapshotResolver) {
        this.mtmv = mtmv;
        this.snapshotResolver = snapshotResolver;
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

    public Map<BaseTableInfo, MTMVSnapshotIf> getBaseTableSnapshotCache() {
        return baseTableSnapshotCache;
    }

    /** Loads the union of mapped base partitions once per related table. */
    public PreparedPartitionSnapshots preparePartitionSnapshots(Set<String> mtmvPartitionNames)
            throws AnalysisException {
        return preparePartitionSnapshots(mtmvPartitionNames, false);
    }

    /** Loads only mappings whose persisted partition-name set still matches and needs version comparison. */
    public PreparedPartitionSnapshots prepareComparablePartitionSnapshots(Set<String> mtmvPartitionNames)
            throws AnalysisException {
        return preparePartitionSnapshots(mtmvPartitionNames, true);
    }

    private PreparedPartitionSnapshots preparePartitionSnapshots(
            Set<String> mtmvPartitionNames, boolean comparableOnly)
            throws AnalysisException {
        Map<MTMVRelatedTableIf, Set<String>> namesByTable = new LinkedHashMap<>();
        Map<MTMVRelatedTableIf, BaseTableInfo> tableInfos = comparableOnly
                ? new LinkedHashMap<>() : Collections.emptyMap();
        for (String mtmvPartitionName : mtmvPartitionNames) {
            for (Map.Entry<MTMVRelatedTableIf, Set<String>> entry
                    : getByPartitionName(mtmvPartitionName).entrySet()) {
                if (!entry.getKey().needAutoRefresh()) {
                    continue;
                }
                if (comparableOnly) {
                    BaseTableInfo tableInfo = tableInfos.computeIfAbsent(entry.getKey(), BaseTableInfo::new);
                    if (!Objects.equals(entry.getValue(), mtmv.getRefreshSnapshot()
                            .getPctSnapshots(mtmvPartitionName, tableInfo))) {
                        continue;
                    }
                }
                namesByTable.computeIfAbsent(entry.getKey(), ignored -> new LinkedHashSet<>())
                        .addAll(entry.getValue());
            }
        }
        for (Map.Entry<MTMVRelatedTableIf, Set<String>> entry : namesByTable.entrySet()) {
            loadSnapshots(entry.getKey(), entry.getValue());
        }
        return new PreparedPartitionSnapshots(this);
    }

    private void loadSnapshots(MTMVRelatedTableIf table, Set<String> partitionNames) throws AnalysisException {
        Map<String, MTMVSnapshotIf> cached = partitionSnapshotCache.computeIfAbsent(
                table, ignored -> new LinkedHashMap<>());
        Set<String> knownMissing = missingPartitionSnapshotCache.computeIfAbsent(
                table, ignored -> new LinkedHashSet<>());
        Set<String> missing = new LinkedHashSet<>(partitionNames);
        missing.removeAll(cached.keySet());
        missing.removeAll(knownMissing);
        if (missing.isEmpty()) {
            return;
        }
        Map<String, MTMVSnapshotIf> loaded = table.getPartitionSnapshots(
                missing, this, snapshotResolver.apply(table));
        if (loaded == null || loaded.containsKey(null) || loaded.containsValue(null)
                || !missing.containsAll(loaded.keySet())) {
            throw new AnalysisException("Invalid partition snapshot result for table " + table.getName()
                    + ": requestedCount=" + missing.size() + ", returnedCount="
                    + (loaded == null ? "null" : loaded.size()));
        }
        cached.putAll(loaded);
        missing.removeAll(loaded.keySet());
        knownMissing.addAll(missing);
    }

    void recordPartitionSnapshotFailure(
            MTMVRelatedTableIf table, String partitionName, AnalysisException failure) {
        partitionSnapshotFailureCache.computeIfAbsent(table, ignored -> new LinkedHashMap<>())
                .put(partitionName, failure);
    }

    public static MTMVRefreshContext buildContext(MTMV mtmv, Map<List<String>, Set<String>> queryUsedPartitions)
            throws AnalysisException {
        return buildContext(mtmv, queryUsedPartitions, MvccUtil::getSnapshotFromContext);
    }

    public static MTMVRefreshContext buildContext(MTMV mtmv, Map<List<String>, Set<String>> queryUsedPartitions,
            Map<MvccTableInfo, MvccSnapshot> pinnedSnapshots) throws AnalysisException {
        Map<MvccTableInfo, MvccSnapshot> snapshotCopy = new LinkedHashMap<>(pinnedSnapshots);
        return buildContext(mtmv, queryUsedPartitions,
                table -> Optional.ofNullable(snapshotCopy.get(new MvccTableInfo(table))));
    }

    private static MTMVRefreshContext buildContext(MTMV mtmv,
            Map<List<String>, Set<String>> queryUsedPartitions,
            Function<MTMVRelatedTableIf, Optional<MvccSnapshot>> snapshotResolver) throws AnalysisException {
        MTMVRefreshContext context = new MTMVRefreshContext(mtmv, snapshotResolver);
        context.partitionMappings = mtmv.calculatePartitionMappings(queryUsedPartitions, snapshotResolver);
        context.baseVersions = MTMVPartitionUtil.getBaseVersions(mtmv, context.partitionMappings);
        return context;
    }

    /** Read-only access to partition snapshots that have already been loaded as one bulk operation. */
    public static final class PreparedPartitionSnapshots {
        private final MTMVRefreshContext context;

        private PreparedPartitionSnapshots(MTMVRefreshContext context) {
            this.context = context;
        }

        public MTMVSnapshotIf get(MTMVRelatedTableIf table, String partitionName) throws AnalysisException {
            Map<String, MTMVSnapshotIf> snapshots = context.partitionSnapshotCache.get(table);
            if (snapshots != null && snapshots.containsKey(partitionName)) {
                return snapshots.get(partitionName);
            }
            Map<String, AnalysisException> failures = context.partitionSnapshotFailureCache.get(table);
            if (failures != null && failures.containsKey(partitionName)) {
                throw failures.get(partitionName);
            }
            Set<String> missing = context.missingPartitionSnapshotCache.get(table);
            if (missing != null && missing.contains(partitionName)) {
                throw new AnalysisException("can not find partition: " + partitionName);
            }
            throw new AnalysisException("Partition snapshot was not prepared: table=" + table.getName()
                    + ", partition=" + partitionName);
        }
    }

}
