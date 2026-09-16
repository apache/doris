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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.tvf.source.IndexDiskUsageScanNode;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TIndexDiskUsageMetadataParams;
import org.apache.doris.thrift.TIndexDiskUsageTablet;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The implement of table valued function
 * index_disk_usage("database" = "db1", "table" = "table1").
 * It reports the physical bytes of every inverted index of the table, split by component.
 */
public class IndexDiskUsageTableValuedFunction extends MetadataTableValuedFunction {
    public static final String NAME = "index_disk_usage";

    private static final String DATABASE = "database";
    private static final String TABLE = "table";
    private static final String PARTITIONS = "partitions";
    private static final String INDEXES = "indexes";
    private static final String LEVEL = "level";
    private static final String POSITION_DETAIL = "position_detail";

    private static final ImmutableSet<String> PROPERTIES_SET =
            ImmutableSet.of(DATABASE, TABLE, PARTITIONS, INDEXES, LEVEL, POSITION_DETAIL);
    private static final ImmutableSet<String> LEVELS = ImmutableSet.of("tablet", "rowset", "segment");

    private static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            varcharColumn("PARTITION_NAME"),
            varcharColumn("MATERIALIZED_INDEX_NAME"),
            bigintColumn("TABLET_ID"),
            bigintColumn("BACKEND_ID"),
            varcharColumn("ROWSET_ID"),
            new Column("SEGMENT_ID", ScalarType.createType(PrimitiveType.INT), true),
            bigintColumn("INDEX_ID"),
            varcharColumn("INDEX_NAME"),
            varcharColumn("INDEX_TYPE"),
            varcharColumn("COLUMN_NAME"),
            varcharColumn("INDEX_SUFFIX"),
            varcharColumn("STRUCTURE"),
            varcharColumn("STORAGE_FORMAT"),
            bigintColumn("SEGMENT_COUNT"),
            bigintColumn("ROW_COUNT"),
            bigintColumn("TOTAL_BYTES"),
            bigintColumn("DICT_BYTES"),
            bigintColumn("POSTING_BYTES"),
            bigintColumn("POSITION_BYTES"),
            bigintColumn("STATS_BYTES"),
            bigintColumn("OTHER_BYTES"),
            varcharColumn("STATS_SOURCE"));

    /**
     * A tablet of a base or rollup index to inspect, pinned to the visible version of its partition.
     */
    public static class TabletTarget {
        private final Tablet tablet;
        private final long partitionId;
        private final long materializedIndexId;
        private final long version;

        public TabletTarget(Tablet tablet, long partitionId, long materializedIndexId, long version) {
            this.tablet = tablet;
            this.partitionId = partitionId;
            this.materializedIndexId = materializedIndexId;
            this.version = version;
        }

        public long getMaterializedIndexId() {
            return materializedIndexId;
        }

        public Tablet getTablet() {
            return tablet;
        }

        public long getTabletId() {
            return tablet.getId();
        }

        public long getPartitionId() {
            return partitionId;
        }

        public long getVersion() {
            return version;
        }

        public TIndexDiskUsageTablet toThrift() {
            TIndexDiskUsageTablet target = new TIndexDiskUsageTablet();
            target.setTabletId(getTabletId());
            target.setPartitionId(partitionId);
            target.setMaterializedIndexId(materializedIndexId);
            target.setVersion(version);
            return target;
        }
    }

    private final String level;
    private final boolean positionDetail;
    private final List<Long> indexIds;
    private final Map<Long, String> partitionNames;
    private final Map<Long, String> materializedIndexNames;
    private final List<TabletTarget> tabletTargets;

    public IndexDiskUsageTableValuedFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> validParams = Maps.newHashMap();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String key = entry.getKey().toLowerCase();
            if (!PROPERTIES_SET.contains(key)) {
                throw new AnalysisException("'" + entry.getKey() + "' is invalid property");
            }
            validParams.put(key, entry.getValue());
        }
        String dbName = validParams.get(DATABASE);
        String tableName = validParams.get(TABLE);
        if (StringUtils.isEmpty(dbName) || StringUtils.isEmpty(tableName)) {
            throw new AnalysisException("'database' and 'table' are required for index_disk_usage");
        }
        this.level = parseLevel(validParams.getOrDefault(LEVEL, "tablet"));
        this.positionDetail = parsePositionDetail(validParams.getOrDefault(POSITION_DETAIL, "false"));
        checkShowPrivilege(dbName, tableName);

        OlapTable table = getOlapTable(dbName, tableName);
        String qualifiedName = dbName + "." + tableName;
        List<Long> resolvedIndexIds;
        List<Partition> partitions;
        Map<Long, String> resolvedPartitionNames = Maps.newLinkedHashMap();
        Map<Long, String> resolvedMaterializedIndexNames = Maps.newLinkedHashMap();
        Map<Long, List<Pair<Long, List<Tablet>>>> tabletsByPartition = Maps.newHashMap();
        List<Long> versions = null;
        table.readLock();
        try {
            resolvedIndexIds = resolveIndexIds(table, validParams.get(INDEXES), qualifiedName);
            partitions = resolvePartitions(table, validParams.get(PARTITIONS), qualifiedName);
            for (Partition partition : partitions) {
                resolvedPartitionNames.put(partition.getId(), partition.getName());
                // A light ADD INDEX also installs indexes on rollups, so their tablets can hold index files.
                List<Pair<Long, List<Tablet>>> indexTablets = Lists.newArrayList();
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    resolvedMaterializedIndexNames.putIfAbsent(index.getId(), table.getIndexNameById(index.getId()));
                    indexTablets.add(Pair.of(index.getId(), Lists.newArrayList(index.getTablets())));
                }
                tabletsByPartition.put(partition.getId(), indexTablets);
            }
            // Local replica choice filters replicas by version, so read it with the tablets it applies to.
            if (!Config.isCloudMode()) {
                versions = partitions.stream().map(Partition::getVisibleVersion).collect(Collectors.toList());
            }
        } finally {
            table.readUnlock();
        }
        // Cloud versions come from meta-service, so they are fetched without holding the table lock.
        if (versions == null) {
            versions = cloudVisibleVersions(partitions);
        }
        List<TabletTarget> targets = Lists.newArrayList();
        for (int i = 0; i < partitions.size(); ++i) {
            long partitionId = partitions.get(i).getId();
            for (Pair<Long, List<Tablet>> indexTablets : tabletsByPartition.get(partitionId)) {
                for (Tablet tablet : indexTablets.second) {
                    targets.add(new TabletTarget(tablet, partitionId, indexTablets.first, versions.get(i)));
                }
            }
        }
        this.indexIds = ImmutableList.copyOf(resolvedIndexIds);
        this.partitionNames = resolvedPartitionNames;
        this.materializedIndexNames = resolvedMaterializedIndexNames;
        this.tabletTargets = ImmutableList.copyOf(targets);
        checkTabletLimits();
    }

    public List<TabletTarget> getTabletTargets() {
        return tabletTargets;
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.INDEX_DISK_USAGE;
    }

    @Override
    public TMetaScanRange getMetaScanRange(List<String> requiredFields) {
        TIndexDiskUsageMetadataParams params = new TIndexDiskUsageMetadataParams();
        params.setLevel(level);
        params.setPositionDetail(positionDetail);
        params.setIndexIds(Lists.newArrayList(indexIds));
        params.setPartitionNames(Maps.newHashMap(partitionNames));
        params.setMaterializedIndexNames(Maps.newHashMap(materializedIndexNames));
        params.setTablets(tabletTargets.stream().map(TabletTarget::toThrift).collect(Collectors.toList()));
        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.INDEX_DISK_USAGE);
        metaScanRange.setIndexDiskUsageParams(params);
        return metaScanRange;
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        return new IndexDiskUsageScanNode(id, desc, this,
                ScanContext.builder().clusterName(sv.resolveCloudClusterName()).build());
    }

    @Override
    public String getTableName() {
        return "IndexDiskUsageTableValuedFunction";
    }

    @Override
    public List<Column> getTableColumns() {
        return SCHEMA;
    }

    private static Column varcharColumn(String name) {
        return new Column(name, ScalarType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH), true);
    }

    private static Column bigintColumn(String name) {
        return new Column(name, ScalarType.createType(PrimitiveType.BIGINT), true);
    }

    private static String parseLevel(String raw) {
        String normalized = raw.toLowerCase();
        if (!LEVELS.contains(normalized)) {
            throw new AnalysisException("Unsupported level '" + raw
                    + "' for index_disk_usage, expected tablet, rowset or segment");
        }
        return normalized;
    }

    private static boolean parsePositionDetail(String raw) {
        if ("true".equalsIgnoreCase(raw)) {
            return true;
        }
        if ("false".equalsIgnoreCase(raw)) {
            return false;
        }
        throw new AnalysisException("Invalid position_detail '" + raw + "', expected true or false");
    }

    private static void checkShowPrivilege(String dbName, String tableName) {
        ConnectContext ctx = ConnectContext.get();
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, InternalCatalog.INTERNAL_CATALOG_NAME,
                dbName, tableName, PrivPredicate.SHOW)) {
            throw new AnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR.formatErrorMsg("SHOW",
                    ctx.getQualifiedUser(), ctx.getRemoteIP(), dbName + ": " + tableName));
        }
    }

    private static OlapTable getOlapTable(String dbName, String tableName) {
        TableIf table;
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName);
            table = db.getTableOrAnalysisException(tableName);
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        if (!(table instanceof OlapTable)) {
            throw new AnalysisException("index_disk_usage only supports OLAP table");
        }
        return (OlapTable) table;
    }

    private static List<Partition> resolvePartitions(OlapTable table, String raw, String qualifiedName) {
        if (StringUtils.isEmpty(raw)) {
            return Lists.newArrayList(table.getPartitions());
        }
        List<Partition> partitions = Lists.newArrayList();
        for (String name : splitNames(PARTITIONS, raw)) {
            // Temporary partitions are excluded, so look the name up among formal partitions only.
            Partition partition = table.getPartition(name, false);
            if (partition == null) {
                throw new AnalysisException("Unknown partition '" + name + "' in table " + qualifiedName);
            }
            partitions.add(partition);
        }
        return partitions;
    }

    private static List<Long> resolveIndexIds(OlapTable table, String raw, String qualifiedName) {
        if (StringUtils.isEmpty(raw)) {
            return Lists.newArrayList();
        }
        List<Long> ids = Lists.newArrayList();
        for (String name : splitNames(INDEXES, raw)) {
            Index index = table.getIndexes().stream()
                    .filter(candidate -> candidate.getIndexName().equalsIgnoreCase(name))
                    .findFirst()
                    .orElseThrow(() -> new AnalysisException(
                            "Unknown index '" + name + "' in table " + qualifiedName));
            if (index.getIndexType() != IndexType.INVERTED && index.getIndexType() != IndexType.ANN) {
                throw new AnalysisException("Index '" + name + "' in table " + qualifiedName
                        + " is not an inverted or ANN index");
            }
            ids.add(index.getIndexId());
        }
        return ids;
    }

    private static List<Long> cloudVisibleVersions(List<Partition> partitions) {
        List<CloudPartition> cloudPartitions =
                partitions.stream().map(CloudPartition.class::cast).collect(Collectors.toList());
        List<Long> versions;
        try {
            versions = CloudPartition.getSnapshotVisibleVersion(cloudPartitions);
        } catch (RpcException e) {
            throw new AnalysisException("Failed to get partition visible versions: " + e.getMessage(), e);
        }
        for (int i = 0; i < versions.size(); ++i) {
            if (versions.get(i) < 0) {
                throw new AnalysisException(
                        "Visible version of partition '" + partitions.get(i).getName() + "' is not found");
            }
        }
        return versions;
    }

    // A filter that names nothing would silently widen or empty the scan, so it is rejected.
    private static Collection<String> splitNames(String property, String raw) {
        Collection<String> names = Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        if (names.isEmpty()) {
            throw new AnalysisException("'" + property + "' must list at least one name");
        }
        return names;
    }

    // Every tablet costs index file metadata reads, so a wide scan must be narrowed explicitly.
    private void checkTabletLimits() {
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        int limit = sessionVariable.indexDiskUsageMaxTablets;
        if (tabletTargets.size() > limit) {
            throw new AnalysisException(String.format("index_disk_usage covers %d tablets, exceeding "
                    + "index_disk_usage_max_tablets=%d; narrow partitions or indexes",
                    tabletTargets.size(), limit));
        }
        if (!positionDetail) {
            return;
        }
        int positionDetailLimit = sessionVariable.indexDiskUsagePositionDetailMaxTablets;
        if (tabletTargets.size() > positionDetailLimit) {
            throw new AnalysisException(String.format("position_detail covers %d tablets, exceeding "
                    + "index_disk_usage_position_detail_max_tablets=%d; narrow partitions or indexes",
                    tabletTargets.size(), positionDetailLimit));
        }
    }
}
