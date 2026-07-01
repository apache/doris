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

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.MaxLiteral;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TableSample;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ColumnToThrift;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.IndexToThriftConvertor;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.RowBinlogTableWrapper;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.stream.OlapTableStreamUpdate;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.plans.ScoreRangeInfo;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
import org.apache.doris.planner.normalize.Normalizer;
import org.apache.doris.planner.normalize.PartitionRangePredicateNormalizer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TAggregationType;
import org.apache.doris.thrift.TBinlogScanType;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TNormalizedOlapScanNode;
import org.apache.doris.thrift.TNormalizedPlanNode;
import org.apache.doris.thrift.TOlapScanNode;
import org.apache.doris.thrift.TOlapTableIndex;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TPartitionBoundary;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TSortInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// Full scan of an Olap table.
public class OlapScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(OlapScanNode.class);

    // average compression ratio in doris storage engine
    private static final int COMPRESSION_RATIO = 5;

    public static final String OLAP_START_TIMESTAMP = "startTimestamp";
    public static final String OLAP_END_TIMESTAMP = "endTimestamp";
    public static final String OLAP_INCREMENT_TYPE = "incrementType";

    /*
     * When the field value is ON, the storage engine can return the data directly
     * without pre-aggregation.
     * When the field value is OFF, the storage engine needs to aggregate the data
     * before returning to scan node. And if the table is an aggregation table,
     * all key columns need to be read an participate in aggregation.
     * For example:
     * Aggregate table: k1, k2, v1 sum
     * Field value is ON
     * Query1: select k1, sum(v1) from table group by k1
     * This aggregation function in query is same as the schema.
     * So the field value is ON while the query can scan data directly.
     *
     * Field value is OFF
     * Query1: select k1 , k2 from table
     * This aggregation info is null.
     * Query2: select k1, min(v1) from table group by k1
     * This aggregation function in query is min which different from the schema.
     * So the data stored in storage engine need to be merged firstly before
     * returning to scan node. Although we only queried key column k1, key column
     * k2 still needs to be detected and participate in aggregation to ensure the
     * results are correct.
     *
     * There are currently two places to modify this variable:
     * 1. The turnOffPreAgg() method of SingleNodePlanner.
     * This method will only be called on the left deepest OlapScanNode the plan
     * tree,
     * while other nodes are false by default (because the Aggregation operation is
     * executed after Join,
     * we cannot judge whether other OlapScanNodes can close the pre-aggregation).
     * So even the Duplicate key table, if it is not the left deepest node, it will
     * remain false too.
     *
     * 2. After MaterializedViewSelector selects the materialized view, the
     * updateScanRangeInfoByNewMVSelector()\
     * method of OlapScanNode may be called to update this variable.
     * This call will be executed on all ScanNodes in the plan tree. In this step,
     * for the DuplicateKey table, the variable will be set to true.
     * See comment of "isPreAggregation" variable in MaterializedViewSelector for
     * details.
     */
    private boolean isPreAggregation = false;
    private String reasonOfPreAggregation = null;
    private OlapTable olapTable = null;
    private String tableNameInPlan = null;
    private long totalTabletsNum = 0;
    private long selectedIndexId = -1;
    private Collection<Long> selectedPartitionIds = Lists.newArrayList();
    private long totalBytes = 0;
    // tablet id to single replica bytes
    private Map<Long, Long> tabletBytes = Maps.newLinkedHashMap();

    private SortInfo sortInfo = null;
    private Set<Integer> outputColumnUniqueIds = new HashSet<>();

    // When scan match sort_info, we can push limit into OlapScanNode.
    // It's limit for scanner instead of scanNode so we add a new limit.
    private long sortLimit = -1;

    // List of tablets will be scanned by current olap_scan_node
    private ArrayList<Long> scanTabletIds = Lists.newArrayList();

    private ArrayList<Long> scanReplicaIds = Lists.newArrayList();

    private Set<Long> sampleTabletIds = Sets.newHashSet();
    private Set<Long> nereidsPrunedTabletIds = Sets.newHashSet();
    private TableSample tableSample;

    private Map<Long, Integer> tabletId2BucketSeq = Maps.newHashMap();
    // a bucket seq may map to many tablets, and each tablet has a
    // TScanRangeLocations.
    public ArrayListMultimap<Integer, TScanRangeLocations> bucketSeq2locations = ArrayListMultimap.create();
    public Map<Integer, Long> bucketSeq2Bytes = Maps.newLinkedHashMap();

    private Set<Integer> distributionColumnIds;

    private long maxVersion = -1L;

    // Only for debug: restrict tablets to scan.
    private Set<Long> specifiedTabletIds = Sets.newHashSet();
    private SortInfo annSortInfo = null;
    private long annSortLimit = -1;

    private SortInfo scoreSortInfo = null;
    private long scoreSortLimit = -1;
    private ScoreRangeInfo scoreRangeInfo = null;

    // cached for prepared statement to quickly prune partition
    // only used in short circuit plan at present
    private final PartitionPruneV2ForShortCircuitPlan cachedPartitionPruner =
                        new PartitionPruneV2ForShortCircuitPlan();

    private boolean isTopnLazyMaterialize = false;
    private List<Column> topnLazyMaterializeOutputColumns = new ArrayList<>();

    private Column globalRowIdColumn;

    protected TableScanParams scanParams;

    // Constructs node to scan given data files of table 'tbl'.
    public OlapScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName, ScanContext scanContext) {
        super(id, desc, planNodeName, scanContext);
        olapTable = (OlapTable) desc.getTable();
        tableNameInPlan = olapTable.getName();
        distributionColumnIds = Sets.newTreeSet();

        Set<String> distColumnName = getDistributionColumnNames();
        // use for Nereids to generate uniqueId set for inverted index to avoid scan unnecessary big size column

        int columnId = 0;
        for (SlotDescriptor slotDescriptor : desc.getSlots()) {
            if (slotDescriptor.getColumn() != null) {
                outputColumnUniqueIds.add(slotDescriptor.getColumn().getUniqueId());
                if (distColumnName.contains(slotDescriptor.getColumn().getName().toLowerCase())) {
                    distributionColumnIds.add(columnId);
                }
                columnId++;
            }
        }
    }


    public boolean isPreAggregation() {
        return isPreAggregation;
    }

    public HashSet<Long> getScanBackendIds() {
        return scanBackendIds;
    }

    public void setTableSample(TableSample tSample) {
        this.tableSample = tSample;
    }

    public void setNereidsPrunedTabletIds(Set<Long> nereidsPrunedTabletIds) {
        this.nereidsPrunedTabletIds = nereidsPrunedTabletIds;
    }

    public long getTotalTabletsNum() {
        return totalTabletsNum;
    }

    public ArrayList<Long> getScanTabletIds() {
        return scanTabletIds;
    }

    public SortInfo getSortInfo() {
        return sortInfo;
    }

    public void setSortInfo(SortInfo sortInfo) {
        this.sortInfo = sortInfo;
    }

    public void setSortLimit(long sortLimit) {
        this.sortLimit = sortLimit;
    }

    public void setScoreSortInfo(SortInfo scoreSortInfo) {
        this.scoreSortInfo = scoreSortInfo;
    }

    public void setScoreSortLimit(long scoreSortLimit) {
        this.scoreSortLimit = scoreSortLimit;
    }

    public void setScoreRangeInfo(ScoreRangeInfo scoreRangeInfo) {
        this.scoreRangeInfo = scoreRangeInfo;
    }

    public void setAnnSortInfo(SortInfo annSortInfo) {
        this.annSortInfo = annSortInfo;
    }

    public void setAnnSortLimit(long annSortLimit) {
        this.annSortLimit = annSortLimit;
    }

    public Collection<Long> getSelectedPartitionIds() {
        return selectedPartitionIds;
    }

    // only used for UT and Nereids
    public void setSelectedPartitionIds(Collection<Long> selectedPartitionIds) {
        this.selectedPartitionIds = selectedPartitionIds;
    }

    /**
     * Only used for Nereids to set rollup or materialized view selection result.
     */
    public void setSelectedIndexInfo(
            long selectedIndexId,
            boolean isPreAggregation,
            String reasonOfPreAggregation) {
        this.selectedIndexId = selectedIndexId;
        this.isPreAggregation = isPreAggregation;
        this.reasonOfPreAggregation = reasonOfPreAggregation;
    }

    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    public OlapTable getOlapTable() {
        return olapTable;
    }

    public String getTableNameInPlan() {
        return tableNameInPlan;
    }

    /**
     * Init OlapScanNode, ONLY used for Nereids. Should NOT use this function in anywhere else.
     */
    public void init() throws UserException {
        selectedPartitionNum = selectedPartitionIds.size();
        try {
            createScanRangeLocations();
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }
    }


    @Override
    protected void computeNumNodes() {
        if (cardinality > 0) {
            numNodes = scanBackendIds.size();
        }
        // even current node scan has no data,at least on backend will be assigned when
        // the fragment actually execute
        numNodes = numNodes <= 0 ? 1 : numNodes;
    }


    private Collection<Long> partitionPrune(PartitionInfo partitionInfo) throws AnalysisException {
        PartitionPruner partitionPruner = null;
        Map<Long, PartitionItem> keyItemMap;
        keyItemMap = partitionInfo.getIdToItem(false);
        if (partitionInfo.getType() == PartitionType.RANGE) {
            if (isPointQuery() && partitionInfo.getPartitionColumns().size() == 1) {
                // short circuit, a quick path to find partition
                ColumnRange filterRange = columnNameToRange.get(partitionInfo.getPartitionColumns().get(0).getName());
                Range<ColumnBound> range = filterRange.getRangeSet().get().span();
                LiteralExpr lowerBound = range.lowerEndpoint().getValue();
                LiteralExpr upperBound = range.upperEndpoint().getValue();
                cachedPartitionPruner.update(keyItemMap);
                return cachedPartitionPruner.prune(lowerBound, upperBound);
            }
            partitionPruner = new RangePartitionPrunerV2(keyItemMap,
                    partitionInfo.getPartitionColumns(), columnNameToRange);
        } else if (partitionInfo.getType() == PartitionType.LIST) {
            partitionPruner = new ListPartitionPrunerV2(keyItemMap, partitionInfo.getPartitionColumns(),
                    columnNameToRange);
        }
        return partitionPruner.prune();
    }

    private Collection<Long> distributionPrune(
            List<Column> schema,
            List<Long> tabletIdsInOrder,
            DistributionInfo distributionInfo,
            boolean pruneTablesByNereids) throws AnalysisException {
        if (pruneTablesByNereids) {
            if (nereidsPrunedTabletIds.isEmpty()) {
                return null;
            }
            // Filter to tablets belonging to this partition. Without this, the caller's
            // per-partition loop in computeTabletInfo becomes O(partitionNum * globalPrunedSize)
            // getTablet hash lookups (most returning null), which dominates plan time
            // when both partition count and pruned tablet count are large.
            List<Long> result = new ArrayList<>();
            for (Long id : tabletIdsInOrder) {
                if (nereidsPrunedTabletIds.contains(id)) {
                    result.add(id);
                }
            }
            return result;
        }
        DistributionPruner distributionPruner = null;
        switch (distributionInfo.getType()) {
            case HASH: {
                HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
                distributionPruner = new HashDistributionPruner(schema, tabletIdsInOrder,
                        info.getDistributionColumns(),
                        columnFilters,
                        info.getBucketNum(),
                        getSelectedIndexId() == olapTable.getBaseIndexId());
                return new ArrayList<>(distributionPruner.prune());
            }
            case RANDOM: {
                return null;
            }
            default: {
                return null;
            }
        }
    }

    // Update the visible version of the scan range locations. for cloud mode. called as the end of
    // NereidsPlanner.splitFragments
    public void updateScanRangeVersions(Map<Long, Long> visibleVersionMap) {
        if (LOG.isDebugEnabled() && ConnectContext.get() != null) {
            LOG.debug("query id: {}, selectedPartitionIds: {}, visibleVersionMap: {}",
                    DebugUtil.printId(ConnectContext.get().queryId()), selectedPartitionIds, visibleVersionMap);
        }

        Map<Long, TScanRangeLocations> locationsMap = scanRangeLocations.stream()
                .collect(Collectors.toMap(loc -> loc.getScanRange().getPaloScanRange().getTabletId(), loc -> loc));
        for (Long partitionId : selectedPartitionIds) {
            final Partition partition = olapTable.getPartition(partitionId);
            final MaterializedIndex selectedTable = olapTable.getPartitionIndex(partition, selectedIndexId);
            final List<Tablet> tablets = selectedTable.getTablets();
            Long visibleVersion = visibleVersionMap.get(partitionId);
            assert visibleVersion != null : "the acquried version is not exists in the visible version map";
            String visibleVersionStr = String.valueOf(visibleVersion);
            for (Tablet tablet : tablets) {
                TScanRangeLocations locations = locationsMap.get(tablet.getId());
                if (locations == null) {
                    continue;
                }
                TPaloScanRange scanRange = locations.getScanRange().getPaloScanRange();
                scanRange.setVersion(visibleVersionStr);
            }
        }
        this.maxVersion = visibleVersionMap.values().stream().max(Long::compareTo).orElse(0L);
    }

    public Long getTabletSingleReplicaSize(Long tabletId) {
        return tabletBytes.get(tabletId);
    }

    public long getMaxVersion() {
        return maxVersion;
    }

    public void setSpecifiedTabletIds(Set<Long> specifiedTabletIds) {
        this.specifiedTabletIds = specifiedTabletIds;
    }

    private void addScanRangeLocations(Partition partition,
            List<Tablet> tablets, Map<Long, Set<Long>> backendAlivePathHashs) throws UserException {
        long visibleVersion = Partition.PARTITION_INIT_VERSION;

        // For cloud mode, set scan range visible version in Coordinator.exec so that we could
        // assign a snapshot version of all partitions.
        if (!(Config.isCloudMode() && Config.enable_cloud_snapshot_version)) {
            visibleVersion = partition.getVisibleVersion();
        }
        if (olapTable instanceof OlapTableStreamWrapper
                && ((OlapTableStreamWrapper) olapTable).getStreamUpdate(partition.getId()).second != null) {
            // legacy support, will be removed after full olap table stream history function ready
            visibleVersion = ((OlapTableStreamWrapper) olapTable).getStreamUpdate(partition.getId()).second;
        }
        // for non-cloud mode. for cloud mode see `updateScanRangeVersions`
        maxVersion = Math.max(maxVersion, visibleVersion);

        int useFixReplica = -1;
        boolean skipMissingVersion = false;
        ConnectContext context = ConnectContext.get();
        ComputeGroup computeGroup = null;
        if (context != null) {
            computeGroup = context.getComputeGroupSafely();
            useFixReplica = context.getSessionVariable().useFixReplica;
            if (useFixReplica == -1
                    && context.getState().isNereids() && context.getSessionVariable().getEnableQueryCache()) {
                useFixReplica = 0;
            }
            // if use_fix_replica is set to true, set skip_missing_version to false
            skipMissingVersion = useFixReplica == -1 && context.getSessionVariable().skipMissingVersion;
            if (LOG.isDebugEnabled()) {
                LOG.debug("query id: {}, partition id:{} visibleVersion: {}",
                        DebugUtil.printId(context.queryId()), partition.getId(), visibleVersion);
            }
        }
        boolean isInvalidComputeGroup = ComputeGroup.INVALID_COMPUTE_GROUP.equals(computeGroup);
        boolean isNotCloudComputeGroup = computeGroup != null && !Config.isCloudMode();

        ImmutableMap<Long, Backend> allBackends = olapTable.getAllBackendsByAllCluster();
        long partitionVisibleVersion = visibleVersion;
        String partitionVisibleVersionStr = fastToString(visibleVersion);
        // Lazy: resolved on the first CloudReplica that needs it.
        String cachedClusterId = null;
        for (Tablet tablet : tablets) {
            long tabletId = tablet.getId();
            long tabletVisibleVersion = partitionVisibleVersion;
            if (skipMissingVersion) {
                long tabletVersion = -1L;
                for (Replica replica : tablet.getReplicas()) {
                    if (replica.getVersion() > tabletVersion) {
                        tabletVersion = replica.getVersion();
                    }
                }
                if (tabletVersion != visibleVersion) {
                    LOG.warn("tablet {} version {} is not equal to partition {} version {}",
                            tabletId, tabletVersion, partition.getId(), visibleVersion);
                    tabletVisibleVersion = tabletVersion;
                    maxVersion = Math.max(maxVersion, visibleVersion);
                }
            }
            TScanRangeLocations locations = new TScanRangeLocations();
            TPaloScanRange paloRange = new TPaloScanRange();
            paloRange.setDbName("");
            paloRange.setSchemaHash("0");
            paloRange.setVersion(
                    tabletVisibleVersion == partitionVisibleVersion
                            ? partitionVisibleVersionStr
                            : fastToString(tabletVisibleVersion)
            );
            paloRange.setVersionHash("");
            paloRange.setTabletId(tabletId);
            if (olapTable instanceof RowBinlogTableWrapper) {
                TBinlogScanType binlogScanType =
                        parseBinlogScanType(scanParams, ((RowBinlogTableWrapper) olapTable).getOriginTable());
                if (((RowBinlogTableWrapper) olapTable).getParent().isPresent()) {
                    Pair<Long, Long> update = getStreamUpdate(partition.getId());
                    if (update.first != null) {
                        paloRange.setStartTso(update.first);
                    }
                    if (update.second != null) {
                        paloRange.setEndTso(update.second);
                    } else {
                        paloRange.setEndTso(partition.getTso());
                    }
                }
                if (binlogScanType != TBinlogScanType.NONE) {
                    paloRange.setBinlogScanType(binlogScanType);
                }
            }

            // random shuffle List && only collect one copy
            //
            // ATTN: visibleVersion is not used in cloud mode, see CloudReplica.checkVersionCatchup
            // for details.
            List<Replica> replicas = tablet.getQueryableReplicas(
                    visibleVersion, backendAlivePathHashs, skipMissingVersion);
            locations.setLocations(new ArrayList<>(replicas.size()));
            paloRange.setHosts(new ArrayList<>(replicas.size()));
            if (replicas.isEmpty()) {
                if (context.getSessionVariable().skipBadTablet) {
                    continue;
                }
                LOG.warn("no queryable replica found in tablet {}. visible version {}", tabletId, visibleVersion);
                StringBuilder sb = new StringBuilder(
                        "Failed to get scan range, no queryable replica found in tablet: " + tabletId);
                if (Config.show_details_for_unaccessible_tablet) {
                    sb.append(". Reason: ").append(tablet.getDetailsStatusForQuery(visibleVersion));
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug(sb.toString());
                }
                throw new UserException(sb.toString());
            }

            if (useFixReplica <= -1) {
                if (skipMissingVersion) {
                    // sort by replica's last success version, higher success version in the front.
                    replicas.sort(Replica.LAST_SUCCESS_VERSION_COMPARATOR);
                } else if (replicas.size() > 1) {
                    Collections.shuffle(replicas);
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("use fix replica, value: {}, replica count: {}", useFixReplica, replicas.size());
                }
                // sort by replica id
                replicas.sort(Replica.ID_COMPARATOR);
                Replica replica = replicas.get(useFixReplica >= replicas.size() ? replicas.size() - 1 : useFixReplica);
                if (context.getSessionVariable().fallbackOtherReplicaWhenFixedCorrupt) {
                    long beId;
                    if (replica instanceof CloudReplica) {
                        if (cachedClusterId == null) {
                            cachedClusterId = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                                    .getCurrentClusterId();
                        }
                        beId = ((CloudReplica) replica).getBackendIdWithClusterId(cachedClusterId);
                    } else {
                        beId = replica.getBackendId();
                    }
                    Backend backend = allBackends.get(beId);
                    // If the fixed replica is bad, then not clear the replicas using random replica
                    if (backend == null || !backend.isAlive()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("backend {} not exists or is not alive for replica {}", beId,
                                    replica.getId());
                        }
                        Collections.shuffle(replicas);
                    } else {
                        replicas.clear();
                        replicas.add(replica);
                    }
                } else {
                    replicas.clear();
                    replicas.add(replica);
                }
            }

            if (isEnableCooldownReplicaAffinity(context)) {
                final long coolDownReplicaId = tablet.getCooldownReplicaId();
                // we prefer to query using cooldown replica to make sure the cache is fully utilized
                // for example: consider there are 3BEs(A,B,C) and each has one replica for tablet X. and X
                // is now under cooldown
                // first time we choose BE A, and A will download data into cache while the other two's cache is empty
                // second time we choose BE B, this time B will be cached, C is still empty
                // third time we choose BE C, after this time all replica is cached
                // but it means we will do 3 S3 IO to get the data which will bring 3 slow query
                if (-1L != coolDownReplicaId) {
                    final Optional<Replica> replicaOptional = replicas.stream()
                            .filter(r -> r.getId() == coolDownReplicaId).findAny();
                    replicaOptional.ifPresent(
                            r -> {
                                Backend backend = allBackends.get(r.getBackendIdWithoutException());
                                if (backend != null && backend.isAlive()) {
                                    replicas.clear();
                                    replicas.add(r);
                                }
                            }
                    );
                }
            }

            boolean tabletIsNull = true;
            boolean collectedStat = false;
            boolean clusterException = false;
            List<String> errs = Lists.newArrayList();

            int replicaInTablet = 0;
            long oneReplicaBytes = 0;
            for (Replica replica : replicas) {
                Backend backend = null;
                long backendId = -1;
                try {
                    if (replica instanceof CloudReplica) {
                        if (cachedClusterId == null) {
                            cachedClusterId = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                                    .getCurrentClusterId();
                        }
                        backendId = ((CloudReplica) replica).getBackendIdWithClusterId(cachedClusterId);
                    } else {
                        backendId = replica.getBackendId();
                    }
                    backend = allBackends.get(backendId);
                } catch (ComputeGroupException e) {
                    LOG.warn("failed to get backend {} for replica {}", backendId, replica.getId(), e);
                    errs.add(e.toString());
                    clusterException = true;
                    continue;
                }
                if (backend == null || !backend.isAlive()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("backend {} not exists or is not alive for replica {}", backendId,
                                replica.getId());
                    }
                    String err = "replica " + replica.getId() + "'s backend " + backendId
                            + (backend != null ? " with tag " + backend.getLocationTag() : "")
                            + " does not exist or not alive";
                    errs.add(err);
                    continue;
                }
                if (!backend.isMixNode()) {
                    continue;
                }
                String beTagName = backend.getLocationTag().value;
                if (isInvalidComputeGroup || (isNotCloudComputeGroup && !computeGroup.containsBackend(beTagName))) {
                    String err = String.format(
                            "Replica on backend %d with tag %s," + " which is not in user's resource tag: %s",
                            backend.getId(), beTagName, computeGroup.toString());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(err);
                    }
                    errs.add(err);
                    continue;
                }
                scanReplicaIds.add(replica.getId());
                String ip = backend.getHost();
                int port = backend.getBePort();
                TNetworkAddress networkAddress = new TNetworkAddress(ip, port);
                TScanRangeLocation scanRangeLocation = new TScanRangeLocation(networkAddress);
                scanRangeLocation.setBackendId(backendId);
                locations.addToLocations(scanRangeLocation);
                paloRange.addToHosts(networkAddress);
                tabletIsNull = false;

                // for CBO
                if (!collectedStat && replica.getRowCount() != -1) {
                    long dataSize = replica.getDataSize();
                    if (replicaInTablet == 0) {
                        oneReplicaBytes = dataSize;
                        tabletBytes.put(tabletId, dataSize);
                    }
                    replicaInTablet++;
                    totalBytes += dataSize;
                    collectedStat = true;
                }
                scanBackendIds.add(backend.getId());
                // For skipping missing version of tablet, we only select the backend with the highest last
                // success version replica to save as much data as possible.
                if (skipMissingVersion) {
                    break;
                }
            }
            if (clusterException) {
                throw new UserException("tablet " + tabletId + " err: " + Joiner.on(", ").join(errs));
            }
            if (tabletIsNull) {
                throw new UserException("tablet " + tabletId + " has no queryable replicas. err: "
                        + Joiner.on(", ").join(errs));
            }
            TScanRange scanRange = new TScanRange();
            scanRange.setPaloScanRange(paloRange);
            locations.setScanRange(scanRange);

            addBucketSeqStatsIfNeeded(tabletId, locations, oneReplicaBytes);
            scanRangeLocations.add(locations);
        }
    }

    private void addBucketSeqStatsIfNeeded(long tabletId, TScanRangeLocations locations, long oneReplicaBytes) {
        if (!isPointQuery()) {
            Integer bucketSeq = tabletId2BucketSeq.get(tabletId);
            bucketSeq2locations.put(bucketSeq, locations);
            bucketSeq2Bytes.merge(bucketSeq, oneReplicaBytes, Long::sum);
        }
    }

    private String fastToString(long version) {
        if (version < 10) {
            return new String(new char[]{(char) ('0' + version)});
        }
        char[] chars = new char[24];
        chars[23] = '0';
        int index = 23;
        while (version > 0) {
            chars[index--] = (char) ('0' + (version % 10));
            version /= 10;
        }
        return new String(chars, index + 1, 23 - index);
    }

    private boolean isEnableCooldownReplicaAffinity(ConnectContext connectContext) {
        if (connectContext != null) {
            return connectContext.getSessionVariable().isEnableCooldownReplicaAffinity();
        }
        return true;
    }

    public void computePartitionInfo() throws AnalysisException {
        long start = System.currentTimeMillis();
        // Step1: compute partition ids
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo.getType() == PartitionType.RANGE || partitionInfo.getType() == PartitionType.LIST) {
            setHasPartitionPredicate(ScanNode.containsPartitionPredicate(
                    partitionInfo.getPartitionColumns(), desc, conjuncts, partitionInfo));
            selectedPartitionIds = partitionPrune(partitionInfo);
        } else {
            setHasPartitionPredicate(false);
            selectedPartitionIds = olapTable.getPartitionIds();
        }
        selectedPartitionIds = olapTable.selectNonEmptyPartitionIds(selectedPartitionIds);
        selectedPartitionNum = selectedPartitionIds.size();

        for (long id : selectedPartitionIds) {
            Partition partition = olapTable.getPartition(id);
            if (partition.getState() == PartitionState.RESTORE) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_PARTITION_STATE,
                        partition.getName(), "RESTORING");
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("partition prune cost: {} ms, partitions: {}",
                    (System.currentTimeMillis() - start), selectedPartitionIds);
        }
    }

    @Override
    protected void createScanRangeLocations() throws UserException {
        scanRangeLocations = Lists.newArrayList();
        if (selectedPartitionIds.isEmpty()) {
            return;
        }
        Preconditions.checkState(selectedIndexId != -1);
        // compute tablet info by selected index id and selected partition ids
        long start = System.currentTimeMillis();
        computeSampleTabletIds();
        computeTabletInfo();
        if (LOG.isDebugEnabled()) {
            LOG.debug("distribution prune cost: {} ms", (System.currentTimeMillis() - start));
        }
    }

    /**
     * Sample some tablets in the selected partition.
     * If Seek is specified, the tablets sampled each time are the same.
     */
    public void computeSampleTabletIds() {
        if (tableSample == null) {
            return;
        }
        OlapTable olapTable = (OlapTable) desc.getTable();

        // 1. Calculate the total number of rows in the selected partition, and sort partition list.
        long selectedRows = 0;
        long totalSampleRows = 0;
        List<Long> selectedPartitionList = new ArrayList<>();
        Preconditions.checkState(selectedIndexId != -1);
        for (Long partitionId : selectedPartitionIds) {
            final Partition partition = olapTable.getPartition(partitionId);
            final MaterializedIndex selectedIndex = olapTable.getPartitionIndex(partition, selectedIndexId);
            // selectedIndex is not expected to be null, because MaterializedIndex ids in one rollup's partitions
            // are all same. skip this partition here.
            if (selectedIndex != null) {
                selectedRows += selectedIndex.getRowCount();
                selectedPartitionList.add(partitionId);
            }
        }
        selectedPartitionList.sort(Comparator.naturalOrder());

        // 2.Sampling is not required in some cases, will not take effect after clear sampleTabletIds.
        if (tableSample.isPercent()) {
            if (tableSample.getSampleValue() >= 100) {
                return;
            }
            totalSampleRows = (long) Math.max(selectedRows * (tableSample.getSampleValue() / 100.0), 1);
        } else {
            if (tableSample.getSampleValue() > selectedRows) {
                return;
            }
            totalSampleRows = tableSample.getSampleValue();
        }

        // 3. Sampling partition. If Seek is specified, the partition will be the same for each sampling.
        long hitRows = 0; // The number of rows hit by the tablet
        Set<Long> hitTabletIds = Sets.newHashSet();
        long partitionSeek = tableSample.getSeek() != -1
                ? tableSample.getSeek() : (long) (new SecureRandom().nextDouble() * selectedPartitionList.size());
        for (int i = 0; i < selectedPartitionList.size(); i++) {
            int seekPid = (int) ((i + partitionSeek) % selectedPartitionList.size());
            final Partition partition = olapTable.getPartition(selectedPartitionList.get(seekPid));
            final MaterializedIndex selectedTable = olapTable.getPartitionIndex(partition, selectedIndexId);
            List<Tablet> tablets = selectedTable.getTablets();
            if (tablets.isEmpty()) {
                continue;
            }

            // 4. Calculate the number of rows that need to be sampled in the current partition.
            long sampleRows = 0; // The number of sample rows in partition
            if (tableSample.isPercent()) {
                sampleRows = (long) Math.max(selectedTable.getRowCount() * (tableSample.getSampleValue() / 100.0), 1);
            } else {
                sampleRows = (long) Math.max(
                        tableSample.getSampleValue() * (selectedTable.getRowCount() / selectedRows), 1);
            }

            // 5. Sampling tablets. If Seek is specified, the same tablet will be sampled each time.
            long tabletSeek = tableSample.getSeek() != -1
                    ? tableSample.getSeek() : (long) (new SecureRandom().nextDouble() * tablets.size());
            for (int j = 0; j < tablets.size(); j++) {
                int seekTid = (int) ((j + tabletSeek) % tablets.size());
                Tablet tablet = tablets.get(seekTid);
                if (!sampleTabletIds.isEmpty() && !sampleTabletIds.contains(tablet.getId())) {
                    // After PruneOlapScanTablet, sampleTabletIds.size() != 0,
                    // continue sampling only in sampleTabletIds.
                    // If it is percentage sample, the number of sampled rows is a percentage of the
                    // total number of rows, and It is not related to sampleTabletI after PruneOlapScanTablet.
                    continue;
                }
                long tabletRowCount;
                if (!FeConstants.runningUnitTest) {
                    tabletRowCount = tablet.getRowCount(true);
                } else {
                    tabletRowCount = selectedTable.getRowCount() / tablets.size();
                }
                if (tabletRowCount == 0) {
                    continue;
                }
                hitTabletIds.add(tablet.getId());
                sampleRows -= tabletRowCount;
                hitRows += tabletRowCount;
                if (sampleRows <= 0) {
                    break;
                }
            }
            if (hitRows > totalSampleRows) {
                break;
            }
        }
        if (!sampleTabletIds.isEmpty()) {
            sampleTabletIds.retainAll(hitTabletIds);
            if (LOG.isDebugEnabled()) {
                LOG.debug("after computeSampleTabletIds, hitRows {}, totalRows {}, selectedTablets {}, sampleRows {}",
                        hitRows, selectedRows, sampleTabletIds.size(), totalSampleRows);
            }
        } else {
            sampleTabletIds = hitTabletIds;
            if (LOG.isDebugEnabled()) {
                LOG.debug("after computeSampleTabletIds, hitRows {}, selectedRows {}, sampleRows {}",
                        hitRows, selectedRows, totalSampleRows);
            }
        }
    }

    public boolean isPointQuery() {
        return ConnectContext.get().getStatementContext().isShortCircuitQuery();
    }

    private void computeTabletInfo() throws UserException {
        /**
         * The tablet info could be computed only once.
         * So the scanBackendIds should be empty in the beginning.
         */
        Preconditions.checkState(scanBackendIds.isEmpty());
        Preconditions.checkState(scanTabletIds.isEmpty());
        Map<Long, Set<Long>> backendAlivePathHashs = Maps.newHashMap();
        for (Backend backend : olapTable.getAllBackendsByAllCluster().values()) {
            Set<Long> hashSet = Sets.newLinkedHashSet();
            for (DiskInfo diskInfo : backend.getDisks().values()) {
                if (diskInfo.isAlive()) {
                    hashSet.add(diskInfo.getPathHash());
                }
            }
            backendAlivePathHashs.put(backend.getId(), hashSet);
        }

        ConnectContext connectContext = ConnectContext.get();
        boolean isNereids = connectContext != null && connectContext.getState().isNereids();
        boolean isPointQuery = connectContext != null
                && connectContext.getStatementContext() != null
                && connectContext.getStatementContext().isShortCircuitQuery();
        for (Long partitionId : selectedPartitionIds) {
            final Partition partition = olapTable.getPartition(partitionId);
            final MaterializedIndex selectedTable = olapTable.getPartitionIndex(partition, selectedIndexId);
            final List<Tablet> tablets = Lists.newArrayList();
            List<Long> allTabletIds = selectedTable.getTabletIdsInOrder();
            // point query need prune tablets at this place
            Collection<Long> prunedTabletIds = distributionPrune(olapTable.getSchemaByIndexId(selectedIndexId),
                    allTabletIds, partition.getDistributionInfo(), isNereids && !isPointQuery);
            if (LOG.isDebugEnabled()) {
                LOG.debug("distribution prune tablets: {}", prunedTabletIds);
            }
            if (!sampleTabletIds.isEmpty()) {
                if (prunedTabletIds != null) {
                    prunedTabletIds.retainAll(sampleTabletIds);
                } else {
                    prunedTabletIds = sampleTabletIds;
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("after sample tablets: {}", prunedTabletIds);
                }
            }

            if (specifiedTabletIds != null && !specifiedTabletIds.isEmpty()) {
                if (prunedTabletIds != null) {
                    prunedTabletIds.retainAll(specifiedTabletIds);
                } else {
                    prunedTabletIds = new ArrayList<>(specifiedTabletIds);
                }
            }

            boolean notExistsSampleAndPrunedTablets = sampleTabletIds.isEmpty() && nereidsPrunedTabletIds.isEmpty();
            if (prunedTabletIds != null) {
                for (Long id : prunedTabletIds) {
                    Tablet tablet = selectedTable.getTablet(id);
                    if (tablet != null) {
                        tablets.add(tablet);
                        scanTabletIds.add(id);
                    } else if (notExistsSampleAndPrunedTablets) {
                        // The tabletID specified in query does not exist in this partition, skip scan partition.
                        throw new IllegalStateException("tablet " + id + " does not exist");
                    }
                }
            } else {
                tablets.addAll(selectedTable.getTablets());
                scanTabletIds.addAll(allTabletIds);
            }

            if (!isPointQuery()) {
                for (int i = 0; i < allTabletIds.size(); i++) {
                    tabletId2BucketSeq.put(allTabletIds.get(i), i);
                }
            }

            totalTabletsNum += selectedTable.getTablets().size();
            selectedSplitNum += tablets.size();
            addScanRangeLocations(partition, tablets, backendAlivePathHashs);
        }
    }

    /**
     * We query Palo Meta to get request's data location
     * extra result info will pass to backend ScanNode
     */
    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocations;
    }

    // Only called when Coordinator exec in high performance point query
    public List<TScanRangeLocations> lazyEvaluateRangeLocations() throws UserException {
        // Lazy evaluation
        selectedIndexId = olapTable.getBaseIndexId();
        // Only key columns
        computeColumnsFilter(olapTable.getBaseSchemaKeyColumns(), olapTable.getPartitionInfo());
        computePartitionInfo();
        scanBackendIds.clear();
        scanTabletIds.clear();
        tabletId2BucketSeq.clear();
        bucketSeq2locations.clear();
        bucketSeq2Bytes.clear();
        scanReplicaIds.clear();
        sampleTabletIds.clear();
        try {
            createScanRangeLocations();
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }
        return scanRangeLocations;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        long selectedIndexIdForExplain = selectedIndexId;
        if (selectedIndexIdForExplain == -1) {
            // If there is no data in table, the selectedIndexId will be -1, set it to base index id,
            // so that to avoid "null" in explain result.
            selectedIndexIdForExplain = olapTable.getBaseIndexId();
        }
        String indexName = olapTable.getIndexNameById(selectedIndexIdForExplain);
        output.append(prefix).append("TABLE: ").append(olapTable.getQualifiedName())
                .append("(").append(indexName).append(")");
        if (detailLevel == TExplainLevel.BRIEF) {
            output.append("\n").append(prefix).append(String.format("cardinality=%,d", cardinality));
            if (cardinalityAfterFilter != -1) {
                output.append("\n").append(prefix).append(String.format("afterFilter=%,d", cardinalityAfterFilter));
            }
            if (!conjuncts.isEmpty()) {
                output.append("\n").append(prefix).append("PREDICATES: ").append(conjuncts.size()).append("\n");
            }
            return output.toString();
        }
        if (isPreAggregation) {
            output.append(", PREAGGREGATION: ON");
        } else {
            output.append(", PREAGGREGATION: OFF. Reason: ").append(reasonOfPreAggregation);
        }
        output.append("\n");

        if (sortColumn != null) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }
        if (sortInfo != null) {
            output.append(prefix).append("SORT INFO:\n");
            sortInfo.getOrderingExprs().forEach(expr -> {
                output.append(prefix).append(prefix)
                        .append(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE)).append("\n");
            });
        }
        if (sortLimit != -1) {
            output.append(prefix).append("SORT LIMIT: ").append(sortLimit).append("\n");
        }
        if (scoreSortInfo != null) {
            output.append(prefix).append("SCORE SORT INFO:\n");
            scoreSortInfo.getOrderingExprs().forEach(expr -> {
                output.append(prefix).append(prefix)
                        .append(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE)).append("\n");
            });
        }
        if (scoreSortLimit != -1) {
            output.append(prefix).append("SCORE SORT LIMIT: ").append(scoreSortLimit).append("\n");
        }

        if (annSortInfo != null) {
            output.append(prefix).append("ANN SORT INFO:\n");
            annSortInfo.getOrderingExprs().forEach(expr -> {
                output.append(prefix).append(prefix)
                        .append(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE)).append("\n");
            });
        }
        if (annSortLimit != -1) {
            output.append(prefix).append("ANN SORT LIMIT: ").append(annSortLimit).append("\n");
        }

        if (useTopnFilter()) {
            String topnFilterSources = String.join(",",
                    topnFilterSortNodes.stream()
                            .map(node -> node.getId().asInt() + "").collect(Collectors.toList()));
            output.append(prefix).append("TOPN OPT:").append(topnFilterSources).append("\n");
        }

        if (!conjuncts.isEmpty()) {
            Expr expr = convertConjunctsToAndCompoundPredicate(conjuncts);
            output.append(prefix).append("PREDICATES: ")
                    .append(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE)).append("\n");
        }

        String selectedPartitions = getSelectedPartitionIds().stream().sorted()
                .map(id -> olapTable.getPartition(id).getName())
                .collect(Collectors.joining(","));
        output.append(prefix).append(String.format("partitions=%s/%s (%s)", selectedPartitionNum,
                olapTable.getPartitions().size(), selectedPartitions)).append("\n");
        output.append(prefix).append(String.format("tablets=%s/%s", selectedSplitNum, totalTabletsNum));
        // We print up to 3 tablet, and we print "..." if the number is more than 3
        if (scanTabletIds.size() > 3) {
            List<Long> firstTenTabletIds = scanTabletIds.subList(0, 3);
            output.append(String.format(", tabletList=%s ...", Joiner.on(",").join(firstTenTabletIds)));
        } else {
            output.append(String.format(", tabletList=%s", Joiner.on(",").join(scanTabletIds)));
        }
        output.append("\n");

        output.append(prefix).append(String.format("cardinality=%s", cardinality))
                .append(String.format(", avgRowSize=%s", avgRowSize)).append(String.format(", numNodes=%s", numNodes));
        output.append("\n");
        if (pushDownAggNoGroupingOp != null) {
            output.append(prefix).append("pushAggOp=").append(pushDownAggNoGroupingOp).append("\n");
        }
        if (isPointQuery()) {
            output.append(prefix).append("SHORT-CIRCUIT\n");
        }
        if (fragment.useSerialSource(ConnectContext.get())) {
            output.append(prefix).append("POOLING-SCAN\n");
        }

        printNestedColumns(output, prefix, getTupleDesc());

        return output.toString();
    }

    @Override
    public int getNumInstances() {
        // In pipeline exec engine, the instance num equals be_num * parallel instance.
        // so here we need count distinct be_num to do the work. make sure get right instance
        ConnectContext context = ConnectContext.get();
        if (context != null && context.getSessionVariable().isIgnoreStorageDataDistribution()) {
            return context.getSessionVariable().getParallelExecInstanceNum(scanContext.getClusterName());
        }
        return scanRangeLocations.size();
    }

    public int getBucketNum() {
        // In bucket shuffle join, we have 2 situation.
        // 1. Only one partition: in this case, we use scanNode.getTotalTabletsNum() to get the right bucket num
        //    because when table turn on dynamic partition, the bucket number in default distribution info
        //    is not correct.
        // 2. Table is colocated: in this case, table could have more than one partition, but all partition's
        //    bucket number must be same, so we use default bucket num is ok.
        if (olapTable.isColocateTable()) {
            return olapTable.getDefaultDistributionInfo().getBucketNum();
        } else {
            return (int) totalTabletsNum;
        }
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        List<String> keyColumnNames = new ArrayList<String>();
        List<TPrimitiveType> keyColumnTypes = new ArrayList<TPrimitiveType>();
        List<TColumn> columnsDesc = new ArrayList<TColumn>();
        List<TOlapTableIndex> indexDesc = Lists.newArrayList();
        if (isTopnLazyMaterialize) {
            Set<String> materializedColumnNames = topnLazyMaterializeOutputColumns.stream()
                    .map(Column::getName).collect(Collectors.toSet());
            olapTable.getColumnDesc(selectedIndexId, columnsDesc, keyColumnNames, keyColumnTypes,
                    materializedColumnNames);
            columnsDesc.add(ColumnToThrift.toThrift(globalRowIdColumn));
        } else {
            olapTable.getColumnDesc(selectedIndexId, columnsDesc, keyColumnNames, keyColumnTypes);

            // Add extra row id column
            ArrayList<SlotDescriptor> slots = desc.getSlots();
            Column lastColumn = slots.get(slots.size() - 1).getColumn();
            if (lastColumn != null && lastColumn.getName().equalsIgnoreCase(Column.ROWID_COL)) {
                TColumn tColumn = new TColumn();
                tColumn.setColumnName(Column.ROWID_COL);
                tColumn.setColumnType(ScalarType.createStringType().toColumnTypeThrift());
                tColumn.setAggregationType(TAggregationType.REPLACE);
                tColumn.setIsKey(false);
                tColumn.setIsAllowNull(false);
                // keep compatibility
                tColumn.setVisible(false);
                tColumn.setColUniqueId(Integer.MAX_VALUE);
                columnsDesc.add(tColumn);
            }
        }

        // Add virtual column to ColumnsDesc so that backend could
        // get correct table_schema.
        for (SlotDescriptor slot : desc.getSlots()) {
            if (slot.getVirtualColumn() != null) {
                TColumn tColumn = ColumnToThrift.toThrift(slot.getColumn());
                columnsDesc.add(tColumn);
            }
        }

        for (Index index : olapTable.getIndexes()) {
            TOlapTableIndex tIndex = IndexToThriftConvertor.toThrift(index, olapTable.getBaseSchema());
            indexDesc.add(tIndex);
        }

        msg.node_type = TPlanNodeType.OLAP_SCAN_NODE;
        if (olapTable.getBaseSchema().stream().anyMatch(Column::isClusterKey)) {
            keyColumnNames.clear();
            keyColumnTypes.clear();
        }
        msg.olap_scan_node = new TOlapScanNode(desc.getId().asInt(), keyColumnNames, keyColumnTypes, isPreAggregation);
        msg.olap_scan_node.setColumnsDesc(columnsDesc);
        msg.olap_scan_node.setIndexesDesc(indexDesc);
        if (olapTable instanceof RowBinlogTableWrapper) {
            msg.olap_scan_node.setReadRowBinlog(true);
        }
        if (selectedIndexId != -1) {
            msg.olap_scan_node.setSchemaVersion(olapTable.getIndexSchemaVersion(selectedIndexId));
        }
        if (null != sortColumn) {
            msg.olap_scan_node.setSortColumn(sortColumn);
        }
        if (sortInfo != null) {
            TSortInfo tSortInfo = new TSortInfo(
                    ExprToThriftVisitor.treesToThrift(sortInfo.getOrderingExprs()),
                    sortInfo.getIsAscOrder(),
                    sortInfo.getNullsFirst());
            msg.olap_scan_node.setSortInfo(tSortInfo);
        }
        if (sortLimit != -1) {
            msg.olap_scan_node.setSortLimit(sortLimit);
        }
        if (scoreSortInfo != null) {
            TSortInfo tScoreSortInfo = new TSortInfo(
                    ExprToThriftVisitor.treesToThrift(scoreSortInfo.getOrderingExprs()),
                    scoreSortInfo.getIsAscOrder(),
                    scoreSortInfo.getNullsFirst());
            msg.olap_scan_node.setScoreSortInfo(tScoreSortInfo);
        }
        if (scoreSortLimit != -1) {
            msg.olap_scan_node.setScoreSortLimit(scoreSortLimit);
        }
        if (scoreRangeInfo != null) {
            msg.olap_scan_node.setScoreRangeInfo(scoreRangeInfo.toThrift());
        }
        if (annSortInfo != null) {
            TSortInfo tAnnSortInfo = new TSortInfo(
                    ExprToThriftVisitor.treesToThrift(annSortInfo.getOrderingExprs()),
                    annSortInfo.getIsAscOrder(),
                    annSortInfo.getNullsFirst());
            msg.olap_scan_node.setAnnSortInfo(tAnnSortInfo);
        }
        if (annSortLimit != -1) {
            msg.olap_scan_node.setAnnSortLimit(annSortLimit);
        }
        if (selectedIndexId != -1) {
            msg.olap_scan_node.setKeyType(olapTable.getIndexMetaByIndexId(selectedIndexId).getKeysType().toThrift());
        } else {
            msg.olap_scan_node.setKeyType(olapTable.getKeysType().toThrift());
        }
        String tableName = olapTable.getName();
        if (selectedIndexId != -1) {
            tableName = tableName + "(" + getSelectedIndexName() + ")";
        }
        msg.olap_scan_node.setTableName(tableName);
        msg.olap_scan_node.setEnableUniqueKeyMergeOnWrite(olapTable.getEnableUniqueKeyMergeOnWrite());

        // Set MOR value predicate pushdown flag based on session variable
        if (olapTable.isMorTable() && ConnectContext.get() != null) {
            String dbName = olapTable.getQualifiedDbName();
            String tblName = olapTable.getName();
            boolean enabled = ConnectContext.get().getSessionVariable()
                    .isMorValuePredicatePushdownEnabled(dbName, tblName);
            msg.olap_scan_node.setEnableMorValuePredicatePushdown(enabled);
            if (ConnectContext.get().getSessionVariable()
                    .isReadMorAsDupEnabled(dbName, tblName)) {
                msg.olap_scan_node.setReadMorAsDup(true);
            }
        }

        msg.setPushDownAggTypeOpt(pushDownAggNoGroupingOp);

        msg.olap_scan_node.setPushDownAggTypeOpt(pushDownAggNoGroupingOp);
        // In TOlapScanNode , pushDownAggNoGroupingOp field is deprecated.

        if (outputColumnUniqueIds != null) {
            msg.olap_scan_node.setOutputColumnUniqueIds(outputColumnUniqueIds);
        }

        msg.olap_scan_node.setDistributeColumnIds(new ArrayList<>(distributionColumnIds));

        if (parseBinlogScanType(scanParams, olapTable) != TBinlogScanType.NONE
                || (selectedIndexId != -1 && olapTable.getIndexMetaByIndexId(selectedIndexId).isRowBinlogIndex())) {
            msg.olap_scan_node.setReadRowBinlog(true);
        }

        // Populate partition boundaries for BE-side runtime filter partition pruning.
        // Only serialize when this scan node actually has at least one runtime
        // filter whose target expression can drive partition pruning according
        // to the FE-side classifier, so we don't bloat thrift for tables with
        // many partitions but no usable RF target.
        // Gated by session variable `enable_runtime_filter_partition_prune`.
        ConnectContext rfPruneCtx = ConnectContext.get();
        if (rfPruneCtx != null
                && rfPruneCtx.getSessionVariable().isEnableRuntimeFilterPartitionPrune()
                && hasRfDrivingPartitionPruning()) {
            setPartitionBoundaries(msg.olap_scan_node);
        }

        super.toThrift(msg);
    }

    private boolean hasRfDrivingPartitionPruning() {
        if (selectedPartitionIds == null || selectedPartitionIds.size() < 2) {
            return false;
        }
        PlanNodeId myId = this.getId();
        for (RuntimeFilter rf : runtimeFilters) {
            if (rf.canPrunePartitionsFor(myId)) {
                return true;
            }
        }
        return false;
    }

    private void setPartitionBoundaries(TOlapScanNode olapScanNode) {
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        PartitionType partType = partitionInfo.getType();
        if (partType != PartitionType.RANGE && partType != PartitionType.LIST) {
            return;
        }
        List<Column> partColumns = partitionInfo.getPartitionColumns();
        if (partColumns.isEmpty()) {
            return;
        }

        // Build partition column name → slot ID mapping
        Map<String, Integer> partColToSlotId = Maps.newHashMap();
        for (SlotDescriptor slot : desc.getSlots()) {
            if (slot.getColumn() == null) {
                continue;
            }
            for (Column partCol : partColumns) {
                if (slot.getColumn().getName().equalsIgnoreCase(partCol.getName())) {
                    partColToSlotId.put(partCol.getName(), slot.getId().asInt());
                    break;
                }
            }
        }
        if (partColToSlotId.isEmpty()) {
            return;
        }

        List<TPartitionBoundary> boundaries = new ArrayList<>();
        for (Long partitionId : selectedPartitionIds) {
            PartitionItem item = partitionInfo.getItem(partitionId);
            if (item == null) {
                continue;
            }
            if (item instanceof RangePartitionItem) {
                addRangeBoundaries(boundaries, partitionId, (RangePartitionItem) item,
                        partColumns, partColToSlotId);
            } else if (item instanceof ListPartitionItem) {
                addListBoundaries(boundaries, partitionId, (ListPartitionItem) item,
                        partColumns, partColToSlotId);
            }
        }
        if (!boundaries.isEmpty()) {
            olapScanNode.setPartitionBoundaries(boundaries);
        }
    }

    private void addRangeBoundaries(List<TPartitionBoundary> boundaries, long partitionId,
            RangePartitionItem rangeItem, List<Column> partColumns,
            Map<String, Integer> partColToSlotId) {
        // We always project a (possibly multi-column) RANGE partition onto its
        // first partition column, since the BE pruner only consumes per-column
        // boundaries. Projection rules (lex compare semantics):
        //
        //   single column [L, U):
        //       projection = [L, U)              → range_end_inclusive = false
        //
        //   multi-column [(L1, L2, ...), (U1, U2, ...)):
        //       k1 = L1 is reachable (inner tuple can be ≥ (L2, ...))
        //       k1 ∈ (L1, U1) is fully reachable
        //       k1 = U1 may be reachable via inner tuple < (U2, ...)
        //       projection = [L1, U1] (CLOSED both ends, conservative)
        //                                        → range_end_inclusive = true
        //
        // The half-open form [L1, U1) for multi-column would be a strict
        // UNDER-approximation. Example: partition [(1,1), (1,5)) projects to
        // {1}, but [1, 1) is empty and would let the BE wrongly prune the
        // partition for an RF like k1 = 1.
        String colName = partColumns.get(0).getName();
        Integer slotId = partColToSlotId.get(colName);
        if (slotId == null) {
            return;
        }
        com.google.common.collect.Range<PartitionKey> range = rangeItem.getItems();
        TPartitionBoundary boundary = new TPartitionBoundary();
        boundary.setPartitionId(partitionId);
        boundary.setSlotId(slotId);
        if (range.hasLowerBound() && !range.lowerEndpoint().isMinValue()) {
            LiteralExpr lower = range.lowerEndpoint().getKeys().get(0);
            if (!(lower instanceof MaxLiteral)) {
                boundary.setRangeStart(
                        ExprToThriftVisitor.treeToThrift(lower).getNodes().get(0));
            }
        }
        if (range.hasUpperBound() && !range.upperEndpoint().isMaxValue()) {
            LiteralExpr upper = range.upperEndpoint().getKeys().get(0);
            if (!(upper instanceof MaxLiteral)) {
                boundary.setRangeEnd(
                        ExprToThriftVisitor.treeToThrift(upper).getNodes().get(0));
            }
        }
        if (partColumns.size() > 1) {
            boundary.setRangeEndInclusive(true);
        }
        boundaries.add(boundary);
    }

    private void addListBoundaries(List<TPartitionBoundary> boundaries, long partitionId,
            ListPartitionItem listItem, List<Column> partColumns,
            Map<String, Integer> partColToSlotId) {
        if (listItem.isDefaultPartition()) {
            return;
        }
        List<PartitionKey> partitionKeys = listItem.getItems();
        // For LIST partitions, emit per-column distinct value sets. NULL keys
        // are emitted as NULL_LITERAL TExprNode so the BE parser can translate
        // them into ColumnValueRange::set_contain_null(true) rather than
        // treating NULL as an ordinary fixed value (which would crash the
        // typed value extractor in the parser).
        for (int i = 0; i < partColumns.size(); i++) {
            String colName = partColumns.get(i).getName();
            Integer slotId = partColToSlotId.get(colName);
            if (slotId == null) {
                continue;
            }
            TPartitionBoundary boundary = new TPartitionBoundary();
            boundary.setPartitionId(partitionId);
            boundary.setSlotId(slotId);
            List<TExprNode> listValues = new ArrayList<>(partitionKeys.size());
            for (PartitionKey pk : partitionKeys) {
                LiteralExpr literalExpr = pk.getKeys().get(i);
                if (literalExpr.isNullLiteral()) {
                    listValues.add(ExprToThriftVisitor.treeToThrift(
                            NullLiteral.create(literalExpr.getType())).getNodes().get(0));
                } else {
                    listValues.add(
                            ExprToThriftVisitor.treeToThrift(literalExpr).getNodes().get(0));
                }
            }
            boundary.setListValues(listValues);
            boundaries.add(boundary);
        }
    }

    @Override
    public void normalize(TNormalizedPlanNode normalizedPlan, Normalizer normalizer) {
        TNormalizedOlapScanNode normalizedOlapScanNode = new TNormalizedOlapScanNode();
        normalizedOlapScanNode.setTableId(olapTable.getId());

        long selectIndexId = selectedIndexId == -1 ? olapTable.getBaseIndexId() : selectedIndexId;
        normalizedOlapScanNode.setIndexId(selectIndexId);
        normalizedOlapScanNode.setIsPreaggregation(isPreAggregation);
        normalizedOlapScanNode.setSortColumn(sortColumn);
        normalizedOlapScanNode.setRollupName(olapTable.getIndexNameById(selectIndexId));

        normalizeSchema(normalizedOlapScanNode);
        normalizeSelectColumns(normalizedOlapScanNode, normalizer);

        normalizedPlan.setNodeType(TPlanNodeType.OLAP_SCAN_NODE);
        normalizedPlan.setOlapScanNode(normalizedOlapScanNode);
    }

    private void normalizeSelectColumns(TNormalizedOlapScanNode normalizedOlapScanNode, Normalizer normalizer) {
        List<SlotDescriptor> slots = tupleIds
                .stream()
                .flatMap(tupleId -> normalizer.getDescriptorTable().getTupleDesc(tupleId).getSlots().stream())
                .collect(Collectors.toList());
        List<Pair<SlotId, String>> selectColumns = slots.stream()
                .map(slot -> {
                    // For variant subcolumns, use the materialized column name (e.g. "data.int_1")
                    // to distinguish different subcolumns of the same variant column in cache digest.
                    List<String> subColPath = slot.getSubColLables();
                    String colName = slot.getColumn().getName();
                    if (subColPath != null && !subColPath.isEmpty()) {
                        colName = colName + "." + String.join(".", subColPath);
                    }
                    return Pair.of(slot.getId(), colName);
                })
                .collect(Collectors.toList());
        for (Column partitionColumn : olapTable.getPartitionInfo().getPartitionColumns()) {
            boolean selectPartitionColumn = false;
            String partitionColumnName = partitionColumn.getName();
            for (Pair<SlotId, String> selectColumn : selectColumns) {
                if (selectColumn.second.equalsIgnoreCase(partitionColumnName)) {
                    selectPartitionColumn = true;
                    break;
                }
            }
            if (!selectPartitionColumn) {
                selectColumns.add(Pair.of(new SlotId(-1), partitionColumnName));
            }
        }

        selectColumns.sort(Comparator.comparing(Pair::value));

        for (Pair<SlotId, String> selectColumn : selectColumns) {
            normalizer.normalizeSlotId(selectColumn.first.asInt());
        }

        normalizedOlapScanNode.setSelectColumns(
                selectColumns.stream().map(Pair::value).collect(Collectors.toList())
        );
    }

    private void normalizeSchema(TNormalizedOlapScanNode normalizedOlapScanNode) {
        List<Column> columns = selectedIndexId == -1
                ? olapTable.getBaseSchema() : olapTable.getSchemaByIndexId(selectedIndexId);
        List<Column> keyColumns = columns.stream().filter(Column::isKey).collect(Collectors.toList());

        normalizedOlapScanNode.setKeyColumnNames(
                keyColumns.stream()
                        .map(Column::getName)
                        .collect(Collectors.toList())
        );

        normalizedOlapScanNode.setKeyColumnTypes(
                keyColumns.stream()
                        .map(column -> column.getDataType().toThrift())
                        .collect(Collectors.toList())
        );
    }

    @Override
    protected void normalizeConjuncts(TNormalizedPlanNode normalizedPlan, Normalizer normalizer) {
        List<Expr> normalizedPredicates = new PartitionRangePredicateNormalizer(normalizer, this)
                .normalize();

        List<TExpr> normalizedConjuncts = normalizeExprs(normalizedPredicates, normalizer);
        normalizedPlan.setConjuncts(normalizedConjuncts);
    }

    @Override
    protected void normalizeProjects(TNormalizedPlanNode normalizedPlanNode, Normalizer normalizer) {
        List<SlotDescriptor> outputSlots =
                getOutputTupleIds()
                        .stream()
                        .flatMap(tupleId -> normalizer.getDescriptorTable().getTupleDesc(tupleId).getSlots().stream())
                        .collect(Collectors.toList());

        List<Expr> projectList = this.projectList;
        if (projectList == null) {
            projectList = outputSlots.stream().map(SlotRef::new).collect(Collectors.toList());
        }

        List<TExpr> projectThrift = normalizeProjects(outputSlots, projectList, normalizer);
        normalizedPlanNode.setProjects(projectThrift);
    }

    public TupleId getTupleId() {
        Preconditions.checkNotNull(desc);
        return desc.getId();
    }

    @VisibleForTesting
    public String getSelectedIndexName() {
        return olapTable.getIndexNameById(selectedIndexId);
    }

    @Override
    public void finalizeForNereids() {
        computeNumNodes();
        computeStatsForNereids();
        // Update SlotDescriptor before construction of thrift message.
        int virtualColumnIdx = 0;
        for (SlotDescriptor slot : desc.getSlots()) {
            if (slot.getVirtualColumn() != null) {
                virtualColumnIdx++;
                // Set the name of virtual column to be unique.
                Column column = new Column("__DORIS_VIRTUAL_COL__" + virtualColumnIdx, slot.getType());
                // Just make sure the unique id is not conflict with other columns.
                column.setUniqueId(Integer.MAX_VALUE - virtualColumnIdx);
                column.setIsAllowNull(slot.getIsNullable());
                slot.setColumn(column);
            }
        }
    }

    private void computeStatsForNereids() {
        if (cardinality > 0 && avgRowSize <= 0) {
            avgRowSize = totalBytes / (float) cardinality * COMPRESSION_RATIO;
            capCardinalityAtLimit();
        }
        // when node scan has no data, cardinality should be 0 instead of an invalid
        // value after computeStats()
        cardinality = cardinality == -1 ? 0 : cardinality;
    }

    Set<String> getDistributionColumnNames() {
        return olapTable != null
                ? olapTable.getDistributionColumnNames()
                : Sets.newTreeSet();
    }

    /**
     * Update required_slots in scan node contexts. This is called after Nereids planner do the projection.
     * In the projection process, some slots may be removed. So call this to update the slots info.
     * Currently, it is only used by ExternalFileScanNode, add the interface here to keep the Nereids code clean.
     */
    public void updateRequiredSlots(PlanTranslatorContext context,
            Set<SlotId> requiredByProjectSlotIdSet) {
        outputColumnUniqueIds.clear();
        for (SlotDescriptor slot : context.getTupleDesc(this.getTupleId()).getSlots()) {
            if (requiredByProjectSlotIdSet.contains(slot.getId()) && slot.getColumn() != null) {
                outputColumnUniqueIds.add(slot.getColumn().getUniqueId());
            }
        }
        // Do not add input slots of virtual columns into outputColumnUniqueIds.
        // Backend can decide whether the underlying source columns are truly needed
        // (e.g., ANN distance index-only scan can produce the virtual distance without
        // reading the source vector column). Keeping only the real projected slots here
        // avoids forcing unnecessary reads in BE.
    }

    @Override
    public int getScanRangeNum() {
        return getScanTabletIds().size();
    }

    public void setIsTopnLazyMaterialize(boolean isTopnLazyMaterialize) {
        this.isTopnLazyMaterialize = isTopnLazyMaterialize;
    }

    public void addTopnLazyMaterializeOutputColumns(Column column) {
        this.topnLazyMaterializeOutputColumns.add(column);
    }

    public void setGlobalRowIdColumn(Column globalRowIdColumn) {
        this.globalRowIdColumn = globalRowIdColumn;
    }

    @Override
    public long getCatalogId() {
        if (olapTable != null) {
            return olapTable.getCatalogId();
        }
        return super.getCatalogId();
    }

    public OlapTableStreamUpdate getStreamUpdate() {
        Map<Long, Long> prev = Maps.newHashMap();
        Map<Long, Long> next = Maps.newHashMap();
        for (Long partitionId : getSelectedPartitionIds()) {
            Pair<Long, Long> streamUpdate = getStreamUpdate(partitionId);
            if (streamUpdate.first != null) {
                // prev could be null, in case of historical scan
                prev.put(partitionId, streamUpdate.first);
            }
            if (streamUpdate.second != null) {
                next.put(partitionId, streamUpdate.second);
            } else {
                // next could be null, in case of incremental scan
                next.put(partitionId, olapTable.getPartition(partitionId).getTso());
            }
        }
        return new OlapTableStreamUpdate(prev, next);
    }

    private Pair<Long, Long> getStreamUpdate(Long partitionId) {
        // unprotected assume partitionId is in SelectedPartitionIds
        Pair<Long, Long> streamUpdate;
        if (olapTable instanceof RowBinlogTableWrapper) {
            streamUpdate = ((RowBinlogTableWrapper) olapTable).getParent().get().getStreamUpdate(partitionId);
        } else {
            streamUpdate = ((OlapTableStreamWrapper) olapTable).getStreamUpdate(partitionId);
        }
        return streamUpdate;
    }

    public boolean isChangeScan() {
        return scanParams != null && scanParams.incrementalRead() && !isIncrementalScan();
    }

    public boolean isIncrementalScan() {
        return (olapTable instanceof RowBinlogTableWrapper)
                && ((RowBinlogTableWrapper) olapTable).getParent().isPresent();
    }

    public void setScanParams(TableScanParams scanParams) {
        this.scanParams = scanParams;
    }

    public long getIncrementalScanEndTime() {
        if (scanParams != null && scanParams.incrementalRead()
                && scanParams.getMapParams().containsKey(OLAP_END_TIMESTAMP)) {
            return parseChangeTimestamp(scanParams.getMapParams().get(OLAP_END_TIMESTAMP));
        }
        return 0;
    }

    public static long parseChangeTimestamp(String ts) {
        if (ts != null) {
            long changeTimestamp;
            if (ts.equals("0")) {
                changeTimestamp = 0;
            } else {
                changeTimestamp = TimeUtils.timeStringToLong(ts);
            }
            if (changeTimestamp < 0) {
                throw new ParseException("Invalid TIMESTAMP format in incr clause: " + ts);
            }
            return changeTimestamp;
        }
        throw new ParseException("Invalid timestamp:" + ts);
    }

    public static TBinlogScanType parseBinlogScanType(TableScanParams scanParams, OlapTable olapTable)
            throws ParseException {
        if (scanParams == null) {
            return TBinlogScanType.NONE;
        }
        TBinlogScanType scanType = TBinlogScanType.MIN_DELTA;
        if (olapTable.getKeysType() == KeysType.DUP_KEYS) {
            scanType = TBinlogScanType.APPEND_ONLY;
        }
        if (scanParams.getMapParams().containsKey(OlapScanNode.OLAP_INCREMENT_TYPE)) {
            String info = scanParams.getMapParams().get(OlapScanNode.OLAP_INCREMENT_TYPE).toUpperCase();
            if ("APPEND_ONLY".equals(info) || olapTable.getKeysType() == KeysType.DUP_KEYS) {
                scanType = TBinlogScanType.APPEND_ONLY;
            } else if ("MIN_DELTA".equals(info)) {
                scanType = TBinlogScanType.MIN_DELTA;
            } else if ("DETAIL".equals(info)) {
                scanType = TBinlogScanType.DETAIL;
            } else {
                throw new ParseException("Unsupported increment type in incr query: " + info);
            }
        }
        return scanType;
    }

    @Override
    public Pair<PlanNode, LocalExchangeType> enforceAndDeriveLocalExchange(
            PlanTranslatorContext translatorContext, PlanNode parent,
            LocalExchangeTypeRequire parentRequire) {
        boolean useSerialSource = fragment != null
                && fragment.useSerialSource(translatorContext.getConnectContext());
        if (useSerialSource) {
            return Pair.of(this, LocalExchangeType.NOOP);
        }
        // Non-pooling OlapScan has bucket distribution — each instance scans specific buckets
        return Pair.of(this, LocalExchangeType.BUCKET_HASH_SHUFFLE);
    }
}
