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

import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TableSample;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.normalize.Normalizer;
import org.apache.doris.planner.normalize.PartitionRangePredicateNormalizer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TNormalizedOlapScanNode;
import org.apache.doris.thrift.TNormalizedPlanNode;
import org.apache.doris.thrift.TOlapScanNode;
import org.apache.doris.thrift.TOlapTableIndex;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TSortInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
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
    private boolean forceOpenPreAgg = false;
    private OlapTable olapTable = null;
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

    // For point query
    private Map<SlotRef, Expr> pointQueryEqualPredicats;
    private DescriptorTable descTable;

    private Set<Integer> distributionColumnIds;

    private boolean shouldColoScan = false;

    protected List<Expr> rewrittenProjectList;

    private long maxVersion = -1L;

    private SortInfo scoreSortInfo = null;
    private long scoreSortLimit = -1;

    // cached for prepared statement to quickly prune partition
    // only used in short circuit plan at present
    private final PartitionPruneV2ForShortCircuitPlan cachedPartitionPruner =
                        new PartitionPruneV2ForShortCircuitPlan();

    private boolean isTopnLazyMaterialize = false;
    private List<Column> topnLazyMaterializeOutputColumns = new ArrayList<>();

    private Column globalRowIdColumn;

    // Constructs node to scan given data files of table 'tbl'.
    public OlapScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName, StatisticalType.OLAP_SCAN_NODE);
        olapTable = (OlapTable) desc.getTable();
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

    public void setSampleTabletIds(List<Long> sampleTablets) {
        if (sampleTablets != null) {
            this.sampleTabletIds.addAll(sampleTablets);
        }
    }

    public void setTableSample(TableSample tSample) {
        this.tableSample = tSample;
    }

    public Set<Long> getNereidsPrunedTabletIds() {
        return nereidsPrunedTabletIds;
    }

    public void setNereidsPrunedTabletIds(Set<Long> nereidsPrunedTabletIds) {
        this.nereidsPrunedTabletIds = nereidsPrunedTabletIds;
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

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.addValue("olapTable=" + olapTable.getName());
        return helper.toString();
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


    private Collection<Long> partitionPrune(PartitionInfo partitionInfo,
            PartitionNames partitionNames) throws AnalysisException {
        PartitionPruner partitionPruner = null;
        Map<Long, PartitionItem> keyItemMap;
        if (partitionNames != null) {
            keyItemMap = Maps.newHashMap();
            for (String partName : partitionNames.getPartitionNames()) {
                Partition partition = olapTable.getPartition(partName, partitionNames.isTemp());
                if (partition == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_SUCH_PARTITION, partName);
                }
                keyItemMap.put(partition.getId(), partitionInfo.getItem(partition.getId()));
            }
        } else {
            keyItemMap = partitionInfo.getIdToItem(false);
        }
        if (partitionInfo.getType() == PartitionType.RANGE) {
            if (isPointQuery() && partitionInfo.getPartitionColumns().size() == 1) {
                // short circuit, a quick path to find partition
                ColumnRange filterRange = columnNameToRange.get(partitionInfo.getPartitionColumns().get(0).getName());
                LiteralExpr lowerBound = filterRange.getRangeSet().get().asRanges().stream()
                        .findFirst().get().lowerEndpoint().getValue();
                LiteralExpr upperBound = filterRange.getRangeSet().get().asRanges().stream()
                        .findFirst().get().upperEndpoint().getValue();
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
            List<Long> tabletIdsInOrder,
            DistributionInfo distributionInfo,
            boolean pruneTablesByNereids) throws AnalysisException {
        if (pruneTablesByNereids) {
            return nereidsPrunedTabletIds.isEmpty()
                    ? null
                    : new ArrayList<>(nereidsPrunedTabletIds);
        }
        DistributionPruner distributionPruner = null;
        switch (distributionInfo.getType()) {
            case HASH: {
                HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
                distributionPruner = new HashDistributionPruner(tabletIdsInOrder,
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
            final MaterializedIndex selectedTable = partition.getIndex(selectedIndexId);
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

    // for non-cloud mode. for cloud mode see `updateScanRangeVersions`
    private void addScanRangeLocations(Partition partition,
            List<Tablet> tablets, Map<Long, Set<Long>> backendAlivePathHashs) throws UserException {
        long visibleVersion = Partition.PARTITION_INIT_VERSION;

        // For cloud mode, set scan range visible version in Coordinator.exec so that we could
        // assign a snapshot version of all partitions.
        if (!(Config.isCloudMode() && Config.enable_cloud_snapshot_version)) {
            visibleVersion = partition.getVisibleVersion();
        }
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

        ImmutableMap<Long, Backend> allBackends = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        long partitionVisibleVersion = visibleVersion;
        String partitionVisibleVersionStr = fastToString(visibleVersion);
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
                    long beId = replica.getBackendId();
                    Backend backend = allBackends.get(replica.getBackendId());
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
                    backendId = replica.getBackendId();
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

            Integer bucketSeq = tabletId2BucketSeq.get(tabletId);
            bucketSeq2locations.put(bucketSeq, locations);
            bucketSeq2Bytes.merge(bucketSeq, oneReplicaBytes, Long::sum);
            scanRangeLocations.add(locations);
        }

        if (tablets.isEmpty()) {
            desc.setCardinality(0);
        } else {
            desc.setCardinality(cardinality);
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

    private void computePartitionInfo() throws AnalysisException {
        long start = System.currentTimeMillis();
        // Step1: compute partition ids
        PartitionNames partitionNames = ((BaseTableRef) desc.getRef()).getPartitionNames();
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo.getType() == PartitionType.RANGE || partitionInfo.getType() == PartitionType.LIST) {
            selectedPartitionIds = partitionPrune(partitionInfo, partitionNames);
        } else {
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
            desc.setCardinality(0);
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
            final MaterializedIndex selectedIndex = partition.getIndex(selectedIndexId);
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
            final MaterializedIndex selectedTable = partition.getIndex(selectedIndexId);
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
        for (Backend backend : Env.getCurrentSystemInfo().getAllClusterBackendsNoException().values()) {
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
            final MaterializedIndex selectedTable = partition.getIndex(selectedIndexId);
            final List<Tablet> tablets = Lists.newArrayList();
            List<Long> allTabletIds = selectedTable.getTabletIdsInOrder();
            // point query need prune tablets at this place
            Collection<Long> prunedTabletIds = distributionPrune(
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

            boolean notExistsSampleAndPrunedTablets = sampleTabletIds.isEmpty() && nereidsPrunedTabletIds.isEmpty();
            if (prunedTabletIds != null) {
                for (Long id : prunedTabletIds) {
                    if (selectedTable.getTablet(id) != null) {
                        tablets.add(selectedTable.getTablet(id));
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

            for (int i = 0; i < allTabletIds.size(); i++) {
                tabletId2BucketSeq.put(allTabletIds.get(i), i);
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
        bucketSeq2locations.clear();
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
            sortInfo.getMaterializedOrderingExprs().forEach(expr -> {
                output.append(prefix).append(prefix).append(expr.toSql()).append("\n");
            });
        }
        if (sortLimit != -1) {
            output.append(prefix).append("SORT LIMIT: ").append(sortLimit).append("\n");
        }
        if (scoreSortInfo != null) {
            output.append(prefix).append("SCORE SORT INFO:\n");
            scoreSortInfo.getOrderingExprs().forEach(expr -> {
                output.append(prefix).append(prefix).append(expr.toSql()).append("\n");
            });
        }
        if (scoreSortLimit != -1) {
            output.append(prefix).append("SCORE SORT LIMIT: ").append(scoreSortLimit).append("\n");
        }
        if (useTopnFilter()) {
            String topnFilterSources = String.join(",",
                    topnFilterSortNodes.stream()
                            .map(node -> node.getId().asInt() + "").collect(Collectors.toList()));
            output.append(prefix).append("TOPN OPT:").append(topnFilterSources).append("\n");
        }

        if (!conjuncts.isEmpty()) {
            Expr expr = convertConjunctsToAndCompoundPredicate(conjuncts);
            output.append(prefix).append("PREDICATES: ").append(expr.toSql()).append("\n");
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

        if (!CollectionUtils.isEmpty(rewrittenProjectList)) {
            output.append(prefix).append("rewrittenProjectList: ").append(
                    getExplainString(rewrittenProjectList)).append("\n");
        }
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        // In pipeline exec engine, the instance num equals be_num * parallel instance.
        // so here we need count distinct be_num to do the work. make sure get right instance
        if (ConnectContext.get().getSessionVariable().isIgnoreStorageDataDistribution()) {
            return ConnectContext.get().getSessionVariable().getParallelExecInstanceNum();
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
            TColumn tColumn = globalRowIdColumn.toThrift();
            tColumn.setColumnType(ScalarType.createStringType().toColumnTypeThrift());
            tColumn.setAggregationType(AggregateType.REPLACE.toThrift());
            tColumn.setIsKey(false);
            tColumn.setIsAllowNull(false);
            // keep compatibility
            tColumn.setVisible(false);
            tColumn.setColUniqueId(Integer.MAX_VALUE);
            columnsDesc.add(tColumn);
        } else {
            olapTable.getColumnDesc(selectedIndexId, columnsDesc, keyColumnNames, keyColumnTypes);

            // Add extra row id column
            ArrayList<SlotDescriptor> slots = desc.getSlots();
            Column lastColumn = slots.get(slots.size() - 1).getColumn();
            if (lastColumn != null && lastColumn.getName().equalsIgnoreCase(Column.ROWID_COL)) {
                TColumn tColumn = new TColumn();
                tColumn.setColumnName(Column.ROWID_COL);
                tColumn.setColumnType(ScalarType.createStringType().toColumnTypeThrift());
                tColumn.setAggregationType(AggregateType.REPLACE.toThrift());
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
                TColumn tColumn = slot.getColumn().toThrift();
                columnsDesc.add(tColumn);
            }
        }

        for (Index index : olapTable.getIndexes()) {
            TOlapTableIndex tIndex = index.toThrift(index.getColumnUniqueIds(olapTable.getBaseSchema()));
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
        if (selectedIndexId != -1) {
            msg.olap_scan_node.setSchemaVersion(olapTable.getIndexSchemaVersion(selectedIndexId));
        }
        if (null != sortColumn) {
            msg.olap_scan_node.setSortColumn(sortColumn);
        }
        if (sortInfo != null) {
            TSortInfo tSortInfo = new TSortInfo(
                    Expr.treesToThrift(sortInfo.getOrderingExprs()),
                    sortInfo.getIsAscOrder(),
                    sortInfo.getNullsFirst());
            if (sortInfo.getSortTupleSlotExprs() != null) {
                tSortInfo.setSortTupleSlotExprs(Expr.treesToThrift(sortInfo.getSortTupleSlotExprs()));
            }
            msg.olap_scan_node.setSortInfo(tSortInfo);
        }
        if (sortLimit != -1) {
            msg.olap_scan_node.setSortLimit(sortLimit);
        }
        if (scoreSortInfo != null) {
            TSortInfo tScoreSortInfo = new TSortInfo(
                    Expr.treesToThrift(scoreSortInfo.getOrderingExprs()),
                    scoreSortInfo.getIsAscOrder(),
                    scoreSortInfo.getNullsFirst());
            if (scoreSortInfo.getSortTupleSlotExprs() != null) {
                tScoreSortInfo.setSortTupleSlotExprs(Expr.treesToThrift(scoreSortInfo.getSortTupleSlotExprs()));
            }
            msg.olap_scan_node.setScoreSortInfo(tScoreSortInfo);
        }
        if (scoreSortLimit != -1) {
            msg.olap_scan_node.setScoreSortLimit(scoreSortLimit);
        }
        msg.olap_scan_node.setKeyType(olapTable.getKeysType().toThrift());
        String tableName = olapTable.getName();
        if (selectedIndexId != -1) {
            tableName = tableName + "(" + getSelectedIndexName() + ")";
        }
        msg.olap_scan_node.setTableName(tableName);
        msg.olap_scan_node.setEnableUniqueKeyMergeOnWrite(olapTable.getEnableUniqueKeyMergeOnWrite());

        msg.setPushDownAggTypeOpt(pushDownAggNoGroupingOp);

        msg.olap_scan_node.setPushDownAggTypeOpt(pushDownAggNoGroupingOp);
        // In TOlapScanNode , pushDownAggNoGroupingOp field is deprecated.

        if (outputColumnUniqueIds != null) {
            msg.olap_scan_node.setOutputColumnUniqueIds(outputColumnUniqueIds);
        }

        msg.olap_scan_node.setDistributeColumnIds(new ArrayList<>(distributionColumnIds));

        super.toThrift(msg);
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
                .map(slot -> Pair.of(slot.getId(), slot.getColumn().getName()))
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
    public String getReasonOfPreAggregation() {
        return reasonOfPreAggregation;
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
        for (SlotDescriptor virtualSlot : context.getTupleDesc(this.getTupleId()).getSlots()) {
            Expr virtualColumn = virtualSlot.getVirtualColumn();
            if (virtualColumn == null) {
                continue;
            }
            Set<Expr> slotRefs = Sets.newHashSet();
            virtualColumn.collect(e -> e instanceof SlotRef, slotRefs);
            Set<SlotId> virtualColumnInputSlotIds = slotRefs.stream()
                    .filter(s -> s instanceof SlotRef)
                    .map(s -> (SlotRef) s)
                    .map(SlotRef::getSlotId)
                    .collect(Collectors.toSet());
            for (SlotDescriptor slot : context.getTupleDesc(this.getTupleId()).getSlots()) {
                if (virtualColumnInputSlotIds.contains(slot.getId()) && slot.getColumn() != null) {
                    outputColumnUniqueIds.add(slot.getColumn().getUniqueId());
                }
            }
        }
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
}
