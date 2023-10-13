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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugPointUtil.DebugPoint;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TNodeInfo;
import org.apache.doris.thrift.TOlapTableIndex;
import org.apache.doris.thrift.TOlapTableIndexSchema;
import org.apache.doris.thrift.TOlapTableIndexTablets;
import org.apache.doris.thrift.TOlapTableLocationParam;
import org.apache.doris.thrift.TOlapTablePartition;
import org.apache.doris.thrift.TOlapTablePartitionParam;
import org.apache.doris.thrift.TOlapTableSchemaParam;
import org.apache.doris.thrift.TOlapTableSink;
import org.apache.doris.thrift.TPaloNodesInfo;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TTabletLocation;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

public class OlapTableSink extends DataSink {
    private static final Logger LOG = LogManager.getLogger(OlapTableSink.class);

    // input variables
    private OlapTable dstTable;
    private TupleDescriptor tupleDescriptor;
    // specified partition ids.
    private List<Long> partitionIds;
    // partial update input columns
    private boolean isPartialUpdate = false;
    private HashSet<String> partialUpdateInputColumns;

    // set after init called
    private TDataSink tDataSink;

    private boolean singleReplicaLoad;

    private boolean isStrictMode = false;

    public OlapTableSink(OlapTable dstTable, TupleDescriptor tupleDescriptor, List<Long> partitionIds,
            boolean singleReplicaLoad) {
        this.dstTable = dstTable;
        this.tupleDescriptor = tupleDescriptor;
        this.partitionIds = partitionIds;
        this.singleReplicaLoad = singleReplicaLoad;
    }

    public void init(TUniqueId loadId, long txnId, long dbId, long loadChannelTimeoutS, int sendBatchParallelism,
            boolean loadToSingleTablet, boolean isStrictMode) throws AnalysisException {
        TOlapTableSink tSink = new TOlapTableSink();
        tSink.setLoadId(loadId);
        tSink.setTxnId(txnId);
        tSink.setDbId(dbId);
        tSink.setLoadChannelTimeoutS(loadChannelTimeoutS);
        tSink.setSendBatchParallelism(sendBatchParallelism);
        this.isStrictMode = isStrictMode;
        if (loadToSingleTablet && !(dstTable.getDefaultDistributionInfo() instanceof RandomDistributionInfo)) {
            throw new AnalysisException(
                    "if load_to_single_tablet set to true," + " the olap table must be with random distribution");
        }
        tSink.setLoadToSingleTablet(loadToSingleTablet);
        tDataSink = new TDataSink(getDataSinkType());
        tDataSink.setOlapTableSink(tSink);

        if (partitionIds == null) {
            partitionIds = dstTable.getPartitionIds();
            if (partitionIds.isEmpty() && dstTable.getPartitionInfo().enableAutomaticPartition() == false) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_EMPTY_PARTITION_IN_TABLE, dstTable.getName());
            }
        }
        for (Long partitionId : partitionIds) {
            Partition part = dstTable.getPartition(partitionId);
            if (part == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_PARTITION, partitionId, dstTable.getName());
            }
        }

        if (singleReplicaLoad && dstTable.getStorageFormat() == TStorageFormat.V1) {
            // Single replica load not supported by TStorageFormat.V1
            singleReplicaLoad = false;
            LOG.warn("Single replica load not supported by TStorageFormat.V1. table: {}", dstTable.getName());
        }
        if (dstTable.getEnableUniqueKeyMergeOnWrite()) {
            singleReplicaLoad = false;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Single replica load not supported by merge-on-write table: {}", dstTable.getName());
            }
        }
    }

    public void setPartialUpdateInputColumns(boolean isPartialUpdate, HashSet<String> columns) {
        this.isPartialUpdate = isPartialUpdate;
        this.partialUpdateInputColumns = columns;
    }

    public void updateLoadId(TUniqueId newLoadId) {
        tDataSink.getOlapTableSink().setLoadId(newLoadId);
    }

    // must called after tupleDescriptor is computed
    public void complete(Analyzer analyzer) throws UserException {
        TOlapTableSink tSink = tDataSink.getOlapTableSink();

        tSink.setTableId(dstTable.getId());
        tSink.setTupleId(tupleDescriptor.getId().asInt());
        int numReplicas = 1;
        for (Partition partition : dstTable.getPartitions()) {
            numReplicas = dstTable.getPartitionInfo().getReplicaAllocation(partition.getId()).getTotalReplicaNum();
            break;
        }
        tSink.setNumReplicas(numReplicas);
        tSink.setNeedGenRollup(dstTable.shouldLoadToNewRollup());
        tSink.setSchema(createSchema(tSink.getDbId(), dstTable, analyzer));
        tSink.setPartition(createPartition(tSink.getDbId(), dstTable, analyzer));
        List<TOlapTableLocationParam> locationParams = createLocation(dstTable);
        tSink.setLocation(locationParams.get(0));
        if (singleReplicaLoad) {
            tSink.setSlaveLocation(locationParams.get(1));
        }
        tSink.setWriteSingleReplica(singleReplicaLoad);
        tSink.setNodesInfo(createPaloNodesInfo());
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "OLAP TABLE SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return strBuilder.toString();
        }
        strBuilder.append(prefix + "  TUPLE ID: " + tupleDescriptor.getId() + "\n");
        strBuilder.append(prefix + "  " + DataPartition.RANDOM.getExplainString(explainLevel));
        return strBuilder.toString();
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return DataPartition.RANDOM;
    }

    @Override
    protected TDataSink toThrift() {
        return tDataSink;
    }

    private TOlapTableSchemaParam createSchema(long dbId, OlapTable table, Analyzer analyzer) throws AnalysisException {
        TOlapTableSchemaParam schemaParam = new TOlapTableSchemaParam();
        schemaParam.setDbId(dbId);
        schemaParam.setTableId(table.getId());
        schemaParam.setVersion(table.getIndexMetaByIndexId(table.getBaseIndexId()).getSchemaVersion());
        schemaParam.setIsStrictMode(isStrictMode);

        schemaParam.tuple_desc = tupleDescriptor.toThrift();
        for (SlotDescriptor slotDesc : tupleDescriptor.getSlots()) {
            schemaParam.addToSlotDescs(slotDesc.toThrift());
        }

        for (Map.Entry<Long, MaterializedIndexMeta> pair : table.getIndexIdToMeta().entrySet()) {
            MaterializedIndexMeta indexMeta = pair.getValue();
            List<String> columns = Lists.newArrayList();
            List<TColumn> columnsDesc = Lists.newArrayList();
            List<TOlapTableIndex> indexDesc = Lists.newArrayList();
            columns.addAll(indexMeta.getSchema().stream().map(Column::getNonShadowName).collect(Collectors.toList()));
            for (Column column : indexMeta.getSchema()) {
                TColumn tColumn = column.toThrift();
                column.setIndexFlag(tColumn, table);
                columnsDesc.add(tColumn);
            }
            List<Index> indexes = indexMeta.getIndexes();
            if (indexes.size() == 0 && pair.getKey() == table.getBaseIndexId()) {
                // for compatible with old version befor 2.0-beta
                // if indexMeta.getIndexes() is empty, use table.getIndexes()
                indexes = table.getIndexes();
            }
            for (Index index : indexes) {
                TOlapTableIndex tIndex = index.toThrift();
                indexDesc.add(tIndex);
            }
            TOlapTableIndexSchema indexSchema = new TOlapTableIndexSchema(pair.getKey(), columns,
                    indexMeta.getSchemaHash());
            if (indexMeta.getWhereClause() != null) {
                Expr expr = indexMeta.getWhereClause().clone();
                expr.replaceSlot(tupleDescriptor);
                if (analyzer != null) {
                    tupleDescriptor.setTable(table);
                    analyzer.registerTupleDescriptor(tupleDescriptor);
                    expr.analyze(analyzer);
                }
                indexSchema.setWhereClause(expr.treeToThrift());
            }
            indexSchema.setColumnsDesc(columnsDesc);
            indexSchema.setIndexesDesc(indexDesc);
            schemaParam.addToIndexes(indexSchema);
        }
        schemaParam.setIsPartialUpdate(isPartialUpdate);
        if (isPartialUpdate) {
            for (String s : partialUpdateInputColumns) {
                schemaParam.addToPartialUpdateInputColumns(s);
            }
        }
        return schemaParam;
    }

    private List<String> getDistColumns(DistributionInfo distInfo) throws UserException {
        List<String> distColumns = Lists.newArrayList();
        switch (distInfo.getType()) {
            case HASH: {
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distInfo;
                for (Column column : hashDistributionInfo.getDistributionColumns()) {
                    distColumns.add(column.getName());
                }
                break;
            }
            case RANDOM: {
                // RandomDistributionInfo doesn't have distributedColumns
                break;
            }
            default:
                throw new UserException("unsupported distributed type, type=" + distInfo.getType());
        }
        return distColumns;
    }

    private TOlapTablePartitionParam createPartition(long dbId, OlapTable table, Analyzer analyzer)
            throws UserException {
        TOlapTablePartitionParam partitionParam = new TOlapTablePartitionParam();
        partitionParam.setDbId(dbId);
        partitionParam.setTableId(table.getId());
        partitionParam.setVersion(0);

        PartitionType partType = table.getPartitionInfo().getType();
        switch (partType) {
            case LIST:
            case RANGE: {
                PartitionInfo partitionInfo = table.getPartitionInfo();
                for (Column partCol : partitionInfo.getPartitionColumns()) {
                    partitionParam.addToPartitionColumns(partCol.getName());
                }

                int partColNum = partitionInfo.getPartitionColumns().size();
                DistributionInfo selectedDistInfo = null;

                for (Long partitionId : partitionIds) {
                    Partition partition = table.getPartition(partitionId);
                    TOlapTablePartition tPartition = new TOlapTablePartition();
                    tPartition.setId(partition.getId());
                    // set partition keys
                    setPartitionKeys(tPartition, partitionInfo.getItem(partition.getId()), partColNum);

                    for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                        tPartition.addToIndexes(new TOlapTableIndexTablets(index.getId(), Lists.newArrayList(
                                index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()))));
                        tPartition.setNumBuckets(index.getTablets().size());
                    }
                    tPartition.setIsMutable(table.getPartitionInfo().getIsMutable(partitionId));
                    partitionParam.addToPartitions(tPartition);

                    DistributionInfo distInfo = partition.getDistributionInfo();
                    if (selectedDistInfo == null) {
                        partitionParam.setDistributedColumns(getDistColumns(distInfo));
                        selectedDistInfo = distInfo;
                    } else {
                        if (selectedDistInfo.getType() != distInfo.getType()) {
                            throw new UserException("different distribute types in two different partitions, type1="
                                    + selectedDistInfo.getType() + ", type2=" + distInfo.getType());
                        }
                    }
                }
                boolean  enableAutomaticPartition = partitionInfo.enableAutomaticPartition();
                // for auto create partition by function expr, there is no any partition firstly,
                // But this is required in thrift struct.
                if (enableAutomaticPartition && partitionIds.isEmpty()) {
                    partitionParam.setDistributedColumns(getDistColumns(table.getDefaultDistributionInfo()));
                    partitionParam.setPartitions(new ArrayList<TOlapTablePartition>());
                }
                ArrayList<Expr> exprs = partitionInfo.getPartitionExprs();
                if (enableAutomaticPartition && exprs != null && analyzer != null) {
                    Analyzer funcAnalyzer = new Analyzer(analyzer.getEnv(), analyzer.getContext());
                    tupleDescriptor.setTable(table);
                    funcAnalyzer.registerTupleDescriptor(tupleDescriptor);
                    for (Expr e : exprs) {
                        e.analyze(funcAnalyzer);
                    }
                    partitionParam.setPartitionFunctionExprs(Expr.treesToThrift(exprs));
                }
                partitionParam.setEnableAutomaticPartition(enableAutomaticPartition);
                break;
            }
            case UNPARTITIONED: {
                // there is no partition columns for single partition
                Preconditions.checkArgument(table.getPartitions().size() == 1,
                        "Number of table partitions is not 1 for unpartitioned table, partitionNum="
                                + table.getPartitions().size());
                Partition partition;
                if (partitionIds != null && partitionIds.size() == 1) {
                    partition = table.getPartition(partitionIds.get(0));
                } else {
                    partition = table.getPartitions().iterator().next();
                }

                TOlapTablePartition tPartition = new TOlapTablePartition();
                tPartition.setId(partition.getId());
                tPartition.setIsMutable(table.getPartitionInfo().getIsMutable(partition.getId()));
                // No lowerBound and upperBound for this range
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    tPartition.addToIndexes(new TOlapTableIndexTablets(index.getId(), Lists.newArrayList(
                            index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()))));
                    tPartition.setNumBuckets(index.getTablets().size());
                }
                partitionParam.addToPartitions(tPartition);
                partitionParam.setDistributedColumns(getDistColumns(partition.getDistributionInfo()));
                partitionParam.setEnableAutomaticPartition(false);
                break;
            }
            default: {
                throw new UserException("unsupported partition for OlapTable, partition=" + partType);
            }
        }
        partitionParam.setPartitionType(partType.toThrift());
        return partitionParam;
    }

    public static void setPartitionKeys(TOlapTablePartition tPartition, PartitionItem partitionItem, int partColNum) {
        if (partitionItem instanceof RangePartitionItem) {
            Range<PartitionKey> range = partitionItem.getItems();
            // set start keys
            if (range.hasLowerBound() && !range.lowerEndpoint().isMinValue()) {
                for (int i = 0; i < partColNum; i++) {
                    tPartition.addToStartKeys(range.lowerEndpoint().getKeys().get(i).treeToThrift().getNodes().get(0));
                }
            }
            // set end keys
            if (range.hasUpperBound() && !range.upperEndpoint().isMaxValue()) {
                for (int i = 0; i < partColNum; i++) {
                    tPartition.addToEndKeys(range.upperEndpoint().getKeys().get(i).treeToThrift().getNodes().get(0));
                }
            }
        } else if (partitionItem instanceof ListPartitionItem) {
            List<PartitionKey> partitionKeys = partitionItem.getItems();
            // set in keys
            for (PartitionKey partitionKey : partitionKeys) {
                List<TExprNode> tExprNodes = new ArrayList<>();
                for (int i = 0; i < partColNum; i++) {
                    tExprNodes.add(partitionKey.getKeys().get(i).treeToThrift().getNodes().get(0));
                }
                tPartition.addToInKeys(tExprNodes);
                tPartition.setIsDefaultPartition(partitionItem.isDefaultPartition());
            }
        }
    }

    private List<TOlapTableLocationParam> createLocation(OlapTable table) throws UserException {
        TOlapTableLocationParam locationParam = new TOlapTableLocationParam();
        TOlapTableLocationParam slaveLocationParam = new TOlapTableLocationParam();
        // BE id -> path hash
        Multimap<Long, Long> allBePathsMap = HashMultimap.create();
        for (Long partitionId : partitionIds) {
            Partition partition = table.getPartition(partitionId);
            int loadRequiredReplicaNum = table.getLoadRequiredReplicaNum(partition.getId());
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                // we should ensure the replica backend is alive
                // otherwise, there will be a 'unknown node id, id=xxx' error for stream load
                for (Tablet tablet : index.getTablets()) {
                    Multimap<Long, Long> bePathsMap = tablet.getNormalReplicaBackendPathMap();
                    if (bePathsMap.keySet().size() < loadRequiredReplicaNum) {
                        throw new UserException(InternalErrorCode.REPLICA_FEW_ERR,
                                "tablet " + tablet.getId() + " alive replica num " + bePathsMap.keySet().size()
                                        + " < load required replica num " + loadRequiredReplicaNum
                                        + ", alive backends: [" + StringUtils.join(bePathsMap.keySet(), ",") + "]");
                    }

                    debugWriteRandomChooseSink(tablet, partition.getVisibleVersion(), bePathsMap);
                    if (bePathsMap.keySet().isEmpty()) {
                        throw new UserException(InternalErrorCode.REPLICA_FEW_ERR,
                                "tablet " + tablet.getId() + " no available replica");
                    }

                    if (singleReplicaLoad) {
                        Long[] nodes = bePathsMap.keySet().toArray(new Long[0]);
                        Random random = new SecureRandom();
                        Long masterNode = nodes[random.nextInt(nodes.length)];
                        Multimap<Long, Long> slaveBePathsMap = bePathsMap;
                        slaveBePathsMap.removeAll(masterNode);
                        locationParam.addToTablets(new TTabletLocation(tablet.getId(),
                                Lists.newArrayList(Sets.newHashSet(masterNode))));
                        slaveLocationParam.addToTablets(new TTabletLocation(tablet.getId(),
                                Lists.newArrayList(slaveBePathsMap.keySet())));
                    } else {
                        locationParam.addToTablets(new TTabletLocation(tablet.getId(),
                                Lists.newArrayList(bePathsMap.keySet())));
                    }
                    allBePathsMap.putAll(bePathsMap);
                }
            }
        }

        // for partition by function expr, there is no any partition firstly, But this is required in thrift struct.
        if (partitionIds.isEmpty()) {
            locationParam.setTablets(new ArrayList<TTabletLocation>());
        }
        // check if disk capacity reach limit
        // this is for load process, so use high water mark to check
        Status st = Env.getCurrentSystemInfo().checkExceedDiskCapacityLimit(allBePathsMap, true);
        if (!st.ok()) {
            throw new DdlException(st.getErrorMsg());
        }
        return Arrays.asList(locationParam, slaveLocationParam);
    }

    private void debugWriteRandomChooseSink(Tablet tablet, long version, Multimap<Long, Long> bePathsMap) {
        DebugPoint debugPoint = DebugPointUtil.getDebugPoint("OlapTableSink.write_random_choose_sink");
        if (debugPoint == null) {
            return;
        }

        boolean needCatchup = debugPoint.param("needCatchUp", false);
        int sinkNum = debugPoint.param("sinkNum", 0);
        if (sinkNum == 0) {
            sinkNum = new SecureRandom().nextInt() % bePathsMap.size() + 1;
        }
        List<Long> candidatePaths = tablet.getReplicas().stream()
                .filter(replica -> !needCatchup || replica.getVersion() >= version)
                .map(Replica::getPathHash)
                .collect(Collectors.toList());
        if (sinkNum > 0 && sinkNum < candidatePaths.size()) {
            Collections.shuffle(candidatePaths);
            while (candidatePaths.size() > sinkNum) {
                candidatePaths.remove(candidatePaths.size() - 1);
            }
        }

        Multimap<Long, Long> result = HashMultimap.create();
        bePathsMap.forEach((tabletId, pathHash) -> {
            if (candidatePaths.contains(pathHash)) {
                result.put(tabletId, pathHash);
            }
        });

        bePathsMap.clear();
        bePathsMap.putAll(result);
    }

    private TPaloNodesInfo createPaloNodesInfo() {
        TPaloNodesInfo nodesInfo = new TPaloNodesInfo();
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        for (Long id : systemInfoService.getAllBackendIds(false)) {
            Backend backend = systemInfoService.getBackend(id);
            nodesInfo.addToNodes(new TNodeInfo(backend.getId(), 0, backend.getHost(), backend.getBrpcPort()));
        }
        return nodesInfo;
    }

    protected TDataSinkType getDataSinkType() {
        return TDataSinkType.OLAP_TABLE_SINK;
    }
}
