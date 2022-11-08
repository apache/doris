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

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
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
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TNodeInfo;
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
import org.apache.doris.transaction.DatabaseTransactionMgr;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
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

    // set after init called
    private TDataSink tDataSink;

    private boolean singleReplicaLoad;

    public OlapTableSink(OlapTable dstTable, TupleDescriptor tupleDescriptor, List<Long> partitionIds,
            boolean singleReplicaLoad) {
        this.dstTable = dstTable;
        this.tupleDescriptor = tupleDescriptor;
        this.partitionIds = partitionIds;
        this.singleReplicaLoad = singleReplicaLoad;
    }

    public void init(TUniqueId loadId, long txnId, long dbId, long loadChannelTimeoutS, int sendBatchParallelism,
            boolean loadToSingleTablet) throws AnalysisException {
        TOlapTableSink tSink = new TOlapTableSink();
        tSink.setLoadId(loadId);
        tSink.setTxnId(txnId);
        tSink.setDbId(dbId);
        tSink.setLoadChannelTimeoutS(loadChannelTimeoutS);
        tSink.setSendBatchParallelism(sendBatchParallelism);
        if (loadToSingleTablet && !(dstTable.getDefaultDistributionInfo() instanceof RandomDistributionInfo)) {
            throw new AnalysisException(
                    "if load_to_single_tablet set to true," + " the olap table must be with random distribution");
        }
        tSink.setLoadToSingleTablet(loadToSingleTablet);
        tDataSink = new TDataSink(TDataSinkType.OLAP_TABLE_SINK);
        tDataSink.setOlapTableSink(tSink);

        if (partitionIds == null) {
            partitionIds = dstTable.getPartitionIds();
            if (partitionIds.isEmpty()) {
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
    }

    public void updateLoadId(TUniqueId newLoadId) {
        tDataSink.getOlapTableSink().setLoadId(newLoadId);
    }

    // must called after tupleDescriptor is computed
    public void complete() throws UserException {
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
        tSink.setSchema(createSchema(tSink.getDbId(), dstTable));
        tSink.setPartition(createPartition(tSink.getDbId(), dstTable));
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

    private TOlapTableSchemaParam createSchema(long dbId, OlapTable table) {
        TOlapTableSchemaParam schemaParam = new TOlapTableSchemaParam();
        schemaParam.setDbId(dbId);
        schemaParam.setTableId(table.getId());
        schemaParam.setVersion(table.getIndexMetaByIndexId(table.getBaseIndexId()).getSchemaVersion());

        schemaParam.tuple_desc = tupleDescriptor.toThrift();
        for (SlotDescriptor slotDesc : tupleDescriptor.getSlots()) {
            schemaParam.addToSlotDescs(slotDesc.toThrift());
        }

        for (Map.Entry<Long, MaterializedIndexMeta> pair : table.getIndexIdToMeta().entrySet()) {
            MaterializedIndexMeta indexMeta = pair.getValue();
            List<String> columns = Lists.newArrayList();
            List<TColumn> columnsDesc = Lists.newArrayList();
            columns.addAll(indexMeta.getSchema().stream().map(Column::getName).collect(Collectors.toList()));
            for (Column column : indexMeta.getSchema()) {
                TColumn tColumn = column.toThrift();
                column.setIndexFlag(tColumn, table);
                columnsDesc.add(tColumn);
            }
            TOlapTableIndexSchema indexSchema = new TOlapTableIndexSchema(pair.getKey(), columns,
                    indexMeta.getSchemaHash());
            indexSchema.setColumnsDesc(columnsDesc);
            schemaParam.addToIndexes(indexSchema);
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

    private TOlapTablePartitionParam createPartition(long dbId, OlapTable table) throws UserException {
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
                break;
            }
            case UNPARTITIONED: {
                // there is no partition columns for single partition
                Preconditions.checkArgument(table.getPartitions().size() == 1,
                        "Number of table partitions is not 1 for unpartitioned table, partitionNum="
                                + table.getPartitions().size());
                Partition partition = table.getPartitions().iterator().next();

                TOlapTablePartition tPartition = new TOlapTablePartition();
                tPartition.setId(partition.getId());
                // No lowerBound and upperBound for this range
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    tPartition.addToIndexes(new TOlapTableIndexTablets(index.getId(), Lists.newArrayList(
                            index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()))));
                    tPartition.setNumBuckets(index.getTablets().size());
                }
                partitionParam.addToPartitions(tPartition);
                partitionParam.setDistributedColumns(getDistColumns(partition.getDistributionInfo()));
                break;
            }
            default: {
                throw new UserException("unsupported partition for OlapTable, partition=" + partType);
            }
        }
        return partitionParam;
    }

    private void setPartitionKeys(TOlapTablePartition tPartition, PartitionItem partitionItem, int partColNum) {
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
            }
        }
    }

    private List<TOlapTableLocationParam> createLocation(OlapTable table) throws UserException {
        TOlapTableLocationParam locationParam = new TOlapTableLocationParam();
        TOlapTableLocationParam slaveLocationParam = new TOlapTableLocationParam();
        // BE id -> path hash
        Multimap<Long, Long> allBePathsMap = HashMultimap.create();
        int replicaNum = 0;
        for (Long partitionId : partitionIds) {
            Partition partition = table.getPartition(partitionId);
            int quorum = table.getPartitionInfo().getReplicaAllocation(partition.getId()).getTotalReplicaNum() / 2 + 1;
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                // we should ensure the replica backend is alive
                // otherwise, there will be a 'unknown node id, id=xxx' error for stream load
                for (Tablet tablet : index.getTablets()) {
                    Multimap<Long, Long> bePathsMap = tablet.getNormalReplicaBackendPathMap();
                    if (bePathsMap.keySet().size() < quorum) {
                        throw new UserException(InternalErrorCode.REPLICA_FEW_ERR,
                                "tablet " + tablet.getId() + " has few replicas: " + bePathsMap.keySet().size()
                                        + ", alive backends: [" + StringUtils.join(bePathsMap.keySet(), ",") + "]");
                    }

                    if (singleReplicaLoad) {
                        Long[] nodes = bePathsMap.keySet().toArray(new Long[0]);
                        Random random = new Random();
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
                    replicaNum += bePathsMap.size();
                }
            }
        }

        // check if disk capacity reach limit
        // this is for load process, so use high water mark to check
        Status st = Env.getCurrentSystemInfo().checkExceedDiskCapacityLimit(allBePathsMap, true);
        if (!st.ok()) {
            throw new DdlException(st.getErrorMsg());
        }
        long dbId = tDataSink.getOlapTableSink().getDbId();
        long txnId = tDataSink.getOlapTableSink().getTxnId();
        try {
            DatabaseTransactionMgr mgr = Env.getCurrentGlobalTransactionMgr().getDatabaseTransactionMgr(dbId);
            mgr.registerTxnReplicas(txnId, replicaNum);
        } catch (Exception e) {
            LOG.error("register txn replica failed, txnId={}, dbId={}", txnId, dbId);
        }
        return Arrays.asList(locationParam, slaveLocationParam);
    }

    private TPaloNodesInfo createPaloNodesInfo() {
        TPaloNodesInfo nodesInfo = new TPaloNodesInfo();
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        for (Long id : systemInfoService.getBackendIds(false)) {
            Backend backend = systemInfoService.getBackend(id);
            nodesInfo.addToNodes(new TNodeInfo(backend.getId(), 0, backend.getHost(), backend.getBrpcPort()));
        }
        return nodesInfo;
    }

}
