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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNodeInfo;
import org.apache.doris.thrift.TOlapTableIndexSchema;
import org.apache.doris.thrift.TOlapTableIndexTablets;
import org.apache.doris.thrift.TOlapTableLocationParam;
import org.apache.doris.thrift.TOlapTablePartition;
import org.apache.doris.thrift.TOlapTablePartitionParam;
import org.apache.doris.thrift.TOlapTableSchemaParam;
import org.apache.doris.thrift.TOlapTableSink;
import org.apache.doris.thrift.TPaloNodesInfo;
import org.apache.doris.thrift.TTabletLocation;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OlapTableSink extends DataSink {
    private static final Logger LOG = LogManager.getLogger(OlapTableSink.class);

    // input variables
    private OlapTable dstTable;
    private TupleDescriptor tupleDescriptor;
    private String partitions;
    private Set<String> partitionSet;

    // set after init called
    private TDataSink tDataSink;

    public OlapTableSink(OlapTable dstTable, TupleDescriptor tupleDescriptor) {
        this.dstTable = dstTable;
        this.tupleDescriptor = tupleDescriptor;
    }

    public OlapTableSink(OlapTable dstTable, TupleDescriptor tupleDescriptor, String partitions) {
        this.dstTable = dstTable;
        this.tupleDescriptor = tupleDescriptor;
        this.partitions = partitions;
    }

    public void init(TUniqueId loadId, long txnId, long dbId) throws AnalysisException {
        TOlapTableSink tSink = new TOlapTableSink();
        tSink.setLoad_id(loadId);
        tSink.setTxn_id(txnId);
        tSink.setDb_id(dbId);
        tDataSink = new TDataSink(TDataSinkType.DATA_SPLIT_SINK);
        tDataSink.setType(TDataSinkType.OLAP_TABLE_SINK);
        tDataSink.setOlap_table_sink(tSink);

        // check partition
        if (partitions != null) {
            if (dstTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_PARTITION_CLAUSE_NO_ALLOWED);
            }
            partitionSet = Sets.newHashSet();
            String[] partNames = partitions.trim().split("\\s*,\\s*");
            for (String partName : partNames) {
                Partition part = dstTable.getPartition(partName);
                if (part == null) {
                    ErrorReport.reportAnalysisException(
                            ErrorCode.ERR_UNKNOWN_PARTITION, partName, dstTable.getName());
                }
                partitionSet.add(partName);
            }
            if (partitionSet.isEmpty()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_PARTITION_CLAUSE_ON_NONPARTITIONED);
            }
        }
    }

    // must called after tupleDescriptor is computed
    public void finalize() throws UserException {
        TOlapTableSink tSink = tDataSink.getOlap_table_sink();

        tSink.setTable_id(dstTable.getId());
        tSink.setTuple_id(tupleDescriptor.getId().asInt());
        int numReplicas = 1;
        for (Partition partition : dstTable.getPartitions()) {
            numReplicas = dstTable.getPartitionInfo().getReplicationNum(partition.getId());
            break;
        }
        tSink.setNum_replicas(numReplicas);
        tSink.setNeed_gen_rollup(dstTable.shouldLoadToNewRollup());
        tSink.setSchema(createSchema(tSink.getDb_id(), dstTable));
        tSink.setPartition(createPartition(tSink.getDb_id(), dstTable));
        tSink.setLocation(createLocation(dstTable));
        tSink.setNodes_info(createPaloNodesInfo());
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "OLAP TABLE SINK\n");
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
        schemaParam.setDb_id(dbId);
        schemaParam.setTable_id(table.getId());
        schemaParam.setVersion(0);

        schemaParam.tuple_desc = tupleDescriptor.toThrift();
        for (SlotDescriptor slotDesc : tupleDescriptor.getSlots()) {
            schemaParam.addToSlot_descs(slotDesc.toThrift());
        }

        for (Map.Entry<Long, List<Column>> pair : table.getIndexIdToSchema().entrySet()) {
            List<String> columns = Lists.newArrayList();
            columns.addAll(pair.getValue().stream().map(Column::getName).collect(Collectors.toList()));
            schemaParam.addToIndexes(new TOlapTableIndexSchema(pair.getKey(), columns,
                    table.getSchemaHashByIndexId(pair.getKey())));
        }
        return schemaParam;
    }

    private List<String> getDistColumns(DistributionInfo distInfo, OlapTable table) throws UserException {
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
                for (Column column : table.getBaseSchema()) {
                    distColumns.add(column.getName());
                }
                break;
            }
            default:
                throw new UserException("unsupported distributed type, type=" + distInfo.getType());
        }
        return distColumns;
    }

    private TOlapTablePartitionParam createPartition(long dbId, OlapTable table) throws UserException {
        TOlapTablePartitionParam partitionParam = new TOlapTablePartitionParam();
        partitionParam.setDb_id(dbId);
        partitionParam.setTable_id(table.getId());
        partitionParam.setVersion(0);

        PartitionType partType = table.getPartitionInfo().getType();
        switch (partType) {
            case RANGE: {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
                // range partition's only has one column
                Preconditions.checkArgument(rangePartitionInfo.getPartitionColumns().size() == 1,
                        "number columns of range partition is not 1, number_columns="
                                + rangePartitionInfo.getPartitionColumns().size());
                partitionParam.setPartition_column(rangePartitionInfo.getPartitionColumns().get(0).getName());

                DistributionInfo selectedDistInfo = null;
                for (Partition partition : table.getPartitions()) {
                    if (partitionSet != null && !partitionSet.contains(partition.getName())) {
                        continue;
                    }
                    TOlapTablePartition tPartition = new TOlapTablePartition();
                    tPartition.setId(partition.getId());
                    Range<PartitionKey> range = rangePartitionInfo.getRange(partition.getId());
                    if (range.hasLowerBound()) {
                        tPartition.setStart_key(range.lowerEndpoint().getKeys().get(0).treeToThrift().getNodes().get(0));
                    }
                    if (range.hasUpperBound() && !range.upperEndpoint().isMaxValue()) {
                        tPartition.setEnd_key(range.upperEndpoint().getKeys().get(0).treeToThrift().getNodes().get(0));
                    }
                    for (MaterializedIndex index : partition.getMaterializedIndices()) {
                        tPartition.addToIndexes(new TOlapTableIndexTablets(index.getId(), Lists.newArrayList(
                                index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()))));
                        tPartition.setNum_buckets(index.getTablets().size());
                    }
                    partitionParam.addToPartitions(tPartition);

                    DistributionInfo distInfo = partition.getDistributionInfo();
                    if (selectedDistInfo == null) {
                        partitionParam.setDistributed_columns(getDistColumns(distInfo, table));
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
                for (MaterializedIndex index : partition.getMaterializedIndices()) {
                    tPartition.addToIndexes(new TOlapTableIndexTablets(index.getId(), Lists.newArrayList(
                            index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()))));
                    tPartition.setNum_buckets(index.getTablets().size());
                }
                partitionParam.addToPartitions(tPartition);
                partitionParam.setDistributed_columns(
                        getDistColumns(partition.getDistributionInfo(), table));
                break;
            }
            default: {
                throw new UserException("unsupported partition for OlapTable, partition=" + partType);
            }
        }
        return partitionParam;
    }

    private TOlapTableLocationParam createLocation(OlapTable table) throws UserException {
        TOlapTableLocationParam locationParam = new TOlapTableLocationParam();
        for (Partition partition : table.getPartitions()) {
            int quorum = table.getPartitionInfo().getReplicationNum(partition.getId()) / 2 + 1;            
            for (MaterializedIndex index : partition.getMaterializedIndices()) {
                //we should ensure the replica backend is available
                //otherwise, there will be a 'unknown node id, id=xxx' error for stream load
                for (Tablet tablet : index.getTablets()) {
                    List<Long> beIds = tablet.getAvailableBackendIdsList();
                    if (beIds.size() < quorum) {
                        throw new UserException("tablet " + tablet.getId() + " has few replicas: " + beIds.size());
                    }
                    locationParam.addToTablets(
                            new TTabletLocation(tablet.getId(), Lists.newArrayList(tablet.getAvailableBackendIds())));
                }
            }
        }
        return locationParam;
    }

    private TPaloNodesInfo createPaloNodesInfo() {
        TPaloNodesInfo nodesInfo = new TPaloNodesInfo();
        SystemInfoService systemInfoService = Catalog.getCurrentSystemInfo();
        for (Long id : systemInfoService.getBackendIds(false)) {
            Backend backend = systemInfoService.getBackend(id);
            nodesInfo.addToNodes(new TNodeInfo(backend.getId(), 0, backend.getHost(), backend.getBrpcPort()));
        }
        return nodesInfo;
    }

}
