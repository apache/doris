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

package org.apache.doris.common.proc;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.TimeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions
 * show partitions' detail info within a table
 */
public class PartitionsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("PartitionId").add("PartitionName").add("VisibleVersion").add("VisibleVersionHash")
            .add("State").add("PartitionKey").add("Range").add("DistributionKey")
            .add("Buckets").add("ReplicationNum").add("StorageMedium").add("CooldownTime")
            .add("LastConsistencyCheckTime")
            .build();

    public static final int PARTITION_NAME_INDEX = 1;

    private Database db;
    private OlapTable olapTable;

    public PartitionsProcDir(Database db, OlapTable olapTable) {
        this.db = db;
        this.olapTable = olapTable;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(olapTable);
        Preconditions.checkState(olapTable.getType() == TableType.OLAP);

        // get info
        List<List<Comparable>> partitionInfos = new ArrayList<List<Comparable>>();
        db.readLock();
        try {
            RangePartitionInfo rangePartitionInfo = null;
            Joiner joiner = Joiner.on(", ");
            if (olapTable.getPartitionInfo().getType() == PartitionType.RANGE) {
                rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
                List<Map.Entry<Long, Range<PartitionKey>>> sortedRange = rangePartitionInfo.getSortedRangeMap();
                for (Map.Entry<Long, Range<PartitionKey>> entry : sortedRange) {
                    long partitionId = entry.getKey();
                    Partition partition = olapTable.getPartition(partitionId);
                    List<Comparable> partitionInfo = new ArrayList<Comparable>();
                    String partitionName = partition.getName();
                    partitionInfo.add(partitionId);
                    partitionInfo.add(partitionName);
                    partitionInfo.add(partition.getVisibleVersion());
                    partitionInfo.add(partition.getVisibleVersionHash());
                    partitionInfo.add(partition.getState());

                    // partition
                    List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
                    List<String> colNames = new ArrayList<String>();
                    for (Column column : partitionColumns) {
                        colNames.add(column.getName());
                    }
                    partitionInfo.add(joiner.join(colNames));

                    partitionInfo.add(entry.getValue().toString());

                    // distribution
                    DistributionInfo distributionInfo = partition.getDistributionInfo();
                    if (distributionInfo.getType() == DistributionInfoType.HASH) {
                        HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                        List<Column> distributionColumns = hashDistributionInfo.getDistributionColumns();
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < distributionColumns.size(); i++) {
                            if (i != 0) {
                                sb.append(", ");
                            }
                            sb.append(distributionColumns.get(i).getName());
                        }
                        partitionInfo.add(sb.toString());
                    } else {
                        partitionInfo.add("ALL KEY");
                    }

                    partitionInfo.add(distributionInfo.getBucketNum());
                    
                    short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partitionId);
                    partitionInfo.add(String.valueOf(replicationNum));
                    
                    DataProperty dataProperty = rangePartitionInfo.getDataProperty(partitionId);
                    partitionInfo.add(dataProperty.getStorageMedium().name());
                    partitionInfo.add(TimeUtils.longToTimeString(dataProperty.getCooldownTimeMs()));

                    partitionInfo.add(TimeUtils.longToTimeString(partition.getLastCheckTime()));

                    partitionInfos.add(partitionInfo);
                }
            } else {
                for (Partition partition : olapTable.getPartitions()) {
                    List<Comparable> partitionInfo = new ArrayList<Comparable>();
                    String partitionName = partition.getName();
                    long partitionId = partition.getId();
                    partitionInfo.add(partitionId);
                    partitionInfo.add(partitionName);
                    partitionInfo.add(partition.getVisibleVersion());
                    partitionInfo.add(partition.getVisibleVersionHash());
                    partitionInfo.add(partition.getState());

                    // partition
                    partitionInfo.add("");
                    partitionInfo.add("");

                    // distribution
                    DistributionInfo distributionInfo = partition.getDistributionInfo();
                    if (distributionInfo.getType() == DistributionInfoType.HASH) {
                        HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                        List<Column> distributionColumns = hashDistributionInfo.getDistributionColumns();
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < distributionColumns.size(); i++) {
                            if (i != 0) {
                                sb.append(", ");
                            }
                            sb.append(distributionColumns.get(i).getName());
                        }
                        partitionInfo.add(sb.toString());
                    } else {
                        partitionInfo.add("ALL KEY");
                    }

                    partitionInfo.add(distributionInfo.getBucketNum());

                    short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partitionId);
                    partitionInfo.add(String.valueOf(replicationNum));

                    DataProperty dataProperty = olapTable.getPartitionInfo().getDataProperty(partitionId);
                    partitionInfo.add(dataProperty.getStorageMedium().name());
                    partitionInfo.add(TimeUtils.longToTimeString(dataProperty.getCooldownTimeMs()));

                    partitionInfo.add(TimeUtils.longToTimeString(partition.getLastCheckTime()));

                    partitionInfos.add(partitionInfo);
                }
            }
        } finally {
            db.readUnlock();
        }

        // set result
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        for (List<Comparable> info : partitionInfos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(comparable.toString());
            }
            result.addRow(row);
        }

        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String partitionIdStr) throws AnalysisException {
        long partitionId = -1L;
        try {
            partitionId = Long.valueOf(partitionIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid partition id format: " + partitionIdStr);
        }

        db.readLock();
        try {
            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                throw new AnalysisException("Partition[" + partitionId + "] does not exist");
            }

            return new IndicesProcDir(db, olapTable, partition);
        } finally {
            db.readUnlock();
        }
    }

}
