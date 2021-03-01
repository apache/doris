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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.external.elasticsearch.EsShardPartitions;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
 * SHOW PROC /dbs/dbId/tableId/espartitions
 * show partitions' detail info within a table
 */
public class EsPartitionsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("IndexName").add("PartitionKey").add("Range").add("DistributionKey")
            .add("Shards").add("ReplicationNum")
            .build();

    public static final int PARTITION_NAME_INDEX = 1;

    private Database db;
    private EsTable esTable;

    public EsPartitionsProcDir(Database db, EsTable esTable) {
        this.db = db;
        this.esTable = esTable;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(esTable);
        Preconditions.checkState(esTable.getType() == TableType.ELASTICSEARCH);

        // get info
        List<List<Comparable>> partitionInfos = new ArrayList<List<Comparable>>();
        esTable.readLock();
        try {
            RangePartitionInfo rangePartitionInfo = null;
            if (esTable.getPartitionInfo().getType() == PartitionType.RANGE) {
                rangePartitionInfo = (RangePartitionInfo) esTable.getEsTablePartitions().getPartitionInfo();
            }
            Joiner joiner = Joiner.on(", ");
            Map<String, EsShardPartitions> unPartitionedIndices = esTable.getEsTablePartitions().getUnPartitionedIndexStates();
            Map<String, EsShardPartitions> partitionedIndices = esTable.getEsTablePartitions().getPartitionedIndexStates();
            for (EsShardPartitions esShardPartitions : unPartitionedIndices.values()) {
                List<Comparable> partitionInfo = new ArrayList<Comparable>();
                partitionInfo.add(esShardPartitions.getIndexName());
                partitionInfo.add("-");  // partition key
                partitionInfo.add("-");  // range
                partitionInfo.add("-");  // dis
                partitionInfo.add(esShardPartitions.getShardRoutings().size());  // shards
                partitionInfo.add(1);  //  replica num
                partitionInfos.add(partitionInfo);
            }
            for (EsShardPartitions esShardPartitions : partitionedIndices.values()) {
                List<Comparable> partitionInfo = new ArrayList<Comparable>();
                partitionInfo.add(esShardPartitions.getIndexName());
                List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
                List<String> colNames = new ArrayList<String>();
                for (Column column : partitionColumns) {
                    colNames.add(column.getName());
                }
                partitionInfo.add(joiner.join(colNames));  // partition key
                partitionInfo.add(rangePartitionInfo.getRange(esShardPartitions.getPartitionId()).toString()); // range
                partitionInfo.add("-");  // dis
                partitionInfo.add(esShardPartitions.getShardRoutings().size());  // shards
                partitionInfo.add(1);  //  replica num
                partitionInfos.add(partitionInfo);
            }
        } finally {
            esTable.readUnlock();
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
    public ProcNodeInterface lookup(String indexName) throws AnalysisException {
        return new EsShardProcDir(db, esTable, indexName);
    }

}
