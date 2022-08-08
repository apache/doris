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

package org.apache.doris.common.util;

import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.DdlException;

import java.util.Collection;
import java.util.Set;

public class IdGeneratorUtil {

    public static long getBufferSizeForCreateTable(CreateTableStmt stmt, ReplicaAllocation replicaAlloc)
            throws DdlException {
        long bufferSize = 1;
        long partitionNum = stmt.getPartitionDesc() == null ? 1 :
                stmt.getPartitionDesc().getSinglePartitionDescs().size();
        long indexNum = stmt.getRollupAlterClauseList().size() + 1;
        long bucketNum = stmt.getDistributionDesc().toDistributionInfo(stmt.getColumns()).getBucketNum();
        bufferSize = bufferSize + partitionNum + indexNum;
        if (stmt.getPartitionDesc() == null) {
            bufferSize = bufferSize + (replicaAlloc.getTotalReplicaNum() + 1) * indexNum * bucketNum;
        } else {
            for (SinglePartitionDesc partitionDesc : stmt.getPartitionDesc().getSinglePartitionDescs()) {
                long replicaNum = partitionDesc.getReplicaAlloc().getTotalReplicaNum();
                bufferSize = bufferSize + (replicaNum + 1) * indexNum * bucketNum;
            }
        }
        return bufferSize;
    }

    public static long getBufferSizeForTruncateTable(OlapTable table, Collection<Long> partitionIds) {
        long bufferSize = 0;
        for (Long partitionId : partitionIds) {
            bufferSize = bufferSize + 1;
            long replicaNum = table.getPartitionInfo().getReplicaAllocation(partitionId).getTotalReplicaNum();
            long indexNum = table.getIndexIdToMeta().size();
            long bucketNum = table.getPartition(partitionId).getDistributionInfo().getBucketNum();
            bufferSize = bufferSize + (replicaNum + 1) * indexNum * bucketNum;
        }
        return bufferSize;
    }

    public static long getBufferSizeForAlterTable(OlapTable table, Set<Long> indexIdSet) {
        long bufferSize = 1 + indexIdSet.size();
        for (Long indexId : indexIdSet) {
            for (Partition partition : table.getPartitions()) {
                MaterializedIndex originIndex = partition.getIndex(indexId);
                for (Tablet baseTablet : originIndex.getTablets()) {
                    bufferSize = bufferSize + 1 + baseTablet.getReplicas().size();
                }
            }
        }
        return bufferSize;
    }
}
