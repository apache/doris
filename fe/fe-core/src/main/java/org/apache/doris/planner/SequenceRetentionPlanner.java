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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TOlapTablePartition;
import org.apache.doris.thrift.TOlapTableSink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Captures immutable planning contexts only after callers release catalog locks. */
public final class SequenceRetentionPlanner {
    private SequenceRetentionPlanner() {
    }

    public static void prepare(TOlapTableSink sink) throws UserException {
        if (sink.getPartition().getSequenceRetentionGap() == 0) {
            return;
        }
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(sink.getDbId());
        OlapTable table = (OlapTable) db.getTableOrMetaException(sink.getTableId());
        preparePartitions(sink.getDbId(), sink.getTxnId(), table, sink.getPartition().getPartitions());
    }

    public static void preparePartitions(long dbId, long txnId, OlapTable table,
            List<TOlapTablePartition> partitions) throws UserException {
        long gap = table.getSequenceRetentionGap();
        if (gap == 0) {
            return;
        }
        boolean cloud = Config.isCloudMode();
        List<CloudPartition> cloudPartitions = new ArrayList<>();
        Map<Long, Long> observedVersions = new HashMap<>();
        table.readLock();
        try {
            for (TOlapTablePartition partition : partitions) {
                if (partition.getId() == 0 && table.getPartitionInfo().enableAutomaticPartition()) {
                    continue;
                }
                Partition current = table.getPartition(partition.getId());
                if (current == null) {
                    throw new UserException("Partition was dropped while planning: " + partition.getId());
                }
                if (cloud) {
                    cloudPartitions.add((CloudPartition) current);
                } else {
                    observedVersions.put(current.getId(), current.getVisibleVersion());
                }
            }
        } finally {
            table.readUnlock();
        }
        if (!cloudPartitions.isEmpty()) {
            List<Long> visibleVersions;
            try {
                visibleVersions = CloudPartition.getSnapshotVisibleVersionFromMs(cloudPartitions, true);
            } catch (RpcException e) {
                throw new UserException("Cannot read sequence retention planning versions", e);
            }
            for (int i = 0; i < cloudPartitions.size(); ++i) {
                observedVersions.put(cloudPartitions.get(i).getId(), visibleVersions.get(i));
            }
        }
        if (observedVersions.isEmpty()) {
            return;
        }
        Map<Long, Long> versions = Env.getCurrentGlobalTransactionMgr()
                .captureSequenceRetentionVersions(dbId, txnId, table.getId(), gap, observedVersions);
        for (TOlapTablePartition partition : partitions) {
            if (partition.getId() != 0) {
                partition.setSequenceRetentionPlanVersion(versions.get(partition.getId()));
            }
        }
    }
}
