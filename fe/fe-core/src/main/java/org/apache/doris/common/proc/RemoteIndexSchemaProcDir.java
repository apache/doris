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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.FetchRemoteTabletSchemaUtil;
import org.apache.doris.qe.ConnectContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * SHOW PROC /dbs/dbId/tableId/index_schema/indexId
 * show index schema
 */
public class RemoteIndexSchemaProcDir implements ProcDirInterface {
    private final OlapTable table;
    private final ProcResult baseSchema;
    private final Set<String> bfColumns;

    public RemoteIndexSchemaProcDir(OlapTable table, ProcResult baseSchema, Set<String> bfColumns) {
        this.table = table;
        this.baseSchema = baseSchema;
        this.bfColumns = bfColumns;
    }

    @Override
    public ProcResult fetchResult() {
        return snapshot().fetchResult();
    }

    public RemoteIndexSchemaProcNode snapshot() {
        table.readLock();
        try {
            int maxFetchCount = ConnectContext.get().getSessionVariable().maxFetchRemoteTabletCount;
            if (Config.isCloudMode()) {
                List<Partition> partitions = List.copyOf(table.getPartitions());
                return new RemoteIndexSchemaProcNode(baseSchema, bfColumns,
                        () -> snapshotCloudTablets(partitions, maxFetchCount).fetch());
            }
            return new RemoteIndexSchemaProcNode(baseSchema, bfColumns, table.getSampleTablets(maxFetchCount));
        } finally {
            table.readUnlock();
        }
    }

    private FetchRemoteTabletSchemaUtil snapshotCloudTablets(List<Partition> partitions, int maxFetchCount) {
        // A cache miss in CloudPartition.getVisibleVersion can call Meta Service.
        List<Partition> nonEmptyPartitions = partitions.stream()
                .filter(partition -> partition.getVisibleVersion() > Partition.PARTITION_INIT_VERSION)
                .collect(Collectors.toCollection(ArrayList::new));
        table.readLock();
        try {
            // A partition can be replaced while the version requests are in flight.
            nonEmptyPartitions.removeIf(partition -> table.getPartition(partition.getId()) != partition);
            return new FetchRemoteTabletSchemaUtil(OlapTable.getSampleTablets(nonEmptyPartitions, maxFetchCount));
        } finally {
            table.readUnlock();
        }
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String partitionString) throws AnalysisException {
        String[] partitionNames = partitionString.split(",");
        if (partitionNames.length == 0) {
            throw new AnalysisException("Describe table[" + table.getName() + "] failed");
        }
        table.readLock();
        try {
            List<Tablet> tablets = new ArrayList<>();
            for (String partitionName : partitionNames) {
                Partition partition = table.getPartition(partitionName);
                if (partition == null) {
                    throw new AnalysisException("Describe table[" + table.getName() + "] failed");
                }
                tablets.addAll(partition.getBaseIndex().getTablets());
            }
            int maxFetchCount = ConnectContext.get().getSessionVariable().maxFetchRemoteTabletCount;
            if (tablets.size() > maxFetchCount) {
                Collections.shuffle(tablets);
                tablets = tablets.subList(0, maxFetchCount);
            }
            return new RemoteIndexSchemaProcNode(baseSchema, bfColumns, tablets);
        } finally {
            table.readUnlock();
        }
    }
}
