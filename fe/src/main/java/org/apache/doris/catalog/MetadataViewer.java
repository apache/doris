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

package org.apache.doris.catalog;

import org.apache.doris.analysis.AdminShowReplicaDistributionStmt;
import org.apache.doris.analysis.AdminShowReplicaStatusStmt;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.Replica.ReplicaStatus;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MetadataViewer {

    public static List<List<String>> getTabletStatus(AdminShowReplicaStatusStmt stmt) throws DdlException {
        return getTabletStatus(stmt.getDbName(), stmt.getTblName(), stmt.getPartitions(),
                               stmt.getStatusFilter(), stmt.getOp());
    }

    private static List<List<String>> getTabletStatus(String dbName, String tblName, List<String> partitions,
            ReplicaStatus statusFilter, Operator op) throws DdlException {
        List<List<String>> result = Lists.newArrayList();

        Catalog catalog = Catalog.getCurrentCatalog();
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();

        Database db = catalog.getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exsit");
        }
        
        db.readLock();
        try {
            Table tbl = db.getTable(tblName);
            if (tbl == null || tbl.getType() != TableType.OLAP) {
                throw new DdlException("Table does not exist or is not OLAP table: " + tblName);
            }
            
            OlapTable olapTable = (OlapTable) tbl;
            
            if (partitions.isEmpty()) {
                partitions.addAll(olapTable.getPartitionNames());
            } else {
                // check partition
                for (String partName : partitions) {
                    Partition partition = olapTable.getPartition(partName);
                    if (partition == null) {
                        throw new DdlException("Partition does not exist: " + partName);
                    }
                }
            }
            
            for (String partName : partitions) {
                Partition partition = olapTable.getPartition(partName);
                long visibleVersion = partition.getVisibleVersion();
                short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());

                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    int schemaHash = olapTable.getSchemaHashByIndexId(index.getId());
                    for (Tablet tablet : index.getTablets()) {
                        long tabletId = tablet.getId();
                        int count = replicationNum;
                        for (Replica replica : tablet.getReplicas()) {
                            --count;
                            List<String> row = Lists.newArrayList();
                            
                            ReplicaStatus status = ReplicaStatus.OK;
                            Backend be = infoService.getBackend(replica.getBackendId());
                            if (be == null || !be.isAvailable() || replica.isBad()) {
                                status = ReplicaStatus.DEAD;
                            } else if (replica.getVersion() < visibleVersion
                                        || replica.getLastFailedVersion() > 0) {
                                    status = ReplicaStatus.VERSION_ERROR;

                            } else if (replica.getSchemaHash() != -1 && replica.getSchemaHash() != schemaHash) {
                                status = ReplicaStatus.SCHEMA_ERROR;
                            }
                            
                            if (filterReplica(status, statusFilter, op)) {
                                continue;
                            }
                            
                            row.add(String.valueOf(tabletId));
                            row.add(String.valueOf(replica.getId()));
                            row.add(String.valueOf(replica.getBackendId()));
                            row.add(String.valueOf(replica.getVersion()));
                            row.add(String.valueOf(replica.getLastFailedVersion()));
                            row.add(String.valueOf(replica.getLastSuccessVersion()));
                            row.add(String.valueOf(visibleVersion));
                            row.add(String.valueOf(replica.getSchemaHash()));
                            row.add(String.valueOf(replica.getVersionCount()));
                            row.add(String.valueOf(replica.isBad()));
                            row.add(replica.getState().name());
                            row.add(status.name());
                            result.add(row);
                        }

                        if (filterReplica(ReplicaStatus.MISSING, statusFilter, op)) {
                            continue;
                        }

                        // get missing replicas
                        for (int i = 0; i < count; ++i) {
                            List<String> row = Lists.newArrayList();
                            row.add(String.valueOf(tabletId));
                            row.add("-1");
                            row.add("-1");
                            row.add("-1");
                            row.add("-1");
                            row.add("-1");
                            row.add("-1");
                            row.add("-1");
                            row.add(FeConstants.null_string);
                            row.add(FeConstants.null_string);
                            row.add(ReplicaStatus.MISSING.name());
                            result.add(row);
                        }
                    }
                }
            }
        } finally {
            db.readUnlock();
        }

        return result;
    }

    private static boolean filterReplica(ReplicaStatus status, ReplicaStatus statusFilter, Operator op) {
        if (statusFilter == null) {
            return false;
        }
        if (op == Operator.EQ) {
            return status != statusFilter;
        } else {
            return status == statusFilter;
        }
    }

    public static List<List<String>> getTabletDistribution(AdminShowReplicaDistributionStmt stmt) throws DdlException {
        return getTabletDistribution(stmt.getDbName(), stmt.getTblName(), stmt.getPartitionNames());
    }

    private static List<List<String>> getTabletDistribution(String dbName, String tblName, PartitionNames partitionNames)
            throws DdlException {
        DecimalFormat df = new DecimalFormat("##.00 %");
        
        List<List<String>> result = Lists.newArrayList();

        Catalog catalog = Catalog.getCurrentCatalog();
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();

        Database db = catalog.getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exsit");
        }

        db.readLock();
        try {
            Table tbl = db.getTable(tblName);
            if (tbl == null || tbl.getType() != TableType.OLAP) {
                throw new DdlException("Table does not exist or is not OLAP table: " + tblName);
            }

            OlapTable olapTable = (OlapTable) tbl;

            List<Long> partitionIds = Lists.newArrayList();
            if (partitionNames == null) {
                for (Partition partition : olapTable.getPartitions()) {
                    partitionIds.add(partition.getId());
                }
            } else {
                // check partition
                for (String partName : partitionNames.getPartitionNames()) {
                    Partition partition = olapTable.getPartition(partName, partitionNames.isTemp());
                    if (partition == null) {
                        throw new DdlException("Partition does not exist: " + partName);
                    }
                    partitionIds.add(partition.getId());
                }
            }
            
            // backend id -> replica count
            Map<Long, Integer> countMap = Maps.newHashMap();
            // init map
            List<Long> beIds = infoService.getBackendIds(false);
            for (long beId : beIds) {
                countMap.put(beId, 0);
            }

            int totalReplicaNum = 0;
            for (long partId : partitionIds) {
                Partition partition = olapTable.getPartition(partId);
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    for (Tablet tablet : index.getTablets()) {
                        for (Replica replica : tablet.getReplicas()) {
                            if (!countMap.containsKey(replica.getBackendId())) {
                                continue;
                            }
                            countMap.put(replica.getBackendId(), countMap.get(replica.getBackendId()) + 1);
                            totalReplicaNum++;
                        }
                    }
                }
            }

            // graph
            Collections.sort(beIds);
            for (Long beId : beIds) {
                List<String> row = Lists.newArrayList();
                row.add(String.valueOf(beId));
                row.add(String.valueOf(countMap.get(beId)));
                row.add(graph(countMap.get(beId), totalReplicaNum, beIds.size()));
                row.add(df.format((double) countMap.get(beId) / totalReplicaNum));
                result.add(row);
            }
            
        } finally {
            db.readUnlock();
        }

        return result;
    }

    private static String graph(int num, int totalNum, int mod) {
        StringBuilder sb = new StringBuilder();
        int normalized = (int) Math.ceil(num * mod / totalNum);
        for (int i = 0; i < normalized; ++i) {
            sb.append(">");
        }
        return sb.toString();
    }
}
