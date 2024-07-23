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

import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.ShowDataSkewStmt;
import org.apache.doris.analysis.ShowReplicaDistributionStmt;
import org.apache.doris.analysis.ShowReplicaStatusStmt;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.Replica.ReplicaStatus;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetadataViewer {

    public static List<List<String>> getTabletStatus(ShowReplicaStatusStmt stmt) throws DdlException {
        return getTabletStatus(stmt.getDbName(), stmt.getTblName(), stmt.getPartitions(),
                               stmt.getStatusFilter(), stmt.getOp());
    }

    private static List<List<String>> getTabletStatus(String dbName, String tblName, List<String> partitions,
            ReplicaStatus statusFilter, Operator op) throws DdlException {
        List<List<String>> result = Lists.newArrayList();

        Env env = Env.getCurrentEnv();
        SystemInfoService infoService = Env.getCurrentSystemInfo();

        Database db = env.getInternalCatalog().getDbOrDdlException(dbName);
        OlapTable olapTable = db.getOlapTableOrDdlException(tblName);

        olapTable.readLock();
        try {
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
                short replicationNum = olapTable.getPartitionInfo()
                        .getReplicaAllocation(partition.getId()).getTotalReplicaNum();

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
                            if (be == null || !be.isAlive() || replica.isBad()) {
                                status = ReplicaStatus.DEAD;
                            } else if (replica.getVersion() < visibleVersion
                                        || replica.getLastFailedVersion() > 0) {
                                status = ReplicaStatus.VERSION_ERROR;

                            } else if (replica.getSchemaHash() != -1 && replica.getSchemaHash() != schemaHash) {
                                status = ReplicaStatus.SCHEMA_ERROR;
                            } else if (replica.isUserDrop()) {
                                status = ReplicaStatus.DROP;
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
                            row.add(String.valueOf(replica.getTotalVersionCount()));
                            row.add(String.valueOf(replica.isBad()));
                            row.add(String.valueOf(replica.isUserDrop()));
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
                            row.add(FeConstants.null_string);
                            row.add(ReplicaStatus.MISSING.name());
                            result.add(row);
                        }
                    }
                }
            }
        } finally {
            olapTable.readUnlock();
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

    public static List<List<String>> getTabletDistribution(ShowReplicaDistributionStmt stmt) throws DdlException {
        return getTabletDistribution(stmt.getDbName(), stmt.getTblName(), stmt.getPartitionNames());
    }

    private static List<List<String>> getTabletDistribution(
            String dbName, String tblName, PartitionNames partitionNames)
            throws DdlException {
        DecimalFormat df = new DecimalFormat("00.00 %");

        List<List<String>> result = Lists.newArrayList();

        Env env = Env.getCurrentEnv();
        SystemInfoService infoService = Env.getCurrentSystemInfo();

        Database db = env.getInternalCatalog().getDbOrDdlException(dbName);
        OlapTable olapTable = db.getOlapTableOrDdlException(tblName);
        olapTable.readLock();
        try {
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
            // backend id -> replica size
            Map<Long, Long> sizeMap = Maps.newHashMap();
            // init map
            List<Long> beIds = infoService.getAllBackendIds(false);
            for (long beId : beIds) {
                countMap.put(beId, 0);
                sizeMap.put(beId, 0L);
            }

            int totalReplicaNum = 0;
            long totalReplicaSize = 0;
            for (long partId : partitionIds) {
                Partition partition = olapTable.getPartition(partId);
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    for (Tablet tablet : index.getTablets()) {
                        for (Replica replica : tablet.getReplicas()) {
                            if (!countMap.containsKey(replica.getBackendId())) {
                                continue;
                            }
                            countMap.put(replica.getBackendId(), countMap.get(replica.getBackendId()) + 1);
                            sizeMap.put(replica.getBackendId(),
                                    sizeMap.get(replica.getBackendId()) + replica.getDataSize());
                            totalReplicaNum++;
                            totalReplicaSize += replica.getDataSize();
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
                row.add(String.valueOf(sizeMap.get(beId)));
                row.add(graph(countMap.get(beId), totalReplicaNum));
                row.add(totalReplicaNum == countMap.get(beId) ? (totalReplicaNum == 0 ? "0.00%" : "100.00%")
                        : df.format((double) countMap.get(beId) / totalReplicaNum));
                row.add(graph(sizeMap.get(beId), totalReplicaSize));
                row.add(totalReplicaSize == sizeMap.get(beId) ? (totalReplicaSize == 0 ? "0.00%" : "100.00%")
                        : df.format((double) sizeMap.get(beId) / totalReplicaSize));
                if (Config.isNotCloudMode()) {
                    row.add("");
                    row.add("");
                } else {
                    Backend be = CloudEnv.getCurrentSystemInfo().getBackend(beId);
                    if (be != null) {
                        row.add(be.getTagMap().get(Tag.CLOUD_CLUSTER_NAME));
                        row.add(be.getTagMap().get(Tag.CLOUD_CLUSTER_ID));
                    } else {
                        row.add("not exist be");
                        row.add("not exist be");
                    }
                }
                result.add(row);
            }

        } finally {
            olapTable.readUnlock();
        }

        return result;
    }

    private static String graph(long num, long totalNum) {
        StringBuilder sb = new StringBuilder();
        long normalized = num == totalNum ? (totalNum == 0L ? 0 : 100) : (int) Math.ceil(num * 100 / totalNum);
        for (int i = 0; i < normalized; ++i) {
            sb.append(">");
        }
        return sb.toString();
    }

    public static List<List<String>> getDataSkew(ShowDataSkewStmt stmt) throws DdlException {
        return getDataSkew(stmt.getDbName(), stmt.getTblName(), stmt.getPartitionNames());
    }

    private static List<List<String>> getDataSkew(String dbName, String tblName, PartitionNames partitionNames)
            throws DdlException {
        DecimalFormat df = new DecimalFormat("00.00 %");

        List<List<String>> result = Lists.newArrayList();
        Env env = Env.getCurrentEnv();

        Database db = env.getInternalCatalog().getDbOrDdlException(dbName);
        OlapTable olapTable = db.getOlapTableOrDdlException(tblName);

        if (olapTable.getPartitionNames().isEmpty()) {
            throw new DdlException("Can not find any partition from " + dbName + "." + tblName);
        }

        // patition -> isTmep
        Map<String, Boolean> allPartionNames = new HashMap<>();
        if (partitionNames == null) {
            for (Partition p : olapTable.getPartitions()) {
                allPartionNames.put(p.getName(), false);
            }
            for (Partition p : olapTable.getAllTempPartitions()) {
                allPartionNames.put(p.getName(), true);
            }
        } else {
            for (String name : partitionNames.getPartitionNames()) {
                allPartionNames.put(name, partitionNames.isTemp());
            }
        }

        olapTable.readLock();
        try {
            Partition partition = null;
            // check partition
            for (Map.Entry<String, Boolean> partName : allPartionNames.entrySet()) {
                partition = olapTable.getPartition(partName.getKey(), partName.getValue());
                if (partition == null) {
                    throw new DdlException("Partition does not exist: " + partName);
                }
                DistributionInfo distributionInfo = partition.getDistributionInfo();
                List<Long> rowCountTabletInfos = Lists.newArrayListWithCapacity(distributionInfo.getBucketNum());
                List<Long> dataSizeTabletInfos = Lists.newArrayListWithCapacity(distributionInfo.getBucketNum());
                for (long i = 0; i < distributionInfo.getBucketNum(); i++) {
                    rowCountTabletInfos.add(0L);
                    dataSizeTabletInfos.add(0L);
                }

                long totalSize = 0;
                for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    List<Long> tabletIds = mIndex.getTabletIdsInOrder();
                    for (int i = 0; i < tabletIds.size(); i++) {
                        Tablet tablet = mIndex.getTablet(tabletIds.get(i));
                        long rowCount = tablet.getRowCount(true);
                        long dataSize = tablet.getDataSize(true);
                        rowCountTabletInfos.set(i, rowCountTabletInfos.get(i) + rowCount);
                        dataSizeTabletInfos.set(i, dataSizeTabletInfos.get(i) + dataSize);
                        totalSize += dataSize;
                    }
                }

                // graph
                for (int i = 0; i < distributionInfo.getBucketNum(); i++) {
                    List<String> row = Lists.newArrayList();
                    row.add(partName.getKey());
                    row.add(String.valueOf(i));
                    row.add(rowCountTabletInfos.get(i).toString());
                    row.add(dataSizeTabletInfos.get(i).toString());
                    row.add(graph(dataSizeTabletInfos.get(i), totalSize));
                    row.add(totalSize == dataSizeTabletInfos.get(i) ? (totalSize == 0L ? "0.00%" : "100.00%") :
                            df.format((double) dataSizeTabletInfos.get(i) / totalSize));
                    result.add(row);
                }
            }
        } finally {
            olapTable.readUnlock();
        }

        return result;
    }
}
