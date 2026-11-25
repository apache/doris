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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Type;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.proto.InternalService.PFetchRemoteSchemaRequest;
import org.apache.doris.proto.InternalService.PFetchRemoteSchemaResponse;
import org.apache.doris.proto.InternalService.PTabletsLocation;
import org.apache.doris.proto.OlapFile.ColumnPB;
import org.apache.doris.proto.OlapFile.TabletSchemaPB;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

// This class is used to pull the specified tablets' columns existing on the Backend (BE)
// including regular columns and columns decomposed by variants
public class FetchRemoteTabletSchemaUtil {
    private static final Logger LOG = LogManager.getLogger(FetchRemoteTabletSchemaUtil.class);

    private final int tabletCount;
    private final Map<Long, TabletReplicas> tabletReplicas = Maps.newLinkedHashMap();

    private static final class TabletReplicas {
        private final List<Replica> replicas;
        private final long visibleVersion;

        private TabletReplicas(List<Replica> replicas, long visibleVersion) {
            this.replicas = replicas;
            this.visibleVersion = visibleVersion;
        }
    }

    public static final class VersionedTablet {
        private final Tablet tablet;
        private final long visibleVersion;

        public VersionedTablet(Tablet tablet, long visibleVersion) {
            this.tablet = tablet;
            this.visibleVersion = visibleVersion;
        }

        public Tablet getTablet() {
            return tablet;
        }

        public long getVisibleVersion() {
            return visibleVersion;
        }
    }

    /** Capture the candidate replica lists under the table lock and resolve Cloud routes during fetch. */
    public FetchRemoteTabletSchemaUtil(List<VersionedTablet> tablets) {
        tabletCount = tablets.size();
        for (VersionedTablet target : tablets) {
            Tablet tablet = target.getTablet();
            tabletReplicas.put(tablet.getId(),
                    new TabletReplicas(Lists.newArrayList(tablet.getReplicas()), target.getVisibleVersion()));
        }
    }

    public static final class PartitionTablets {
        private final Partition partition;
        private final List<Tablet> tablets;
        private final long visibleVersion;

        public PartitionTablets(Partition partition, List<Tablet> tablets, long visibleVersion) {
            this.partition = partition;
            this.tablets = tablets;
            this.visibleVersion = visibleVersion;
        }
    }

    /** Capture tablet lists under the table lock. Cloud visibility can require a separate RPC. */
    public static List<PartitionTablets> snapshotPartitions(Collection<Partition> partitions) {
        boolean cloudMode = Config.isCloudMode();
        List<PartitionTablets> snapshot = Lists.newArrayList();
        for (Partition partition : partitions) {
            long visibleVersion = cloudMode ? -1L : partition.getVisibleVersion();
            if (cloudMode || visibleVersion > Partition.PARTITION_INIT_VERSION) {
                snapshot.add(new PartitionTablets(partition, partition.getBaseIndex().getTablets(), visibleVersion));
            }
        }
        return snapshot;
    }

    public static List<VersionedTablet> collectTablets(List<PartitionTablets> partitions) throws AnalysisException {
        if (partitions.isEmpty()) {
            return Collections.emptyList();
        }
        boolean cloudMode = Config.isCloudMode();
        List<Long> cloudVersions = cloudMode ? getCloudVisibleVersions(partitions) : Collections.emptyList();
        int maxTablets = ConnectContext.get().getSessionVariable().maxFetchRemoteTabletCount;
        List<VersionedTablet> tablets = Lists.newArrayList();
        long tabletCount = 0;
        for (int i = 0; i < partitions.size(); ++i) {
            PartitionTablets partition = partitions.get(i);
            long version = cloudMode ? cloudVersions.get(i) : partition.visibleVersion;
            if (version <= Partition.PARTITION_INIT_VERSION) {
                continue;
            }
            tabletCount += partition.tablets.size();
            if (tabletCount <= maxTablets) {
                for (Tablet tablet : partition.tablets) {
                    tablets.add(new VersionedTablet(tablet, version));
                }
            }
        }
        checkTabletCount(tabletCount, maxTablets);
        return tablets;
    }

    private static List<Long> getCloudVisibleVersions(List<PartitionTablets> partitions) throws AnalysisException {
        try {
            // Obtain the authoritative versions in one RPC. A cached empty partition might have been written to.
            return CloudPartition.getSnapshotVisibleVersionFromMs(
                    partitions.stream().map(p -> (CloudPartition) p.partition).collect(Collectors.toList()), false);
        } catch (RpcException e) {
            throw new AnalysisException("Extended DESCRIBE could not fetch partition versions", e);
        }
    }

    private static void checkTabletCount(long tabletCount, int maxTablets) throws AnalysisException {
        if (tabletCount > 0 && tabletCount > maxTablets) {
            throw new AnalysisException("Extended DESCRIBE needs to read " + tabletCount
                    + " tablets, exceeding max_fetch_remote_schema_tablet_count=" + maxTablets);
        }
    }

    private static final Map<String, Type> typeMap = new HashMap<>();

    static {
        typeMap.put("TINYINT", Type.TINYINT);
        typeMap.put("SMALLINT", Type.SMALLINT);
        typeMap.put("INT", Type.INT);
        typeMap.put("BIGINT", Type.BIGINT);
        typeMap.put("LARGEINT", Type.LARGEINT);
        typeMap.put("UNSIGNED_TINYINT", Type.UNSUPPORTED);
        typeMap.put("UNSIGNED_SMALLINT", Type.UNSUPPORTED);
        typeMap.put("UNSIGNED_INT", Type.UNSUPPORTED);
        typeMap.put("UNSIGNED_BIGINT", Type.UNSUPPORTED);
        typeMap.put("FLOAT", Type.FLOAT);
        typeMap.put("DISCRETE_DOUBLE", Type.DOUBLE);
        typeMap.put("DOUBLE", Type.DOUBLE);
        typeMap.put("CHAR", Type.CHAR);
        typeMap.put("DATE", Type.DATE);
        typeMap.put("DATEV2", Type.DATEV2);
        typeMap.put("DATETIMEV2", Type.DATETIMEV2);
        typeMap.put("DATETIME", Type.DATETIME);
        typeMap.put("DECIMAL32", Type.DECIMAL32);
        typeMap.put("DECIMAL64", Type.DECIMAL64);
        typeMap.put("DECIMAL128I", Type.DECIMAL128);
        typeMap.put("DECIMAL", Type.DECIMALV2);
        typeMap.put("VARCHAR", Type.VARCHAR);
        typeMap.put("STRING", Type.STRING);
        typeMap.put("JSONB", Type.JSONB);
        typeMap.put("VARIANT", Type.VARIANT);
        typeMap.put("BOOLEAN", Type.BOOLEAN);
        typeMap.put("HLL", Type.HLL);
        typeMap.put("STRUCT", Type.STRUCT);
        typeMap.put("LIST", Type.UNSUPPORTED);
        typeMap.put("MAP", Type.MAP);
        typeMap.put("OBJECT", Type.UNSUPPORTED);
        typeMap.put("ARRAY", Type.ARRAY);
        typeMap.put("IPV4", Type.IPV4);
        typeMap.put("IPV6", Type.IPV6);
        typeMap.put("QUANTILE_STATE", Type.QUANTILE_STATE);
    }

    public static Type getTypeFromTypeName(String typeName, int precision, int scale) {
        Type res = typeMap.getOrDefault(typeName, Type.UNSUPPORTED);
        if (res.isScalarType() && (res.isDecimalV3() || res.isDecimalV2())) {
            // set precision and scale
            res = ScalarType.createType(res.getPrimitiveType(), 0, precision, scale);
        }
        return res;
    }

    public static Type getTypeFromTypeName(String typeName) {
        return typeMap.getOrDefault(typeName, Type.UNSUPPORTED);
    }

    public List<Column> fetch() throws AnalysisException {
        if (tabletReplicas.isEmpty()) {
            return Collections.emptyList();
        }
        int maxTablets = ConnectContext.get().getSessionVariable().maxFetchRemoteTabletCount;
        checkTabletCount(tabletCount, maxTablets);
        // Resolve a Cloud compute group at most once, after the table lock has been released.
        String clusterId = null;
        Map<Long, Set<Long>> beIdToTabletId = Maps.newHashMap();
        Map<Long, Backend> liveBackends = Maps.newHashMap();
        for (Map.Entry<Long, TabletReplicas> tablet : tabletReplicas.entrySet()) {
            boolean hasLiveBackend = false;
            for (Replica replica : tablet.getValue().replicas) {
                if (!replica.isAlive()) {
                    continue;
                }
                long backendId;
                if (replica instanceof CloudReplica) {
                    if (clusterId == null) {
                        clusterId = resolveComputeGroup();
                    }
                    try {
                        backendId = ((CloudReplica) replica).getBackendIdWithClusterId(clusterId);
                    } catch (ComputeGroupException e) {
                        LOG.debug("Could not route tablet {} for remote schema", tablet.getKey(), e);
                        continue;
                    }
                } else {
                    backendId = replica.getBackendIdWithoutException();
                }
                Backend backend = liveBackends.get(backendId);
                if (backend == null) {
                    backend = Env.getCurrentSystemInfo().getBackend(backendId);
                    if (backend == null || !backend.isAlive()) {
                        continue;
                    }
                    liveBackends.put(backendId, backend);
                }
                beIdToTabletId.computeIfAbsent(backendId, k -> Sets.newHashSet()).add(tablet.getKey());
                hasLiveBackend = true;
            }
            if (!hasLiveBackend) {
                throw new AnalysisException("Extended DESCRIBE could not find a live replica for tablet "
                        + tablet.getKey());
            }
        }

        // 2. Randomly select 2 Backend (BE) servers to act as coordinators.
        // Coordinator BE is responsible for collecting all table columns and returning to the FE.
        // Two BE provide a retry opportunity with the second one in case the first attempt fails.
        List<PTabletsLocation> locations = Lists.newArrayList();
        List<Backend> coordinatorBackend = Lists.newArrayList(liveBackends.values());
        for (Map.Entry<Long, Set<Long>> entry : beIdToTabletId.entrySet()) {
            Backend backend = liveBackends.get(entry.getKey());
            PTabletsLocation.Builder location = PTabletsLocation.newBuilder()
                    .setHost(backend.getHost()).setBrpcPort(backend.getBrpcPort());
            for (long tabletId : entry.getValue()) {
                location.addTabletId(tabletId);
                location.addTabletVisibleVersion(tabletReplicas.get(tabletId).visibleVersion);
            }
            locations.add(location.build());
        }
        Collections.shuffle(coordinatorBackend);
        PFetchRemoteSchemaRequest request = PFetchRemoteSchemaRequest.newBuilder()
                .addAllTabletLocation(locations)
                .setIsCoordinator(true)
                .build();
        long timeoutSeconds = Math.max(0L, ConnectContext.get().getSessionVariable().fetchRemoteSchemaTimeoutSeconds);
        long timeoutNanos = TimeUnit.SECONDS.toNanos(timeoutSeconds);
        long startNanos = System.nanoTime();
        Exception lastFailure = null;
        for (Backend be : coordinatorBackend.subList(0, Math.min(2, coordinatorBackend.size()))) {
            // A second coordinator only gets the time left by the first attempt.
            if (lastFailure != null && System.nanoTime() - startNanos >= timeoutNanos) {
                break;
            }
            PFetchRemoteSchemaResponse response;
            try {
                Future<PFetchRemoteSchemaResponse> future = BackendServiceProxy.getInstance()
                        .fetchRemoteTabletSchemaAsync(be.getBrpcAddress(), request);
                try {
                    while (DebugPointUtil.isEnable("FetchRemoteTabletSchemaUtil.fetch.waitForResponse")) {
                        Thread.sleep(100);
                    }
                    long remainingNanos = Math.max(0L, timeoutNanos - (System.nanoTime() - startNanos));
                    response = future.get(remainingNanos, TimeUnit.NANOSECONDS);
                } catch (TimeoutException e) {
                    future.cancel(true);
                    throw new AnalysisException("Remote schema RPC timed out on " + be.getBrpcAddress(), e);
                } catch (InterruptedException e) {
                    future.cancel(true);
                    throw e;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AnalysisException("Extended DESCRIBE interrupted while fetching remote schema", e);
            } catch (AnalysisException | RpcException | ExecutionException e) {
                lastFailure = e;
                LOG.warn("Failed to fetch remote schema from {}", be.getBrpcAddress(), e);
                continue;
            }
            if (!response.hasStatus() || response.getStatus().getStatusCode() != TStatusCode.OK.getValue()) {
                AnalysisException failure = new AnalysisException("Remote schema RPC failed on "
                        + be.getBrpcAddress() + " " + response.getStatus().getErrorMsgsList());
                // Worker RPCs already retry transport failures within their own deadline.
                // Moving an accepted request to another coordinator repeats the worker fan-out.
                if (response.hasStatus() && response.getCoordinatorAccepted()
                        && response.getStatus().getStatusCode() != TStatusCode.NETWORK_ERROR.getValue()) {
                    throw failure;
                }
                lastFailure = failure;
            } else if (!response.hasMergedSchema()) {
                lastFailure = new AnalysisException("Missing merged schema from " + be.getBrpcAddress());
            } else if (!response.getTabletVersionsVerified()) {
                lastFailure = new AnalysisException("Coordinator could not verify tablet versions on "
                        + be.getBrpcAddress());
            } else {
                try {
                    return decodeColumns(response.getMergedSchema());
                } catch (AnalysisException e) {
                    lastFailure = e;
                }
            }
            LOG.warn("Failed to fetch remote schema from {}", be.getBrpcAddress(), lastFailure);
        }
        throw new AnalysisException("Extended DESCRIBE failed to fetch a complete remote schema"
                + (lastFailure == null ? "" : " (" + lastFailure.getMessage() + ")"), lastFailure);
    }

    private String resolveComputeGroup() throws AnalysisException {
        try {
            return ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCurrentClusterId();
        } catch (ComputeGroupException e) {
            throw new AnalysisException("Extended DESCRIBE could not resolve the compute group", e);
        }
    }

    private List<Column> decodeColumns(TabletSchemaPB schemaPB) throws AnalysisException {
        List<Column> tableColumns = Lists.newArrayListWithCapacity(schemaPB.getColumnCount());
        for (ColumnPB columnPB : schemaPB.getColumnList()) {
            tableColumns.add(initColumnFromPB(columnPB));
        }
        tableColumns.sort(Comparator.comparing(Column::getName));
        return tableColumns;
    }

    private Column initColumnFromPB(ColumnPB column) throws AnalysisException {
        try {
            AggregateType aggType = AggregateType.getAggTypeFromAggName(column.getAggregation());
            Type type = getTypeFromTypeName(column.getType(), column.getPrecision(), column.getFrac());
            String columnName = column.getName();
            boolean isKey = column.getIsKey();
            boolean isNullable = column.getIsNullable();
            String defaultValue = column.getDefaultValue().toString("UTF-8");
            if (defaultValue.equals("")) {
                defaultValue = null;
            }
            if (isKey) {
                aggType = null;
            }
            do {
                if (type.isArrayType()) {
                    List<ColumnPB> childColumn = column.getChildrenColumnsList();
                    if (childColumn == null || childColumn.size() != 1) {
                        break;
                    }
                    Column child = initColumnFromPB(childColumn.get(0));
                    type = new ArrayType(child.getType());
                } else if (type.isMapType()) {
                    List<ColumnPB> childColumn = column.getChildrenColumnsList();
                    if (childColumn == null || childColumn.size() != 2) {
                        break;
                    }
                    Column keyChild = initColumnFromPB(childColumn.get(0));
                    Column valueChild = initColumnFromPB(childColumn.get(1));
                    type = new MapType(keyChild.getType(), valueChild.getType());
                } else if (type.isStructType()) {
                    List<ColumnPB> childColumn = column.getChildrenColumnsList();
                    if (childColumn == null) {
                        break;
                    }
                    List<Type> childTypes = Lists.newArrayList();
                    for (ColumnPB childPB : childColumn) {
                        childTypes.add(initColumnFromPB(childPB).getType());
                    }
                    type = new StructType(childTypes);
                }
            } while (false);
            return new Column(columnName, type, isKey, aggType, isNullable,
                                                    defaultValue, "remote schema");
        } catch (Exception e) {
            throw new AnalysisException("Invalid remote schema column " + column.getName(), e);
        }
    }
}
