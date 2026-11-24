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

// This class is used to pull the specified tablets' columns existing on the Backend (BE)
// including regular columns and columns decomposed by variants
public class FetchRemoteTabletSchemaUtil {
    private static final Logger LOG = LogManager.getLogger(FetchRemoteTabletSchemaUtil.class);

    private final List<Tablet> remoteTablets;

    public FetchRemoteTabletSchemaUtil(List<Tablet> tablets) {
        this.remoteTablets = tablets;
    }

    public static class PartitionTablets {
        private final Partition partition;
        private final List<Tablet> tablets;

        public PartitionTablets(Partition partition, List<Tablet> tablets) {
            this.partition = partition;
            this.tablets = tablets;
        }
    }

    /** Capture tablet lists under the table lock. Cloud visibility can require a separate RPC. */
    public static List<PartitionTablets> snapshotPartitions(Collection<Partition> partitions) {
        boolean cloudMode = Config.isCloudMode();
        List<PartitionTablets> snapshot = Lists.newArrayList();
        for (Partition partition : partitions) {
            if (cloudMode || partition.getVisibleVersion() > Partition.PARTITION_INIT_VERSION) {
                snapshot.add(new PartitionTablets(partition, partition.getBaseIndex().getTablets()));
            }
        }
        return snapshot;
    }

    public static List<Column> fetchPartitions(List<PartitionTablets> partitions) throws AnalysisException {
        if (partitions.isEmpty()) {
            return Collections.emptyList();
        }
        boolean cloudMode = Config.isCloudMode();
        int maxTablets = ConnectContext.get().getSessionVariable().maxFetchRemoteTabletCount;
        List<Tablet> tablets = Lists.newArrayList();
        long tabletCount = 0;
        for (PartitionTablets partition : partitions) {
            if (cloudMode && partition.partition.getVisibleVersion() <= Partition.PARTITION_INIT_VERSION) {
                continue;
            }
            tabletCount += partition.tablets.size();
            if (tabletCount <= maxTablets) {
                tablets.addAll(partition.tablets);
            }
        }
        checkTabletCount(tabletCount, maxTablets);
        return new FetchRemoteTabletSchemaUtil(tablets).fetch();
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
        if (remoteTablets.isEmpty()) {
            return Collections.emptyList();
        }
        int maxTablets = ConnectContext.get().getSessionVariable().maxFetchRemoteTabletCount;
        checkTabletCount(remoteTablets.size(), maxTablets);
        // 1. Find which Backend (BE) servers the tablets are on
        Map<Long, Set<Long>> beIdToTabletId = Maps.newHashMap();
        Map<Long, Backend> liveBackends = Maps.newHashMap();
        for (Tablet tablet : remoteTablets) {
            boolean hasLiveBackend = false;
            for (Replica replica : tablet.getReplicas()) {
                if (!replica.isAlive()) {
                    continue;
                }
                long backendId = replica.getBackendIdWithoutException();
                Backend backend = liveBackends.get(backendId);
                if (backend == null) {
                    backend = Env.getCurrentSystemInfo().getBackend(backendId);
                    if (backend == null || !backend.isAlive()) {
                        continue;
                    }
                    liveBackends.put(backendId, backend);
                }
                beIdToTabletId.computeIfAbsent(backendId, k -> Sets.newHashSet()).add(tablet.getId());
                hasLiveBackend = true;
            }
            if (!hasLiveBackend) {
                throw new AnalysisException("Extended DESCRIBE could not find a live replica for tablet "
                        + tablet.getId());
            }
        }

        // 2. Randomly select 2 Backend (BE) servers to act as coordinators.
        // Coordinator BE is responsible for collecting all table columns and returning to the FE.
        // Two BE provide a retry opportunity with the second one in case the first attempt fails.
        List<PTabletsLocation> locations = Lists.newArrayList();
        List<Backend> coordinatorBackend = Lists.newArrayList(liveBackends.values());
        for (Map.Entry<Long, Set<Long>> entry : beIdToTabletId.entrySet()) {
            Backend backend = liveBackends.get(entry.getKey());
            locations.add(PTabletsLocation.newBuilder()
                    .setHost(backend.getHost())
                    .setBrpcPort(backend.getBrpcPort())
                    .addAllTabletId(entry.getValue())
                    .build());
        }
        Collections.shuffle(coordinatorBackend);
        PFetchRemoteSchemaRequest request = PFetchRemoteSchemaRequest.newBuilder()
                .addAllTabletLocation(locations)
                .setIsCoordinator(true)
                .build();
        long timeoutSeconds = ConnectContext.get().getSessionVariable().fetchRemoteSchemaTimeoutSeconds;
        Exception lastFailure = null;
        for (Backend be : coordinatorBackend.subList(0, Math.min(2, coordinatorBackend.size()))) {
            try {
                Future<PFetchRemoteSchemaResponse> future = BackendServiceProxy.getInstance()
                        .fetchRemoteTabletSchemaAsync(be.getBrpcAddress(), request);
                PFetchRemoteSchemaResponse response;
                try {
                    response = future.get(timeoutSeconds, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    future.cancel(true);
                    throw new AnalysisException("Remote schema RPC timed out on " + be.getBrpcAddress(), e);
                } catch (InterruptedException e) {
                    future.cancel(true);
                    throw e;
                }
                if (!response.hasStatus() || response.getStatus().getStatusCode() != TStatusCode.OK.getValue()) {
                    throw new AnalysisException("Remote schema RPC failed on " + be.getBrpcAddress() + " "
                            + response.getStatus().getErrorMsgsList());
                }
                if (!response.hasMergedSchema()) {
                    throw new AnalysisException("Missing merged schema from " + be.getBrpcAddress());
                }
                return decodeColumns(response.getMergedSchema());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AnalysisException("Extended DESCRIBE interrupted while fetching remote schema", e);
            } catch (AnalysisException | RpcException | ExecutionException e) {
                lastFailure = e;
                LOG.warn("Failed to fetch remote schema from {}", be.getBrpcAddress(), e);
            }
        }
        throw new AnalysisException("Extended DESCRIBE failed to fetch a complete remote schema"
                + (lastFailure == null ? "" : " (" + lastFailure.getMessage() + ")"), lastFailure);
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
