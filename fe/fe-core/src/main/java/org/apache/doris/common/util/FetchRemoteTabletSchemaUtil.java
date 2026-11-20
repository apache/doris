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
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        typeMap.put("TIMESTAMP_NS", Type.TIMESTAMP_NS);
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
        typeMap.put("UUID", Type.UUID);
        typeMap.put("QUANTILE_STATE", Type.QUANTILE_STATE);
    }

    private static Type getTypeFromTypeName(String typeName, int precision, int scale) {
        Type res = typeMap.getOrDefault(typeName, Type.UNSUPPORTED);
        if (res.isScalarType() && (res.isDecimalV3() || res.isDecimalV2())) {
            // set precision and scale
            res = ScalarType.createType(res.getPrimitiveType(), 0, precision, scale);
        }
        return res;
    }

    public List<Column> fetch() throws AnalysisException {
        // 1. Find which Backend (BE) servers the tablets are on
        Preconditions.checkNotNull(remoteTablets);
        if (remoteTablets.isEmpty()) {
            return Collections.emptyList();
        }
        Map<Long, Set<Long>> beIdToTabletId = Maps.newHashMap();
        for (Tablet tablet : remoteTablets) {
            for (Replica replica : tablet.getReplicas()) {
                // only need alive replica
                if (replica.isAlive()) {
                    Set<Long> tabletIds = beIdToTabletId.computeIfAbsent(
                                    replica.getBackendIdWithoutException(), k -> Sets.newHashSet());
                    tabletIds.add(tablet.getId());
                }
            }
        }

        // 2. Randomly select 2 Backend (BE) servers to act as coordinators.
        // Coordinator BE is responsible for collecting all table columns and returning to the FE.
        // Two BE provide a retry opportunity with the second one in case the first attempt fails.
        List<PTabletsLocation> locations = Lists.newArrayList();
        List<Backend> coordinatorBackend = Lists.newArrayList();
        for (Map.Entry<Long, Set<Long>> entry : beIdToTabletId.entrySet()) {
            Long backendId = entry.getKey();
            Set<Long> tabletIds = entry.getValue();
            Backend backend = Env.getCurrentEnv().getCurrentSystemInfo().getBackend(backendId);
            // A sampled replica can be stale if its backend was removed concurrently.
            if (backend == null || !backend.isAlive()) {
                continue;
            }
            LOG.debug("fetch schema from coord backend {}, sample tablets count {}",
                    backend.getId(), tabletIds.size());
            coordinatorBackend.add(backend);
            PTabletsLocation.Builder locationBuilder = PTabletsLocation.newBuilder()
                                                        .setHost(backend.getHost())
                                                        .setBrpcPort(backend.getBrpcPort());
            PTabletsLocation location = locationBuilder.addAllTabletId(tabletIds).build();
            locations.add(location);
        }
        if (coordinatorBackend.isEmpty()) {
            throw new AnalysisException("No live backend can fetch the remote tablet schema");
        }
        // Pick up to two random coordinators for the two attempts.
        Collections.shuffle(coordinatorBackend);
        List<Backend> coordinators = coordinatorBackend.subList(0, Math.min(2, coordinatorBackend.size()));
        PFetchRemoteSchemaRequest request = PFetchRemoteSchemaRequest.newBuilder()
                .addAllTabletLocation(locations)
                .setIsCoordinator(true)
                .build();
        return fetchFromCoordinators(coordinators, request,
                ConnectContext.get().getSessionVariable().fetchRemoteSchemaTimeoutSeconds);
    }

    List<Column> fetchFromCoordinators(List<Backend> coordinators, PFetchRemoteSchemaRequest request,
            long timeoutSeconds) throws AnalysisException {
        Preconditions.checkArgument(!coordinators.isEmpty());
        Exception lastFailure = null;
        for (Backend backend : coordinators) {
            try {
                Future<PFetchRemoteSchemaResponse> future = BackendServiceProxy.getInstance()
                        .fetchRemoteTabletSchemaAsync(backend.getBrpcAddress(), request);
                PFetchRemoteSchemaResponse response;
                try {
                    response = future.get(timeoutSeconds, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    future.cancel(true);
                    throw e;
                }
                int code = response.getStatus().getStatusCode();
                if (code != TStatusCode.OK.getValue()) {
                    String message = response.getStatus().getErrorMsgsCount() > 0
                            ? response.getStatus().getErrorMsgs(0)
                            : "backend returned status code " + code;
                    throw new AnalysisException(message);
                }
                return readColumns(response);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AnalysisException("Interrupted while fetching the remote tablet schema", e);
            } catch (AnalysisException | RpcException | ExecutionException | TimeoutException e) {
                lastFailure = e;
                LOG.warn("Failed to fetch remote tablet schema from {}", backend.getBrpcAddress(), e);
            }
        }
        throw new AnalysisException("Failed to fetch remote tablet schema: " + lastFailure.getMessage(),
                lastFailure);
    }

    private List<Column> readColumns(PFetchRemoteSchemaResponse response) throws AnalysisException {
        TabletSchemaPB schemaPB = response.getMergedSchema();
        List<Column> columns = Lists.newArrayList();
        for (ColumnPB columnPB : schemaPB.getColumnList()) {
            columns.add(initColumnFromPB(columnPB));
        }
        columns.sort(Comparator.comparing(Column::getName));
        return columns;
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
            throw new AnalysisException("default value to string failed");
        }
    }
}
