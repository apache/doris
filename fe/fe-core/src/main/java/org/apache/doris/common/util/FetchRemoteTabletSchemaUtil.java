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

    private List<Tablet> remoteTablets;
    private List<Column> tableColumns;

    public FetchRemoteTabletSchemaUtil(List<Tablet> tablets) {
        this.remoteTablets = tablets;
        this.tableColumns = Lists.newArrayList();
    }

    public List<Column> fetch() {
        // 1. Find which Backend (BE) servers the tablets are on
        Preconditions.checkNotNull(remoteTablets);
        Map<Long, Set<Long>> beIdToTabletId = Maps.newHashMap();
        for (Tablet tablet : remoteTablets) {
            for (Replica replica : tablet.getReplicas()) {
                // only need alive replica
                if (replica.isAlive()) {
                    Set<Long> tabletIds = beIdToTabletId.computeIfAbsent(
                                    replica.getBackendId(), k -> Sets.newHashSet());
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
            LOG.debug("fetch schema from coord backend {}, sample tablets count {}",
                            backend.getId(), tabletIds.size());
            // only need alive be
            if (!backend.isAlive()) {
                continue;
            }
            coordinatorBackend.add(backend);
            PTabletsLocation.Builder locationBuilder = PTabletsLocation.newBuilder()
                                                        .setHost(backend.getHost())
                                                        .setBrpcPort(backend.getBrpcPort());
            PTabletsLocation location = locationBuilder.addAllTabletId(tabletIds).build();
            locations.add(location);
        }
        // pick 2 random coordinator
        Collections.shuffle(coordinatorBackend);
        if (!coordinatorBackend.isEmpty()) {
            coordinatorBackend = coordinatorBackend.subList(0, Math.min(2, coordinatorBackend.size()));
            LOG.debug("pick coordinator backend {}", coordinatorBackend.get(0));
        }
        PFetchRemoteSchemaRequest.Builder requestBuilder = PFetchRemoteSchemaRequest.newBuilder()
                                                                    .addAllTabletLocation(locations)
                                                                    .setIsCoordinator(true);
        // 3. Send rpc to coordinatorBackend util succeed or retry
        for (Backend be : coordinatorBackend) {
            try {
                PFetchRemoteSchemaRequest request = requestBuilder.build();
                Future<PFetchRemoteSchemaResponse> future = BackendServiceProxy.getInstance()
                                            .fetchRemoteTabletSchemaAsync(be.getBrpcAddress(), request);
                PFetchRemoteSchemaResponse response = null;
                try {
                    response = future.get(
                        ConnectContext.get().getSessionVariable().fetchRemoteSchemaTimeoutSeconds, TimeUnit.SECONDS);
                    TStatusCode code = TStatusCode.findByValue(response.getStatus().getStatusCode());
                    String errMsg;
                    if (code != TStatusCode.OK) {
                        if (!response.getStatus().getErrorMsgsList().isEmpty()) {
                            errMsg = response.getStatus().getErrorMsgsList().get(0);
                        } else {
                            errMsg = "fetchRemoteTabletSchemaAsync failed. backend address: "
                                    + be.getHost() + " : " + be.getBrpcPort();
                        }
                        throw new RpcException(be.getHost(), errMsg);
                    }
                    fillColumns(response);
                    return tableColumns;
                } catch (AnalysisException e) {
                    // continue to get result
                    LOG.warn(e);
                } catch (InterruptedException e) {
                    // continue to get result
                    LOG.warn("fetch remote schema future get interrupted Exception");
                } catch (TimeoutException e) {
                    future.cancel(true);
                    // continue to get result
                    LOG.warn("fetch remote schema result timeout, addr {}", be.getBrpcAddress());
                }
            }    catch (RpcException e) {
                LOG.warn("fetch remote schema result rpc exception {}, e {}", be.getBrpcAddress(), e);
            } catch (ExecutionException e) {
                LOG.warn("fetch remote schema ExecutionException, addr {}, e {}", be.getBrpcAddress(), e);
            }
        }
        return tableColumns;
    }

    private void fillColumns(PFetchRemoteSchemaResponse response) throws AnalysisException {
        TabletSchemaPB schemaPB = response.getMergedSchema();
        for (ColumnPB columnPB : schemaPB.getColumnList()) {
            try {
                Column remoteColumn = initColumnFromPB(columnPB);
                tableColumns.add(remoteColumn);
            } catch (Exception e) {
                throw new AnalysisException("column default value to string failed");
            }
        }
        // sort the columns
        Collections.sort(tableColumns, new Comparator<Column>() {
            @Override
            public int compare(Column c1, Column c2) {
                return c1.getName().compareTo(c2.getName());
            }
        });
    }

    private Column initColumnFromPB(ColumnPB column) throws AnalysisException {
        try {
            AggregateType aggType = AggregateType.getAggTypeFromAggName(column.getAggregation());
            Type type = Type.getTypeFromTypeName(column.getType());
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
