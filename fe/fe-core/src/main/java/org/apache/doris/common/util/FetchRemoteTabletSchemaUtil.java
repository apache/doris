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


public class FetchRemoteTabletSchemaUtil {
    private static final Logger LOG = LogManager.getLogger(FetchRemoteTabletSchemaUtil.class);

    private List<Tablet> tablets;
    private List<Column> columns;

    public FetchRemoteTabletSchemaUtil(List<Tablet> tablets) {
        this.tablets = tablets;
        this.columns = Lists.newArrayList();
    }

    public List<Column> fetch() {
        // find be
        Preconditions.checkNotNull(tablets);
        Map<Long, Set<Long>> beIdToTabletId = Maps.newHashMap();
        for (Tablet tablet : tablets) {
            for (Replica replica : tablet.getReplicas()) {
                Set<Long> tabletIds = beIdToTabletId.computeIfAbsent(
                                    replica.getBackendId(), k -> Sets.newHashSet());
                tabletIds.add(tablet.getId());
            }
        }

        // build PTabletsLocation
        List<PTabletsLocation> locations = Lists.newArrayList();
        List<Backend> coordinatorBackend = Lists.newArrayList();
        for (Map.Entry<Long, Set<Long>> entry : beIdToTabletId.entrySet()) {
            Long backendId = entry.getKey();
            Set<Long> tabletIds = entry.getValue();
            Backend backend = Env.getCurrentEnv().getCurrentSystemInfo().getBackend(backendId);
            if (coordinatorBackend.size() < 2) {
                coordinatorBackend.add(backend);
            }
            PTabletsLocation.Builder locationBuilder = PTabletsLocation.newBuilder()
                                                        .setHost(backend.getHost())
                                                        .setBrpcPort(backend.getBrpcPort());
            PTabletsLocation location = locationBuilder.addAllTabletId(tabletIds).build();
            locations.add(location);
        }
        PFetchRemoteSchemaRequest.Builder requestBuilder = PFetchRemoteSchemaRequest.newBuilder()
                                                                    .addAllTabletLocation(locations)
                                                                    .setIsCoordinator(true);
        // send rpc to coordinatorBackend util succeed or 2 times
        for (Backend be : coordinatorBackend) {
            try {
                PFetchRemoteSchemaRequest request = requestBuilder.build();
                Future<PFetchRemoteSchemaResponse> future = BackendServiceProxy.getInstance()
                                            .fetchRemoteTabletSchemaAsync(be.getBrpcAdress(), request);
                PFetchRemoteSchemaResponse response = null;
                try {
                    response = future.get(60, TimeUnit.SECONDS);
                    TStatusCode code = TStatusCode.findByValue(response.getStatus().getStatusCode());
                    String errMsg;
                    if (code != TStatusCode.OK) {
                        if (!response.getStatus().getErrorMsgsList().isEmpty()) {
                            errMsg = response.getStatus().getErrorMsgsList().get(0);
                        } else {
                            errMsg = "fetchRemoteTabletSchemaAsync failed. backend address: "
                                    + be.getHost() + " : " + be.getBrpcPort();
                        }
                        throw new AnalysisException(errMsg);
                    }
                    fillColumns(response);
                    break;
                } catch (AnalysisException e) {
                    // continue to get result
                    LOG.warn(e);
                } catch (InterruptedException e) {
                    // continue to get result
                    LOG.warn("fetch remote schema future get interrupted Exception");
                } catch (TimeoutException e) {
                    future.cancel(true);
                    // continue to get result
                    LOG.warn("fetch remote schema result timeout, addr {}", be.getBrpcAdress());
                }
            } catch (RpcException e) {
                LOG.warn("fetch remote schema result rpc exception {}, e {}", be.getBrpcAdress(), e);
            } catch (ExecutionException e) {
                LOG.warn("fetch remote schema result execution exception {}, addr {}", e, be.getBrpcAdress());
            }
        }
        return columns;
    }

    private void fillColumns(PFetchRemoteSchemaResponse response) throws AnalysisException {
        TabletSchemaPB schemaPB = response.getMergedSchema();
        for (ColumnPB columnPB : schemaPB.getColumnList()) {
            try {
                Column remoteColumn = initColumnFromPB(columnPB);
                columns.add(remoteColumn);
            } catch (Exception e) {
                throw new AnalysisException("default value to string failed");
            }
        }
        int variantColumntIdx = 0;
        for (Column column : columns) {
            variantColumntIdx++;
            if (column.getType().isVariantType()) {
                break;
            }
        }
        if (variantColumntIdx == columns.size()) {
            return;
        }
        List<Column> subList = columns.subList(variantColumntIdx, columns.size());
        Collections.sort(subList, new Comparator<Column>() {
            @Override
            public int compare(Column c1, Column c2) {
                return c1.getName().compareTo(c2.getName());
            }
        });
    }

    private Column initColumnFromPB(ColumnPB column) throws AnalysisException {
        try {
            AggregateType aggType = getAggTypeFromAggName(column.getAggregation());
            Type type = getTypeFromTypeName(column.getType());
            String columnName = column.getName();
            boolean isKey = column.getIsKey();
            boolean isNullable = column.getIsNullable();
            String defaultValue = column.getDefaultValue().toString("UTF-8");
            if (defaultValue.equals("")) {
                defaultValue = "NULL";
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

    private Type getTypeFromTypeName(String typeName) {
        Type type;
        if (typeName.equals("TINYINT")) {
            type = Type.TINYINT;
        } else if (typeName.equals("SMALLINT")) {
            type = Type.SMALLINT;
        } else if (typeName.equals("INT")) {
            type = Type.INT;
        } else if (typeName.equals("BIGINT")) {
            type = Type.BIGINT;
        } else if (typeName.equals("LARGEINT")) {
            type = Type.LARGEINT;
        } else if (typeName.equals("UNSIGNED_TINYINT")) {
            type = Type.BIGINT;
        } else if (typeName.equals("UNSIGNED_SMALLINT")) {
            type = Type.UNSUPPORTED;
        } else if (typeName.equals("UNSIGNED_INT")) {
            type = Type.UNSUPPORTED;
        } else if (typeName.equals("UNSIGNED_BIGINT")) {
            type = Type.UNSUPPORTED;
        } else if (typeName.equals("FLOAT")) {
            type = Type.FLOAT;
        } else if (typeName.equals("DISCRETE_DOUBLE")) {
            type = Type.DOUBLE;
        } else if (typeName.equals("DOUBLE")) {
            type = Type.DOUBLE;
        } else if (typeName.equals("CHAR")) {
            type = Type.CHAR;
        } else if (typeName.equals("DATE")) {
            type = Type.DATE;
        } else if (typeName.equals("DATEV2")) {
            type = Type.DATEV2;
        } else if (typeName.equals("DATETIMEV2")) {
            type = Type.DATETIMEV2;
        } else if (typeName.equals("DATETIME")) {
            type = Type.DATETIME;
        } else if (typeName.equals("DECIMAL32")) {
            type = Type.DECIMAL32;
        } else if (typeName.equals("DECIMAL64")) {
            type = Type.DECIMAL64;
        } else if (typeName.equals("DECIMAL128I")) {
            type = Type.DECIMAL128;
        } else if (typeName.equals("DECIMAL")) {
            type = Type.DECIMALV2;
        } else if (typeName.equals("VARCHAR")) {
            type = Type.VARCHAR;
        } else if (typeName.equals("STRING")) {
            type = Type.STRING;
        } else if (typeName.equals("JSONB")) {
            type = Type.JSONB;
        } else if (typeName.equals("VARIANT")) {
            type = Type.VARIANT;
        } else if (typeName.equals("BOOLEAN")) {
            type = Type.BOOLEAN;
        } else if (typeName.equals("HLL")) {
            type = Type.HLL;
        } else if (typeName.equals("STRUCT")) {
            type = Type.STRUCT;
        } else if (typeName.equals("LIST")) {
            type = Type.UNSUPPORTED;
        } else if (typeName.equals("MAP")) {
            type = Type.MAP;
        } else if (typeName.equals("OBJECT")) {
            type = Type.UNSUPPORTED;
        } else if (typeName.equals("ARRAY")) {
            type = Type.ARRAY;
        } else if (typeName.equals("QUANTILE_STATE")) {
            type = Type.QUANTILE_STATE;
        } else if (typeName.equals("AGG_STATE")) {
            type = Type.AGG_STATE;
        } else {
            type = Type.UNSUPPORTED;
        }
        return type;
    }

    private AggregateType getAggTypeFromAggName(String aggName) {
        AggregateType aggType;
        if (aggName.equals("NONE")) {
            aggType = AggregateType.NONE;
        } else if (aggName.equals("SUM")) {
            aggType = AggregateType.SUM;
        } else if (aggName.equals("MIN")) {
            aggType = AggregateType.MIN;
        } else if (aggName.equals("MAX")) {
            aggType = AggregateType.MAX;
        } else if (aggName.equals("REPLACE")) {
            aggType = AggregateType.REPLACE;
        } else if (aggName.equals("REPLACE_IF_NOT_NULL")) {
            aggType = AggregateType.REPLACE_IF_NOT_NULL;
        } else if (aggName.equals("HLL_UNION")) {
            aggType = AggregateType.HLL_UNION;
        } else if (aggName.equals("BITMAP_UNION")) {
            aggType = AggregateType.BITMAP_UNION;
        } else if (aggName.equals("QUANTILE_UNION")) {
            aggType = AggregateType.QUANTILE_UNION;
        } else if (!aggName.isEmpty()) {
            aggType = AggregateType.GENERIC_AGGREGATION;
        } else {
            aggType = AggregateType.NONE;
        }
        return aggType;
    }
}
