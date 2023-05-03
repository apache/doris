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

package org.apache.doris.tablefunction;

import org.apache.doris.alter.DecommissionType;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TBackendsMetadataParams;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TIcebergMetadataParams;
import org.apache.doris.thrift.TIcebergQueryType;
import org.apache.doris.thrift.TMetadataTableRequestParams;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetadataGenerator {
    private static final Logger LOG = LogManager.getLogger(MetadataGenerator.class);

    public static TFetchSchemaTableDataResult getMetadataTable(TFetchSchemaTableDataRequest request) {
        if (!request.isSetMetadaTableParams()) {
            return errorResult("Metadata table params is not set. ");
        }
        TFetchSchemaTableDataResult result;
        TMetadataTableRequestParams params = request.getMetadaTableParams();
        switch (request.getMetadaTableParams().getMetadataType()) {
            case ICEBERG:
                result = icebergMetadataResult(params);
                break;
            case BACKENDS:
                result = backendsMetadataResult(params);
                break;
            case RESOURCE_GROUPS:
                result = resourceGroupsMetadataResult(params);
                break;
            default:
                return errorResult("Metadata table params is not set.");
        }
        if (result.getStatus().getStatusCode() == TStatusCode.OK) {
            filterColumns(result, params.getColumnsName(), params.getMetadataType());
        }
        return result;
    }

    @NotNull
    public static TFetchSchemaTableDataResult errorResult(String msg) {
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        result.setStatus(new TStatus(TStatusCode.INTERNAL_ERROR));
        result.status.addToErrorMsgs(msg);
        return result;
    }

    private static TFetchSchemaTableDataResult icebergMetadataResult(TMetadataTableRequestParams params) {
        if (!params.isSetIcebergMetadataParams()) {
            return errorResult("Iceberg metadata params is not set.");
        }
        TIcebergMetadataParams icebergMetadataParams =  params.getIcebergMetadataParams();
        HMSExternalCatalog catalog = (HMSExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                .getCatalog(icebergMetadataParams.getCatalog());
        org.apache.iceberg.Table table;
        try {
            table = getIcebergTable(catalog, icebergMetadataParams.getDatabase(), icebergMetadataParams.getTable());
        } catch (MetaNotFoundException e) {
            return errorResult(e.getMessage());
        }
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        TIcebergQueryType icebergQueryType = icebergMetadataParams.getIcebergQueryType();
        switch (icebergQueryType) {
            case SNAPSHOTS:
                for (Snapshot snapshot : table.snapshots()) {
                    TRow trow = new TRow();
                    LocalDateTime committedAt = LocalDateTime.ofInstant(Instant.ofEpochMilli(
                            snapshot.timestampMillis()), TimeUtils.getTimeZone().toZoneId());
                    long encodedDatetime = convertToDateTimeV2(committedAt.getYear(), committedAt.getMonthValue(),
                            committedAt.getDayOfMonth(), committedAt.getHour(),
                            committedAt.getMinute(), committedAt.getSecond());

                    trow.addToColumnValue(new TCell().setLongVal(encodedDatetime));
                    trow.addToColumnValue(new TCell().setLongVal(snapshot.snapshotId()));
                    if (snapshot.parentId() == null) {
                        trow.addToColumnValue(new TCell().setLongVal(-1L));
                    } else {
                        trow.addToColumnValue(new TCell().setLongVal(snapshot.parentId()));
                    }
                    trow.addToColumnValue(new TCell().setStringVal(snapshot.operation()));
                    trow.addToColumnValue(new TCell().setStringVal(snapshot.manifestListLocation()));

                    dataBatch.add(trow);
                }
                break;
            default:
                return errorResult("Unsupported iceberg inspect type: " + icebergQueryType);
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult backendsMetadataResult(TMetadataTableRequestParams params) {
        if (!params.isSetBackendsMetadataParams()) {
            return errorResult("backends metadata param is not set.");
        }
        TBackendsMetadataParams backendsParam = params.getBackendsMetadataParams();
        final SystemInfoService clusterInfoService = Env.getCurrentSystemInfo();
        List<Long> backendIds = null;
        if (!Strings.isNullOrEmpty(backendsParam.cluster_name)) {
            final Cluster cluster = Env.getCurrentEnv().getCluster(backendsParam.cluster_name);
            // root not in any cluster
            if (null == cluster) {
                return errorResult("Cluster is not existed.");
            }
            backendIds = cluster.getBackendIdList();
        } else {
            backendIds = clusterInfoService.getBackendIds(false);
        }

        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        long start = System.currentTimeMillis();
        Stopwatch watch = Stopwatch.createUnstarted();

        List<TRow> dataBatch = Lists.newArrayList();
        for (long backendId : backendIds) {
            Backend backend = clusterInfoService.getBackend(backendId);
            if (backend == null) {
                continue;
            }

            watch.start();
            Integer tabletNum = Env.getCurrentInvertedIndex().getTabletNumByBackendId(backendId);
            watch.stop();

            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setLongVal(backendId));
            trow.addToColumnValue(new TCell().setStringVal(backend.getOwnerClusterName()));
            trow.addToColumnValue(new TCell().setStringVal(backend.getIp()));
            if (backend.getHostName() != null) {
                trow.addToColumnValue(new TCell().setStringVal(backend.getHostName()));
            } else {
                trow.addToColumnValue(new TCell().setStringVal(backend.getIp()));
            }
            if (Strings.isNullOrEmpty(backendsParam.cluster_name)) {
                trow.addToColumnValue(new TCell().setIntVal(backend.getHeartbeatPort()));
                trow.addToColumnValue(new TCell().setIntVal(backend.getBePort()));
                trow.addToColumnValue(new TCell().setIntVal(backend.getHttpPort()));
                trow.addToColumnValue(new TCell().setIntVal(backend.getBrpcPort()));
            }
            trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(backend.getLastStartTime())));
            trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(backend.getLastUpdateMs())));
            trow.addToColumnValue(new TCell().setStringVal(String.valueOf(backend.isAlive())));
            if (backend.isDecommissioned() && backend.getDecommissionType() == DecommissionType.ClusterDecommission) {
                trow.addToColumnValue(new TCell().setStringVal("false"));
                trow.addToColumnValue(new TCell().setStringVal("true"));
            } else if (backend.isDecommissioned()
                    && backend.getDecommissionType() == DecommissionType.SystemDecommission) {
                trow.addToColumnValue(new TCell().setStringVal("true"));
                trow.addToColumnValue(new TCell().setStringVal("false"));
            } else {
                trow.addToColumnValue(new TCell().setStringVal("false"));
                trow.addToColumnValue(new TCell().setStringVal("false"));
            }
            trow.addToColumnValue(new TCell().setLongVal(tabletNum));

            // capacity
            // data used
            trow.addToColumnValue(new TCell().setLongVal(backend.getDataUsedCapacityB()));

            // available
            long availB = backend.getAvailableCapacityB();
            trow.addToColumnValue(new TCell().setLongVal(availB));

            // total
            long totalB = backend.getTotalCapacityB();
            trow.addToColumnValue(new TCell().setLongVal(totalB));

            // used percent
            double used = 0.0;
            if (totalB <= 0) {
                used = 0.0;
            } else {
                used = (double) (totalB - availB) * 100 / totalB;
            }
            trow.addToColumnValue(new TCell().setDoubleVal(used));
            trow.addToColumnValue(new TCell().setDoubleVal(backend.getMaxDiskUsedPct() * 100));

            // remote used capacity
            trow.addToColumnValue(new TCell().setLongVal(backend.getRemoteUsedCapacityB()));

            // tags
            trow.addToColumnValue(new TCell().setStringVal(backend.getTagMapString()));
            // err msg
            trow.addToColumnValue(new TCell().setStringVal(backend.getHeartbeatErrMsg()));
            // version
            trow.addToColumnValue(new TCell().setStringVal(backend.getVersion()));
            // status
            trow.addToColumnValue(new TCell().setStringVal(new Gson().toJson(backend.getBackendStatus())));
            // heartbeat failure counter
            trow.addToColumnValue(new TCell().setIntVal(backend.getHeartbeatFailureCounter()));

            // node role, show the value only when backend is alive.
            trow.addToColumnValue(new TCell().setStringVal(backend.isAlive() ? backend.getNodeRoleTag().value : ""));

            dataBatch.add(trow);
        }

        // backends proc node get result too slow, add log to observer.
        LOG.debug("backends proc get tablet num cost: {}, total cost: {}",
                watch.elapsed(TimeUnit.MILLISECONDS), (System.currentTimeMillis() - start));

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult resourceGroupsMetadataResult(TMetadataTableRequestParams params) {
        List<List<String>> resourceGroupsInfo = Env.getCurrentEnv().getResourceGroupMgr()
                .getResourcesInfo();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        for (List<String> rGroupsInfo : resourceGroupsInfo) {
            TRow trow = new TRow();
            Long id = Long.valueOf(rGroupsInfo.get(0));
            int value = Integer.valueOf(rGroupsInfo.get(3));
            trow.addToColumnValue(new TCell().setLongVal(id));
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(1)));
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(2)));
            trow.addToColumnValue(new TCell().setIntVal(value));
            dataBatch.add(trow);
        }

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static void filterColumns(TFetchSchemaTableDataResult result,
            List<String> columnNames, TMetadataType type) {
        List<TRow> fullColumnsRow = result.getDataBatch();
        List<TRow> filterColumnsRows = Lists.newArrayList();
        for (TRow row : fullColumnsRow) {
            TRow filterRow = new TRow();
            for (String columnName : columnNames) {
                Integer index = 0;
                switch (type) {
                    case ICEBERG:
                        index = IcebergTableValuedFunction.getColumnIndexFromColumnName(columnName);
                        break;
                    case BACKENDS:
                        index = BackendsTableValuedFunction.getColumnIndexFromColumnName(columnName);
                        break;
                    case RESOURCE_GROUPS:
                        index = ResourceGroupsTableValuedFunction.getColumnIndexFromColumnName(columnName);
                        break;
                    default:
                        break;
                }
                filterRow.addToColumnValue(row.getColumnValue().get(index));
            }
            filterColumnsRows.add(filterRow);
        }
        result.setDataBatch(filterColumnsRows);
    }

    private static org.apache.iceberg.Table getIcebergTable(HMSExternalCatalog catalog, String db, String tbl)
            throws MetaNotFoundException {
        org.apache.iceberg.hive.HiveCatalog hiveCatalog = new org.apache.iceberg.hive.HiveCatalog();
        Configuration conf = new HdfsConfiguration();
        Map<String, String> properties = catalog.getCatalogProperty().getHadoopProperties();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        hiveCatalog.setConf(conf);
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put(HMSProperties.HIVE_METASTORE_URIS, catalog.getHiveMetastoreUris());
        catalogProperties.put("uri", catalog.getHiveMetastoreUris());
        hiveCatalog.initialize("hive", catalogProperties);
        return hiveCatalog.loadTable(TableIdentifier.of(db, tbl));
    }

    private static long convertToDateTimeV2(int year, int month, int day, int hour, int minute, int second) {
        return (long) second << 20 | (long) minute << 26 | (long) hour << 32
                | (long) day << 37 | (long) month << 42 | (long) year << 46;
    }
}
