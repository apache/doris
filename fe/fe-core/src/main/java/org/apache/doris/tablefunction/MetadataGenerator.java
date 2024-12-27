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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SchemaTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.FrontendsProcNode;
import org.apache.doris.common.proc.PartitionsProcDir;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hudi.source.HudiMetadataCacheMgr;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergMetadataCache;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.extensions.mtmv.MTMVJob;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.plsql.metastore.PlsqlManager;
import org.apache.doris.plsql.metastore.PlsqlProcedureKey;
import org.apache.doris.plsql.metastore.PlsqlStoredProcedure;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QeProcessorImpl.QueryInfo;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TBackendsMetadataParams;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TIcebergMetadataParams;
import org.apache.doris.thrift.TIcebergQueryType;
import org.apache.doris.thrift.TJobsMetadataParams;
import org.apache.doris.thrift.TMaterializedViewsMetadataParams;
import org.apache.doris.thrift.TMetadataTableRequestParams;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPartitionValuesMetadataParams;
import org.apache.doris.thrift.TPartitionsMetadataParams;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TSchemaTableRequestParams;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTasksMetadataParams;
import org.apache.doris.thrift.TUserIdentity;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.iceberg.Snapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetadataGenerator {
    private static final Logger LOG = LogManager.getLogger(MetadataGenerator.class);

    private static final ImmutableMap<String, Integer> ACTIVE_QUERIES_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> WORKLOAD_GROUPS_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> ROUTINE_INFO_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> WORKLOAD_SCHED_POLICY_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> TABLE_OPTIONS_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> WORKLOAD_GROUP_PRIVILEGES_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> TABLE_PROPERTIES_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> META_CACHE_STATS_COLUMN_TO_INDEX;

    private static final ImmutableMap<String, Integer> PARTITIONS_COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> activeQueriesbuilder = new ImmutableMap.Builder();
        List<Column> activeQueriesColList = SchemaTable.TABLE_MAP.get("active_queries").getFullSchema();
        for (int i = 0; i < activeQueriesColList.size(); i++) {
            activeQueriesbuilder.put(activeQueriesColList.get(i).getName().toLowerCase(), i);
        }
        ACTIVE_QUERIES_COLUMN_TO_INDEX = activeQueriesbuilder.build();

        ImmutableMap.Builder<String, Integer> workloadGroupBuilder = new ImmutableMap.Builder();
        for (int i = 0; i < WorkloadGroupMgr.WORKLOAD_GROUP_PROC_NODE_TITLE_NAMES.size(); i++) {
            workloadGroupBuilder.put(WorkloadGroupMgr.WORKLOAD_GROUP_PROC_NODE_TITLE_NAMES.get(i).toLowerCase(), i);
        }
        WORKLOAD_GROUPS_COLUMN_TO_INDEX = workloadGroupBuilder.build();

        ImmutableMap.Builder<String, Integer> routineInfoBuilder = new ImmutableMap.Builder();
        for (int i = 0; i < PlsqlManager.ROUTINE_INFO_TITLE_NAMES.size(); i++) {
            routineInfoBuilder.put(PlsqlManager.ROUTINE_INFO_TITLE_NAMES.get(i).toLowerCase(), i);
        }
        ROUTINE_INFO_COLUMN_TO_INDEX = routineInfoBuilder.build();

        ImmutableMap.Builder<String, Integer> policyBuilder = new ImmutableMap.Builder();
        List<Column> policyColList = SchemaTable.TABLE_MAP.get("workload_policy").getFullSchema();
        for (int i = 0; i < policyColList.size(); i++) {
            policyBuilder.put(policyColList.get(i).getName().toLowerCase(), i);
        }
        WORKLOAD_SCHED_POLICY_COLUMN_TO_INDEX = policyBuilder.build();

        ImmutableMap.Builder<String, Integer> optionBuilder = new ImmutableMap.Builder();
        List<Column> optionColList = SchemaTable.TABLE_MAP.get("table_options").getFullSchema();
        for (int i = 0; i < optionColList.size(); i++) {
            optionBuilder.put(optionColList.get(i).getName().toLowerCase(), i);
        }
        TABLE_OPTIONS_COLUMN_TO_INDEX = optionBuilder.build();

        ImmutableMap.Builder<String, Integer> wgPrivsBuilder = new ImmutableMap.Builder();
        List<Column> wgPrivsColList = SchemaTable.TABLE_MAP.get("workload_group_privileges").getFullSchema();
        for (int i = 0; i < wgPrivsColList.size(); i++) {
            wgPrivsBuilder.put(wgPrivsColList.get(i).getName().toLowerCase(), i);
        }
        WORKLOAD_GROUP_PRIVILEGES_COLUMN_TO_INDEX = wgPrivsBuilder.build();

        ImmutableMap.Builder<String, Integer> propertiesBuilder = new ImmutableMap.Builder();
        List<Column> propertiesColList = SchemaTable.TABLE_MAP.get("table_properties").getFullSchema();
        for (int i = 0; i < propertiesColList.size(); i++) {
            propertiesBuilder.put(propertiesColList.get(i).getName().toLowerCase(), i);
        }
        TABLE_PROPERTIES_COLUMN_TO_INDEX = propertiesBuilder.build();

        ImmutableMap.Builder<String, Integer> metaCacheBuilder = new ImmutableMap.Builder();
        List<Column> metaCacheColList = SchemaTable.TABLE_MAP.get("catalog_meta_cache_statistics").getFullSchema();
        for (int i = 0; i < metaCacheColList.size(); i++) {
            metaCacheBuilder.put(metaCacheColList.get(i).getName().toLowerCase(), i);
        }
        META_CACHE_STATS_COLUMN_TO_INDEX = metaCacheBuilder.build();

        ImmutableMap.Builder<String, Integer> partitionsBuilder = new ImmutableMap.Builder();
        List<Column> partitionsColList = SchemaTable.TABLE_MAP.get("partitions").getFullSchema();
        for (int i = 0; i < partitionsColList.size(); i++) {
            partitionsBuilder.put(partitionsColList.get(i).getName().toLowerCase(), i);
        }
        PARTITIONS_COLUMN_TO_INDEX = partitionsBuilder.build();
    }

    public static TFetchSchemaTableDataResult getMetadataTable(TFetchSchemaTableDataRequest request) throws TException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getMetadataTable() start.");
        }
        if (!request.isSetMetadaTableParams() || !request.getMetadaTableParams().isSetMetadataType()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Metadata table params is not set.");
            }
            return errorResult("Metadata table params is not set. ");
        }
        TFetchSchemaTableDataResult result;
        TMetadataTableRequestParams params = request.getMetadaTableParams();
        TMetadataType metadataType = request.getMetadaTableParams().getMetadataType();
        switch (metadataType) {
            case ICEBERG:
                result = icebergMetadataResult(params);
                break;
            case BACKENDS:
                result = backendsMetadataResult(params);
                break;
            case FRONTENDS:
                result = frontendsMetadataResult(params);
                break;
            case FRONTENDS_DISKS:
                result = frontendsDisksMetadataResult(params);
                break;
            case CATALOGS:
                result = catalogsMetadataResult(params);
                break;
            case MATERIALIZED_VIEWS:
                result = mtmvMetadataResult(params);
                break;
            case PARTITIONS:
                result = partitionMetadataResult(params);
                break;
            case JOBS:
                result = jobMetadataResult(params);
                break;
            case TASKS:
                result = taskMetadataResult(params);
                break;
            case PARTITION_VALUES:
                result = partitionValuesMetadataResult(params);
                break;
            default:
                return errorResult("Metadata table params is not set.");
        }
        if (result.getStatus().getStatusCode() == TStatusCode.OK) {
            if (metadataType != TMetadataType.PARTITION_VALUES) {
                // partition_values' result already sorted by column names
                filterColumns(result, params.getColumnsName(), params.getMetadataType(), params);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("getMetadataTable() end.");
        }
        return result;
    }

    public static TFetchSchemaTableDataResult getSchemaTableData(TFetchSchemaTableDataRequest request)
            throws TException {
        if (!request.isSetSchemaTableParams()) {
            return errorResult("schema table params is not set.");
        }
        TFetchSchemaTableDataResult result;
        TSchemaTableRequestParams schemaTableParams = request.getSchemaTableParams();
        ImmutableMap<String, Integer> columnIndex;
        switch (request.getSchemaTableName()) {
            case ACTIVE_QUERIES:
                result = queriesMetadataResult(schemaTableParams, request);
                columnIndex = ACTIVE_QUERIES_COLUMN_TO_INDEX;
                break;
            case WORKLOAD_GROUPS:
                result = workloadGroupsMetadataResult(schemaTableParams);
                columnIndex = WORKLOAD_GROUPS_COLUMN_TO_INDEX;
                break;
            case ROUTINES_INFO:
                result = routineInfoMetadataResult(schemaTableParams);
                columnIndex = ROUTINE_INFO_COLUMN_TO_INDEX;
                break;
            case WORKLOAD_SCHEDULE_POLICY:
                result = workloadSchedPolicyMetadataResult(schemaTableParams);
                columnIndex = WORKLOAD_SCHED_POLICY_COLUMN_TO_INDEX;
                break;
            case TABLE_OPTIONS:
                result = tableOptionsMetadataResult(schemaTableParams);
                columnIndex = TABLE_OPTIONS_COLUMN_TO_INDEX;
                break;
            case WORKLOAD_GROUP_PRIVILEGES:
                result = workloadGroupPrivsMetadataResult(schemaTableParams);
                columnIndex = WORKLOAD_GROUP_PRIVILEGES_COLUMN_TO_INDEX;
                break;
            case TABLE_PROPERTIES:
                result = tablePropertiesMetadataResult(schemaTableParams);
                columnIndex = TABLE_PROPERTIES_COLUMN_TO_INDEX;
                break;
            case CATALOG_META_CACHE_STATS:
                result = metaCacheStatsMetadataResult(schemaTableParams);
                columnIndex = META_CACHE_STATS_COLUMN_TO_INDEX;
                break;
            case PARTITIONS:
                result = partitionsMetadataResult(schemaTableParams);
                columnIndex = PARTITIONS_COLUMN_TO_INDEX;
                break;
            default:
                return errorResult("invalid schema table name.");
        }
        if (schemaTableParams.isSetColumnsName() && result.getStatus().getStatusCode() == TStatusCode.OK) {
            filterColumns(result, schemaTableParams.getColumnsName(), columnIndex);
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

        TIcebergMetadataParams icebergMetadataParams = params.getIcebergMetadataParams();
        TIcebergQueryType icebergQueryType = icebergMetadataParams.getIcebergQueryType();
        IcebergMetadataCache icebergMetadataCache = Env.getCurrentEnv().getExtMetaCacheMgr().getIcebergMetadataCache();
        List<TRow> dataBatch = Lists.newArrayList();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();

        switch (icebergQueryType) {
            case SNAPSHOTS:
                List<Snapshot> snapshotList;
                try {
                    snapshotList = icebergMetadataCache.getSnapshotList(icebergMetadataParams);
                } catch (UserException e) {
                    return errorResult(e.getMessage());
                }
                for (Snapshot snapshot : snapshotList) {
                    TRow trow = new TRow();
                    LocalDateTime committedAt = LocalDateTime.ofInstant(Instant.ofEpochMilli(
                            snapshot.timestampMillis()), TimeUtils.getTimeZone().toZoneId());
                    long encodedDatetime = TimeUtils.convertToDateTimeV2(committedAt.getYear(),
                            committedAt.getMonthValue(),
                            committedAt.getDayOfMonth(), committedAt.getHour(), committedAt.getMinute(),
                            committedAt.getSecond(), committedAt.getNano() / 1000);

                    trow.addToColumnValue(new TCell().setLongVal(encodedDatetime));
                    trow.addToColumnValue(new TCell().setLongVal(snapshot.snapshotId()));
                    if (snapshot.parentId() == null) {
                        trow.addToColumnValue(new TCell().setLongVal(-1L));
                    } else {
                        trow.addToColumnValue(new TCell().setLongVal(snapshot.parentId()));
                    }
                    trow.addToColumnValue(new TCell().setStringVal(snapshot.operation()));
                    trow.addToColumnValue(new TCell().setStringVal(snapshot.manifestListLocation()));
                    trow.addToColumnValue(new TCell().setStringVal(new Gson().toJson(snapshot.summary())));

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
        final SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        List<Long> backendIds = systemInfoService.getAllBackendIds(false);

        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        long start = System.currentTimeMillis();
        Stopwatch watch = Stopwatch.createUnstarted();

        List<TRow> dataBatch = Lists.newArrayList();
        for (long backendId : backendIds) {
            Backend backend = systemInfoService.getBackend(backendId);
            if (backend == null) {
                continue;
            }

            watch.start();
            Integer tabletNum = Env.getCurrentInvertedIndex().getTabletNumByBackendId(backendId);
            watch.stop();

            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setLongVal(backendId));
            trow.addToColumnValue(new TCell().setStringVal(backend.getHost()));
            if (Strings.isNullOrEmpty(backendsParam.cluster_name)) {
                trow.addToColumnValue(new TCell().setIntVal(backend.getHeartbeatPort()));
                trow.addToColumnValue(new TCell().setIntVal(backend.getBePort()));
                trow.addToColumnValue(new TCell().setIntVal(backend.getHttpPort()));
                trow.addToColumnValue(new TCell().setIntVal(backend.getBrpcPort()));
                trow.addToColumnValue(new TCell().setIntVal(backend.getArrowFlightSqlPort()));
            }
            trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(backend.getLastStartTime())));
            trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(backend.getLastUpdateMs())));
            trow.addToColumnValue(new TCell().setBoolVal(backend.isAlive()));
            trow.addToColumnValue(new TCell().setBoolVal(backend.isDecommissioned()));
            trow.addToColumnValue(new TCell().setLongVal(tabletNum));

            // capacity
            // data used
            trow.addToColumnValue(new TCell().setLongVal(backend.getDataUsedCapacityB()));

            // trash used
            trow.addToColumnValue(new TCell().setLongVal(backend.getTrashUsedCapacityB()));

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
        if (LOG.isDebugEnabled()) {
            LOG.debug("backends proc get tablet num cost: {}, total cost: {}",
                    watch.elapsed(TimeUnit.MILLISECONDS), (System.currentTimeMillis() - start));
        }

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult frontendsMetadataResult(TMetadataTableRequestParams params) {
        if (!params.isSetFrontendsMetadataParams()) {
            return errorResult("frontends metadata param is not set.");
        }

        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();

        List<TRow> dataBatch = Lists.newArrayList();
        List<List<String>> infos = Lists.newArrayList();
        FrontendsProcNode.getFrontendsInfo(Env.getCurrentEnv(), infos);
        for (List<String> info : infos) {
            TRow trow = new TRow();
            for (String item : info) {
                trow.addToColumnValue(new TCell().setStringVal(item));
            }
            dataBatch.add(trow);
        }

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult frontendsDisksMetadataResult(TMetadataTableRequestParams params) {
        if (!params.isSetFrontendsMetadataParams()) {
            return errorResult("frontends metadata param is not set.");
        }

        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();

        List<TRow> dataBatch = Lists.newArrayList();
        List<List<String>> infos = Lists.newArrayList();
        FrontendsProcNode.getFrontendsDiskInfo(Env.getCurrentEnv(), infos);
        for (List<String> info : infos) {
            TRow trow = new TRow();
            for (String item : info) {
                trow.addToColumnValue(new TCell().setStringVal(item));
            }
            dataBatch.add(trow);
        }

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult catalogsMetadataResult(TMetadataTableRequestParams params) {
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();

        UserIdentity currentUserIdentity = UserIdentity.fromThrift(params.getCurrentUserIdent());
        List<CatalogIf> info = Env.getCurrentEnv().getCatalogMgr().listCatalogsWithCheckPriv(currentUserIdentity);
        List<TRow> dataBatch = Lists.newArrayList();
        for (CatalogIf catalog : info) {
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setLongVal(catalog.getId()));
            trow.addToColumnValue(new TCell().setStringVal(catalog.getName()));
            trow.addToColumnValue(new TCell().setStringVal(catalog.getType()));

            Map<String, String> properties = CatalogMgr.getCatalogPropertiesWithPrintable(catalog);
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                TRow subTrow = new TRow(trow);
                subTrow.addToColumnValue(new TCell().setStringVal(entry.getKey()));
                subTrow.addToColumnValue(new TCell().setStringVal(entry.getValue()));
                dataBatch.add(subTrow);
            }
            if (properties.isEmpty()) {
                trow.addToColumnValue(new TCell().setStringVal("NULL"));
                trow.addToColumnValue(new TCell().setStringVal("NULL"));
                dataBatch.add(trow);
            }
        }

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult workloadGroupsMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }

        TUserIdentity tcurrentUserIdentity = params.getCurrentUserIdent();
        List<List<String>> workloadGroupsInfo = Env.getCurrentEnv().getWorkloadGroupMgr()
                .getResourcesInfo(tcurrentUserIdentity);
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        for (List<String> rGroupsInfo : workloadGroupsInfo) {
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(0)))); // id
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(1))); // name
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(2)))); // cpu_share
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(3))); // mem_limit
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(4))); // mem overcommit
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(5)))); // max concurrent
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(6)))); // max queue size
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(7)))); // queue timeout
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(8))); // cpu hard limit
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(9)))); // scan thread num
            // max remote scan thread num
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(10))));
            // min remote scan thread num
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(11))));
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(12))); // spill low watermark
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(13))); // spill high watermark
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(14))); // tag
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(15)))); // read bytes per second
            trow.addToColumnValue(
                    new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(16)))); // remote read bytes per second
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(17))); // running query num
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(18))); // waiting query num
            dataBatch.add(trow);
        }

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult workloadSchedPolicyMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }

        TUserIdentity tcurrentUserIdentity = params.getCurrentUserIdent();
        List<List<String>> workloadPolicyList = Env.getCurrentEnv().getWorkloadSchedPolicyMgr()
                .getWorkloadSchedPolicyTvfInfo(tcurrentUserIdentity);
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        for (List<String> policyRow : workloadPolicyList) {
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(policyRow.get(0)))); // id
            trow.addToColumnValue(new TCell().setStringVal(policyRow.get(1))); // name
            trow.addToColumnValue(new TCell().setStringVal(policyRow.get(2))); // condition
            trow.addToColumnValue(new TCell().setStringVal(policyRow.get(3))); // action
            trow.addToColumnValue(new TCell().setIntVal(Integer.valueOf(policyRow.get(4)))); // priority
            trow.addToColumnValue(new TCell().setBoolVal(Boolean.valueOf(policyRow.get(5)))); // enabled
            trow.addToColumnValue(new TCell().setIntVal(Integer.valueOf(policyRow.get(6)))); // version
            trow.addToColumnValue(new TCell().setStringVal(policyRow.get(7))); // workload group id
            dataBatch.add(trow);
        }

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult workloadGroupPrivsMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }
        UserIdentity currentUserIdentity = UserIdentity.fromThrift(params.getCurrentUserIdent());

        List<List<String>> rows = new ArrayList<>();
        Env.getCurrentEnv().getAuth().getUserRoleWorkloadGroupPrivs(rows, currentUserIdentity);
        List<TRow> dataBatch = Lists.newArrayList();
        for (List<String> privRow : rows) {
            TRow trow = new TRow();
            String workloadGroupName = privRow.get(1);
            trow.addToColumnValue(new TCell().setStringVal(privRow.get(0))); // GRANTEE
            trow.addToColumnValue(new TCell().setStringVal(workloadGroupName)); // WORKLOAD_GROUP_NAME
            trow.addToColumnValue(new TCell().setStringVal(privRow.get(2))); // PRIVILEGE_TYPE
            trow.addToColumnValue(new TCell().setStringVal(privRow.get(3))); // IS_GRANTABLE
            dataBatch.add(trow);
        }
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult queriesMetadataResult(TSchemaTableRequestParams tSchemaTableParams,
            TFetchSchemaTableDataRequest parentRequest) {
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();

        String selfNode = Env.getCurrentEnv().getSelfNode().getHost();
        if (ConnectContext.get() != null && !Strings.isNullOrEmpty(ConnectContext.get().getCurrentConnectedFEIp())) {
            selfNode = ConnectContext.get().getCurrentConnectedFEIp();
        }
        selfNode = NetUtils.getHostnameByIp(selfNode);

        List<TRow> dataBatch = Lists.newArrayList();
        Map<String, QueryInfo> queryInfoMap = QeProcessorImpl.INSTANCE.getQueryInfoMap();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (Map.Entry<String, QueryInfo> entry : queryInfoMap.entrySet()) {
            String queryId = entry.getKey();
            QueryInfo queryInfo = entry.getValue();

            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setStringVal(queryId));

            long queryStartTime = queryInfo.getStartExecTime();
            if (queryStartTime > 0) {
                trow.addToColumnValue(new TCell().setStringVal(sdf.format(new Date(queryStartTime))));
                trow.addToColumnValue(
                        new TCell().setLongVal(System.currentTimeMillis() - queryInfo.getStartExecTime()));
            } else {
                trow.addToColumnValue(new TCell());
                trow.addToColumnValue(new TCell().setLongVal(-1));
            }

            List<TPipelineWorkloadGroup> tgroupList = queryInfo.getCoord().gettWorkloadGroups();
            if (tgroupList != null && tgroupList.size() == 1) {
                trow.addToColumnValue(new TCell().setLongVal(tgroupList.get(0).id));
            } else {
                trow.addToColumnValue(new TCell().setLongVal(-1));
            }

            if (queryInfo.getConnectContext() != null) {
                trow.addToColumnValue(new TCell().setStringVal(queryInfo.getConnectContext().getDatabase()));
            } else {
                trow.addToColumnValue(new TCell().setStringVal(""));
            }
            trow.addToColumnValue(new TCell().setStringVal(selfNode));

            long queueStartTime = queryInfo.getQueueStartTime();
            if (queueStartTime > 0) {
                trow.addToColumnValue(new TCell().setStringVal(sdf.format(new Date(queueStartTime))));
            } else {
                trow.addToColumnValue(new TCell());
            }

            long queueEndTime = queryInfo.getQueueEndTime();
            if (queueEndTime > 0) {
                trow.addToColumnValue(new TCell().setStringVal(sdf.format(new Date(queueEndTime))));
            } else {
                trow.addToColumnValue(new TCell());
            }

            String queueMsg = queryInfo.getQueueStatus();
            trow.addToColumnValue(new TCell().setStringVal(queueMsg));

            trow.addToColumnValue(new TCell().setStringVal(queryInfo.getSql()));
            dataBatch.add(trow);
        }

        /* Get the query results from other FE also */
        if (tSchemaTableParams.isReplayToOtherFe()) {
            TSchemaTableRequestParams replaySchemaTableParams = new TSchemaTableRequestParams(tSchemaTableParams);
            replaySchemaTableParams.setReplayToOtherFe(false);

            TFetchSchemaTableDataRequest replayFetchSchemaTableReq = new TFetchSchemaTableDataRequest(parentRequest);
            replayFetchSchemaTableReq.setSchemaTableParams(replaySchemaTableParams);

            List<TFetchSchemaTableDataResult> relayResults = forwardToOtherFrontends(replayFetchSchemaTableReq);
            relayResults
                    .forEach(rs -> rs.getDataBatch()
                            .forEach(row -> dataBatch.add(row)));
        }

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static List<TFetchSchemaTableDataResult> forwardToOtherFrontends(TFetchSchemaTableDataRequest request) {
        List<TFetchSchemaTableDataResult> results = new ArrayList<>();
        List<Pair<String, Integer>> frontends = FrontendsProcNode.getFrontendWithRpcPort(Env.getCurrentEnv(), false);

        FrontendService.Client client = null;
        int waitTimeOut = ConnectContext.get() == null ? 300 : ConnectContext.get().getExecTimeout();
        for (Pair<String, Integer> fe : frontends) {
            TNetworkAddress thriftAddress = new TNetworkAddress(fe.key(), fe.value());
            try {
                client = ClientPool.frontendPool.borrowObject(thriftAddress, waitTimeOut * 1000);
            } catch (Exception e) {
                LOG.warn("Failed to get frontend {} client. exception: {}", fe.key(), e);
                continue;
            }

            boolean isReturnToPool = false;
            try {
                TFetchSchemaTableDataResult result = client.fetchSchemaTableData(request);
                results.add(result);
                isReturnToPool = true;
            } catch (Exception e) {
                LOG.warn("Failed to finish forward fetch operation to fe: {} . exception: {}", fe.key(), e);
            } finally {
                if (isReturnToPool) {
                    ClientPool.frontendPool.returnObject(thriftAddress, client);
                } else {
                    ClientPool.frontendPool.invalidateObject(thriftAddress, client);
                }
            }
        }

        return results;
    }

    private static void filterColumns(TFetchSchemaTableDataResult result,
            List<String> columnNames, TMetadataType type, TMetadataTableRequestParams params) throws TException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("filterColumns() start.");
        }
        List<TRow> fullColumnsRow = result.getDataBatch();
        List<TRow> filterColumnsRows = Lists.newArrayList();
        for (TRow row : fullColumnsRow) {
            TRow filterRow = new TRow();
            try {
                for (String columnName : columnNames) {
                    Integer index = MetadataTableValuedFunction.getColumnIndexFromColumnName(type, columnName, params);
                    filterRow.addToColumnValue(row.getColumnValue().get(index));
                }
            } catch (AnalysisException e) {
                throw new TException(e);
            }
            filterColumnsRows.add(filterRow);
        }
        result.setDataBatch(filterColumnsRows);
        if (LOG.isDebugEnabled()) {
            LOG.debug("filterColumns() end.");
        }
    }

    private static void filterColumns(TFetchSchemaTableDataResult result,
            List<String> columnNames,
            ImmutableMap<String, Integer> columnIndex) throws TException {
        List<TRow> fullColumnsRow = result.getDataBatch();
        List<TRow> filterColumnsRows = Lists.newArrayList();
        for (TRow row : fullColumnsRow) {
            TRow filterRow = new TRow();
            try {
                for (String columnName : columnNames) {
                    Integer index = columnIndex.get(columnName.toLowerCase());
                    filterRow.addToColumnValue(row.getColumnValue().get(index));
                }
            } catch (Throwable e) {
                LOG.info("error happens when filter columns.", e);
                throw new TException(e);
            }
            filterColumnsRows.add(filterRow);
        }
        result.setDataBatch(filterColumnsRows);
    }

    private static TFetchSchemaTableDataResult mtmvMetadataResult(TMetadataTableRequestParams params) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("mtmvMetadataResult() start");
        }
        if (!params.isSetMaterializedViewsMetadataParams()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("MaterializedViews metadata params is not set.");
            }
            return errorResult("MaterializedViews metadata params is not set.");
        }

        TMaterializedViewsMetadataParams mtmvMetadataParams = params.getMaterializedViewsMetadataParams();
        String dbName = mtmvMetadataParams.getDatabase();
        if (LOG.isDebugEnabled()) {
            LOG.debug("dbName: " + dbName);
        }
        TUserIdentity currentUserIdent = mtmvMetadataParams.getCurrentUserIdent();
        UserIdentity userIdentity = UserIdentity.fromThrift(currentUserIdent);
        if (LOG.isDebugEnabled()) {
            LOG.debug("userIdentity: " + userIdentity);
        }
        List<TRow> dataBatch = Lists.newArrayList();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<Table> tables;
        try {
            tables = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalogOrAnalysisException(InternalCatalog.INTERNAL_CATALOG_NAME)
                    .getDbOrAnalysisException(dbName).getTables();
        } catch (AnalysisException e) {
            LOG.warn(e.getMessage());
            return errorResult(e.getMessage());
        }

        for (Table table : tables) {
            if (table instanceof MTMV) {
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(userIdentity, InternalCatalog.INTERNAL_CATALOG_NAME,
                                table.getQualifiedDbName(), table.getName(),
                                PrivPredicate.SHOW)) {
                    continue;
                }
                MTMV mv = (MTMV) table;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("mv: {}", mv.toInfoString());
                }
                List<TableIf> needLocked = Lists.newArrayList();
                needLocked.add(mv);
                boolean alwaysNotSync = false;
                try {
                    for (BaseTableInfo baseTableInfo : mv.getRelation().getBaseTables()) {
                        TableIf baseTable = MTMVUtil.getTable(baseTableInfo);
                        needLocked.add(baseTable);
                    }
                } catch (Exception e) {
                    alwaysNotSync = true;
                }
                needLocked.sort(Comparator.comparing(TableIf::getId));
                MetaLockUtils.readLockTables(needLocked);
                boolean isSync;
                try {
                    isSync = !alwaysNotSync && MTMVPartitionUtil.isMTMVSync(mv);
                } finally {
                    MetaLockUtils.readUnlockTables(needLocked);
                }
                MTMVStatus mtmvStatus = mv.getStatus();
                TRow trow = new TRow();
                trow.addToColumnValue(new TCell().setLongVal(mv.getId()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getName()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getJobInfo().getJobName()));
                trow.addToColumnValue(new TCell().setStringVal(mtmvStatus.getState().name()));
                trow.addToColumnValue(new TCell().setStringVal(mtmvStatus.getSchemaChangeDetail()));
                trow.addToColumnValue(new TCell().setStringVal(mtmvStatus.getRefreshState().name()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getRefreshInfo().toString()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getQuerySql()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getMvProperties().toString()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getMvPartitionInfo().toNameString()));
                trow.addToColumnValue(new TCell().setBoolVal(isSync));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("mv end: {}", mv.getName());
                }
                dataBatch.add(trow);
            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        if (LOG.isDebugEnabled()) {
            LOG.debug("mtmvMetadataResult() end");
        }
        return result;
    }

    private static TFetchSchemaTableDataResult partitionMetadataResult(TMetadataTableRequestParams params) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("partitionMetadataResult() start");
        }
        if (!params.isSetPartitionsMetadataParams()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Partitions metadata params is not set.");
            }
            return errorResult("Partitions metadata params is not set.");
        }

        TPartitionsMetadataParams partitionsMetadataParams = params.getPartitionsMetadataParams();
        String catalogName = partitionsMetadataParams.getCatalog();
        if (LOG.isDebugEnabled()) {
            LOG.debug("catalogName: " + catalogName);
        }
        String dbName = partitionsMetadataParams.getDatabase();
        if (LOG.isDebugEnabled()) {
            LOG.debug("dbName: " + dbName);
        }
        String tableName = partitionsMetadataParams.getTable();
        if (LOG.isDebugEnabled()) {
            LOG.debug("tableName: " + tableName);
        }

        CatalogIf catalog;
        TableIf table;
        DatabaseIf db;
        try {
            catalog = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalogOrAnalysisException(catalogName);
            db = catalog.getDbOrAnalysisException(dbName);
            table = db.getTableOrAnalysisException(tableName);
        } catch (AnalysisException e) {
            LOG.warn(e.getMessage());
            return errorResult(e.getMessage());
        }

        if (catalog instanceof InternalCatalog) {
            return dealInternalCatalog((Database) db, table);
        } else if (catalog instanceof MaxComputeExternalCatalog) {
            return dealMaxComputeCatalog((MaxComputeExternalCatalog) catalog, dbName, tableName);
        } else if (catalog instanceof HMSExternalCatalog) {
            return dealHMSCatalog((HMSExternalCatalog) catalog, dbName, tableName);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("partitionMetadataResult() end");
        }
        return errorResult("not support catalog: " + catalogName);
    }

    private static TFetchSchemaTableDataResult dealHMSCatalog(HMSExternalCatalog catalog, String dbName,
            String tableName) {
        List<TRow> dataBatch = Lists.newArrayList();
        List<String> partitionNames = catalog.getClient().listPartitionNames(dbName, tableName);
        for (String partition : partitionNames) {
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setStringVal(partition));
            dataBatch.add(trow);
        }
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult dealMaxComputeCatalog(MaxComputeExternalCatalog catalog, String dbName,
            String tableName) {
        List<TRow> dataBatch = Lists.newArrayList();
        List<String> partitionNames = catalog.listPartitionNames(dbName, tableName);
        for (String partition : partitionNames) {
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setStringVal(partition));
            dataBatch.add(trow);
        }
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult dealInternalCatalog(Database db, TableIf table) {
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        if (!(table instanceof OlapTable)) {
            return errorResult("not olap table");
        }
        PartitionsProcDir dir = new PartitionsProcDir(db, (OlapTable) table, false);
        try {
            List<TRow> dataBatch = dir.getPartitionInfosForTvf();
            result.setDataBatch(dataBatch);
        } catch (AnalysisException e) {
            return errorResult(e.getMessage());
        }
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult jobMetadataResult(TMetadataTableRequestParams params) {
        if (!params.isSetJobsMetadataParams()) {
            return errorResult("Jobs metadata params is not set.");
        }

        TJobsMetadataParams jobsMetadataParams = params.getJobsMetadataParams();
        String type = jobsMetadataParams.getType();
        JobType jobType = JobType.valueOf(type);
        TUserIdentity currentUserIdent = jobsMetadataParams.getCurrentUserIdent();
        UserIdentity userIdentity = UserIdentity.fromThrift(currentUserIdent);
        List<TRow> dataBatch = Lists.newArrayList();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();

        List<org.apache.doris.job.base.AbstractJob> jobList = Env.getCurrentEnv().getJobManager().queryJobs(jobType);

        for (org.apache.doris.job.base.AbstractJob job : jobList) {
            if (job instanceof MTMVJob) {
                MTMVJob mtmvJob = (MTMVJob) job;
                if (!mtmvJob.hasPriv(userIdentity, PrivPredicate.SHOW)) {
                    continue;
                }
            }
            dataBatch.add(job.getTvfInfo());
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult taskMetadataResult(TMetadataTableRequestParams params) {
        if (!params.isSetTasksMetadataParams()) {
            return errorResult("Tasks metadata params is not set.");
        }

        TTasksMetadataParams tasksMetadataParams = params.getTasksMetadataParams();
        String type = tasksMetadataParams.getType();
        JobType jobType = JobType.valueOf(type);
        TUserIdentity currentUserIdent = tasksMetadataParams.getCurrentUserIdent();
        UserIdentity userIdentity = UserIdentity.fromThrift(currentUserIdent);
        List<TRow> dataBatch = Lists.newArrayList();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();

        List<org.apache.doris.job.base.AbstractJob> jobList = Env.getCurrentEnv().getJobManager().queryJobs(jobType);

        for (org.apache.doris.job.base.AbstractJob job : jobList) {
            if (job instanceof MTMVJob) {
                MTMVJob mtmvJob = (MTMVJob) job;
                if (!mtmvJob.hasPriv(userIdentity, PrivPredicate.SHOW)) {
                    continue;
                }
            }
            List<AbstractTask> tasks = job.queryAllTasks();
            for (AbstractTask task : tasks) {
                TRow tvfInfo = task.getTvfInfo(job.getJobName());
                if (tvfInfo != null) {
                    dataBatch.add(tvfInfo);
                }
            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult routineInfoMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }

        PlsqlManager plSqlClient = Env.getCurrentEnv().getPlsqlManager();

        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();

        Map<PlsqlProcedureKey, PlsqlStoredProcedure> allProc = plSqlClient.getAllPlsqlStoredProcedures();
        for (Map.Entry<PlsqlProcedureKey, PlsqlStoredProcedure> entry : allProc.entrySet()) {
            PlsqlStoredProcedure proc = entry.getValue();
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setStringVal(proc.getName())); // SPECIFIC_NAME
            trow.addToColumnValue(new TCell().setStringVal(Long.toString(proc.getCatalogId()))); // ROUTINE_CATALOG
            CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(proc.getCatalogId());
            if (catalog != null) {
                DatabaseIf db = catalog.getDbNullable(proc.getDbId());
                if (db != null) {
                    trow.addToColumnValue(new TCell().setStringVal(db.getFullName())); // ROUTINE_SCHEMA
                } else {
                    trow.addToColumnValue(new TCell().setStringVal("")); // ROUTINE_SCHEMA
                }
            } else {
                trow.addToColumnValue(new TCell().setStringVal("")); // ROUTINE_SCHEMA
            }
            trow.addToColumnValue(new TCell().setStringVal(proc.getName())); // ROUTINE_NAME
            trow.addToColumnValue(new TCell().setStringVal("PROCEDURE")); // ROUTINE_TYPE
            trow.addToColumnValue(new TCell().setStringVal("")); // DTD_IDENTIFIER
            trow.addToColumnValue(new TCell().setStringVal(proc.getSource())); // ROUTINE_BODY
            trow.addToColumnValue(new TCell().setStringVal("")); // ROUTINE_DEFINITION
            trow.addToColumnValue(new TCell().setStringVal("NULL")); // EXTERNAL_NAME
            trow.addToColumnValue(new TCell().setStringVal("")); // EXTERNAL_LANGUAGE
            trow.addToColumnValue(new TCell().setStringVal("SQL")); // PARAMETER_STYLE
            trow.addToColumnValue(new TCell().setStringVal("")); // IS_DETERMINISTIC
            trow.addToColumnValue(new TCell().setStringVal("")); // SQL_DATA_ACCESS
            trow.addToColumnValue(new TCell().setStringVal("NULL")); // SQL_PATH
            trow.addToColumnValue(new TCell().setStringVal("DEFINER")); // SECURITY_TYPE
            trow.addToColumnValue(new TCell().setStringVal(proc.getCreateTime())); // CREATED
            trow.addToColumnValue(new TCell().setStringVal(proc.getModifyTime())); // LAST_ALTERED
            trow.addToColumnValue(new TCell().setStringVal("")); // SQ_MODE
            trow.addToColumnValue(new TCell().setStringVal("")); // ROUTINE_COMMENT
            trow.addToColumnValue(new TCell().setStringVal(proc.getOwnerName())); // DEFINER
            trow.addToColumnValue(new TCell().setStringVal("")); // CHARACTER_SET_CLIENT
            trow.addToColumnValue(new TCell().setStringVal("")); // COLLATION_CONNECTION
            trow.addToColumnValue(new TCell().setStringVal("")); // DATABASE_COLLATION
            dataBatch.add(trow);
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static void tableOptionsForInternalCatalog(UserIdentity currentUserIdentity,
                    CatalogIf catalog, DatabaseIf database, List<TableIf> tables, List<TRow> dataBatch) {
        for (TableIf table : tables) {
            if (!(table instanceof OlapTable)) {
                continue;
            }
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(currentUserIdentity, catalog.getName(),
                    database.getFullName(), table.getName(), PrivPredicate.SHOW)) {
                continue;
            }
            OlapTable olapTable = (OlapTable) table;
            olapTable.readLock();
            try {
                TRow trow = new TRow();
                trow.addToColumnValue(new TCell().setStringVal(catalog.getName())); // TABLE_CATALOG
                trow.addToColumnValue(new TCell().setStringVal(database.getFullName())); // TABLE_SCHEMA
                trow.addToColumnValue(new TCell().setStringVal(table.getName())); // TABLE_NAME
                trow.addToColumnValue(
                        new TCell().setStringVal(olapTable.getKeysType().toMetadata())); // TABLE_MODEL
                trow.addToColumnValue(
                        new TCell().setStringVal(olapTable.getKeyColAsString())); // key columTypes

                DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
                if (distributionInfo.getType() == DistributionInfoType.HASH) {
                    HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                    List<Column> distributionColumns = hashDistributionInfo.getDistributionColumns();
                    StringBuilder distributeKey = new StringBuilder();
                    for (Column c : distributionColumns) {
                        if (distributeKey.length() != 0) {
                            distributeKey.append(",");
                        }
                        distributeKey.append(c.getName());
                    }
                    if (distributeKey.length() == 0) {
                        trow.addToColumnValue(new TCell().setStringVal(""));
                    } else {
                        trow.addToColumnValue(
                                new TCell().setStringVal(distributeKey.toString()));
                    }
                    trow.addToColumnValue(new TCell().setStringVal("HASH")); // DISTRIBUTE_TYPE
                } else {
                    trow.addToColumnValue(new TCell().setStringVal("RANDOM")); // DISTRIBUTE_KEY
                    trow.addToColumnValue(new TCell().setStringVal("RANDOM")); // DISTRIBUTE_TYPE
                }
                trow.addToColumnValue(new TCell().setIntVal(distributionInfo.getBucketNum())); // BUCKETS_NUM
                trow.addToColumnValue(new TCell().setIntVal(olapTable.getPartitionNum())); // PARTITION_NUM
                dataBatch.add(trow);
            } finally {
                olapTable.readUnlock();
            }
        }
    }

    private static void tableOptionsForExternalCatalog(UserIdentity currentUserIdentity,
                    CatalogIf catalog, DatabaseIf database, List<TableIf> tables, List<TRow> dataBatch) {
        for (TableIf table : tables) {
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(currentUserIdentity, catalog.getName(),
                    database.getFullName(), table.getName(), PrivPredicate.SHOW)) {
                continue;
            }
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setStringVal(catalog.getName())); // TABLE_CATALOG
            trow.addToColumnValue(new TCell().setStringVal(database.getFullName())); // TABLE_SCHEMA
            trow.addToColumnValue(new TCell().setStringVal(table.getName())); // TABLE_NAME
            trow.addToColumnValue(
                    new TCell().setStringVal("")); // TABLE_MODEL
            trow.addToColumnValue(
                    new TCell().setStringVal("")); // key columTypes
            trow.addToColumnValue(new TCell().setStringVal("")); // DISTRIBUTE_KEY
            trow.addToColumnValue(new TCell().setStringVal("")); // DISTRIBUTE_TYPE
            trow.addToColumnValue(new TCell().setIntVal(0)); // BUCKETS_NUM
            trow.addToColumnValue(new TCell().setIntVal(0)); // PARTITION_NUM
            dataBatch.add(trow);
        }
    }

    private static TFetchSchemaTableDataResult tableOptionsMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }
        if (!params.isSetDbId()) {
            return errorResult("current db id is not set.");
        }

        if (!params.isSetCatalog()) {
            return errorResult("current catalog is not set.");
        }

        TUserIdentity tcurrentUserIdentity = params.getCurrentUserIdent();
        UserIdentity currentUserIdentity = UserIdentity.fromThrift(tcurrentUserIdentity);
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        Long dbId = params.getDbId();
        String clg = params.getCatalog();
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(clg);
        if (catalog == null) {
            // catalog is NULL  let return empty to BE
            result.setDataBatch(dataBatch);
            result.setStatus(new TStatus(TStatusCode.OK));
            return result;
        }
        DatabaseIf database = catalog.getDbNullable(dbId);
        if (database == null) {
            // BE gets the database id list from FE and then invokes this interface
            // per database. there is a chance that in between database can be dropped.
            // so need to handle database not exist case and return ok so that BE continue the
            // loop with next database.
            result.setDataBatch(dataBatch);
            result.setStatus(new TStatus(TStatusCode.OK));
            return result;
        }
        List<TableIf> tables = database.getTables();
        if (catalog instanceof InternalCatalog) {
            tableOptionsForInternalCatalog(currentUserIdentity, catalog, database, tables, dataBatch);
        } else if (catalog instanceof ExternalCatalog) {
            tableOptionsForExternalCatalog(currentUserIdentity, catalog, database, tables, dataBatch);
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static void tablePropertiesForInternalCatalog(UserIdentity currentUserIdentity,
                CatalogIf catalog, DatabaseIf database, List<TableIf> tables, List<TRow> dataBatch) {
        for (TableIf table : tables) {
            if (!(table instanceof OlapTable)) {
                continue;
            }
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(currentUserIdentity, catalog.getName(),
                    database.getFullName(), table.getName(), PrivPredicate.SHOW)) {
                continue;
            }
            OlapTable olapTable = (OlapTable) table;
            olapTable.readLock();
            try {
                TableProperty property = olapTable.getTableProperty();
                if (property == null) {
                    // if there is no properties, then write empty properties and check next table.
                    TRow trow = new TRow();
                    trow.addToColumnValue(new TCell().setStringVal(catalog.getName())); // TABLE_CATALOG
                    trow.addToColumnValue(new TCell().setStringVal(database.getFullName())); // TABLE_SCHEMA
                    trow.addToColumnValue(new TCell().setStringVal(table.getName())); // TABLE_NAME
                    trow.addToColumnValue(new TCell().setStringVal("")); // PROPERTIES_NAME
                    trow.addToColumnValue(new TCell().setStringVal("")); // PROPERTIES_VALUE
                    dataBatch.add(trow);
                    continue;
                }

                Map<String, String>  propertiesMap = property.getProperties();
                propertiesMap.forEach((key, value) -> {
                    TRow trow = new TRow();
                    trow.addToColumnValue(new TCell().setStringVal(catalog.getName())); // TABLE_CATALOG
                    trow.addToColumnValue(new TCell().setStringVal(database.getFullName())); // TABLE_SCHEMA
                    trow.addToColumnValue(new TCell().setStringVal(table.getName())); // TABLE_NAME
                    trow.addToColumnValue(new TCell().setStringVal(key)); // PROPERTIES_NAME
                    trow.addToColumnValue(new TCell().setStringVal(value)); // PROPERTIES_VALUE
                    dataBatch.add(trow);
                });
            } finally {
                olapTable.readUnlock();
            }
        } // for table
    }

    private static void tablePropertiesForExternalCatalog(UserIdentity currentUserIdentity,
            CatalogIf catalog, DatabaseIf database, List<TableIf> tables, List<TRow> dataBatch) {
        for (TableIf table : tables) {
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(currentUserIdentity, catalog.getName(),
                    database.getFullName(), table.getName(), PrivPredicate.SHOW)) {
                continue;
            }
            // Currently for external catalog, we put properties as empty, can extend in future
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setStringVal(catalog.getName())); // TABLE_CATALOG
            trow.addToColumnValue(new TCell().setStringVal(database.getFullName())); // TABLE_SCHEMA
            trow.addToColumnValue(new TCell().setStringVal(table.getName())); // TABLE_NAME
            trow.addToColumnValue(new TCell().setStringVal("")); // PROPERTIES_NAME
            trow.addToColumnValue(new TCell().setStringVal("")); // PROPERTIES_VALUE
            dataBatch.add(trow);
        } // for table
    }

    private static TFetchSchemaTableDataResult tablePropertiesMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }

        if (!params.isSetDbId()) {
            return errorResult("current db id is not set.");
        }

        if (!params.isSetCatalog()) {
            return errorResult("current catalog is not set.");
        }

        TUserIdentity tcurrentUserIdentity = params.getCurrentUserIdent();
        UserIdentity currentUserIdentity = UserIdentity.fromThrift(tcurrentUserIdentity);
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        Long dbId = params.getDbId();
        String clg = params.getCatalog();
        List<TRow> dataBatch = Lists.newArrayList();
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(clg);
        if (catalog == null) {
            // catalog is NULL  let return empty to BE
            result.setDataBatch(dataBatch);
            result.setStatus(new TStatus(TStatusCode.OK));
            return result;
        }
        DatabaseIf database = catalog.getDbNullable(dbId);
        if (database == null) {
            // BE gets the database id list from FE and then invokes this interface
            // per database. there is a chance that in between database can be dropped.
            // so need to handle database not exist case and return ok so that BE continue the
            // loop with next database.
            result.setDataBatch(dataBatch);
            result.setStatus(new TStatus(TStatusCode.OK));
            return result;
        }
        List<TableIf> tables = database.getTables();
        if (catalog instanceof InternalCatalog) {
            tablePropertiesForInternalCatalog(currentUserIdentity, catalog, database, tables, dataBatch);
        } else if (catalog instanceof ExternalCatalog) {
            tablePropertiesForExternalCatalog(currentUserIdentity, catalog, database, tables, dataBatch);
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult metaCacheStatsMetadataResult(TSchemaTableRequestParams params) {
        List<TRow> dataBatch = Lists.newArrayList();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        ExternalMetaCacheMgr mgr = Env.getCurrentEnv().getExtMetaCacheMgr();
        for (CatalogIf catalogIf : Env.getCurrentEnv().getCatalogMgr().getCopyOfCatalog()) {
            if (catalogIf instanceof HMSExternalCatalog) {
                HMSExternalCatalog catalog = (HMSExternalCatalog) catalogIf;
                // 1. hive metastore cache
                HiveMetaStoreCache cache = mgr.getMetaStoreCache(catalog);
                if (cache != null) {
                    fillBatch(dataBatch, cache.getStats(), catalog.getName());
                }
                // 2. hudi cache
                HudiMetadataCacheMgr hudiMetadataCacheMgr = mgr.getHudiMetadataCacheMgr();
                fillBatch(dataBatch, hudiMetadataCacheMgr.getCacheStats(catalog), catalog.getName());
            } else if (catalogIf instanceof IcebergExternalCatalog) {
                // 3. iceberg cache
                IcebergMetadataCache icebergCache = mgr.getIcebergMetadataCache();
                fillBatch(dataBatch, icebergCache.getCacheStats(), catalogIf.getName());
            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static void partitionsForInternalCatalog(UserIdentity currentUserIdentity,
                CatalogIf catalog, DatabaseIf database, List<TableIf> tables, List<TRow> dataBatch) {
        for (TableIf table : tables) {
            if (!(table instanceof OlapTable)) {
                continue;
            }
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(currentUserIdentity, catalog.getName(),
                    database.getFullName(), table.getName(), PrivPredicate.SHOW)) {
                continue;
            }

            OlapTable olapTable = (OlapTable) table;
            olapTable.readLock();
            try {
                Collection<Partition> allPartitions = olapTable.getAllPartitions();

                for (Partition partition : allPartitions) {
                    TRow trow = new TRow();
                    trow.addToColumnValue(new TCell().setStringVal(catalog.getName())); // TABLE_CATALOG
                    trow.addToColumnValue(new TCell().setStringVal(database.getFullName())); // TABLE_SCHEMA
                    trow.addToColumnValue(new TCell().setStringVal(table.getName())); // TABLE_NAME
                    trow.addToColumnValue(new TCell().setStringVal(partition.getName())); // PARTITION_NAME
                    trow.addToColumnValue(new TCell().setStringVal("NULL")); // SUBPARTITION_NAME (always null)

                    trow.addToColumnValue(new TCell().setIntVal(0)); //PARTITION_ORDINAL_POSITION (not available)
                    trow.addToColumnValue(new TCell().setIntVal(0)); //SUBPARTITION_ORDINAL_POSITION (not available)
                    trow.addToColumnValue(new TCell().setStringVal(
                            olapTable.getPartitionInfo().getType().toString())); // PARTITION_METHOD
                    trow.addToColumnValue(new TCell().setStringVal("NULL")); // SUBPARTITION_METHOD(always null)
                    PartitionItem item = olapTable.getPartitionInfo().getItem(partition.getId());
                    if ((olapTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED) || (item == null)) {
                        trow.addToColumnValue(new TCell().setStringVal("NULL")); // if unpartitioned, its null
                        trow.addToColumnValue(
                                new TCell().setStringVal("NULL")); // SUBPARTITION_EXPRESSION (always null)
                        trow.addToColumnValue(new TCell().setStringVal("NULL")); // PARITION DESC, its null
                    } else {
                        trow.addToColumnValue(new TCell().setStringVal(
                                olapTable.getPartitionInfo()
                                    .getDisplayPartitionColumns().toString())); // PARTITION_EXPRESSION
                        trow.addToColumnValue(
                                new TCell().setStringVal("NULL")); // SUBPARTITION_EXPRESSION (always null)
                        trow.addToColumnValue(new TCell().setStringVal(
                                item.getItemsSql())); // PARITION DESC
                    }
                    trow.addToColumnValue(new TCell().setLongVal(partition.getRowCount())); //TABLE_ROWS (PARTITION row)
                    trow.addToColumnValue(new TCell().setLongVal(partition.getAvgRowLength())); //AVG_ROW_LENGTH
                    trow.addToColumnValue(new TCell().setLongVal(partition.getDataLength())); //DATA_LENGTH
                    trow.addToColumnValue(new TCell().setIntVal(0)); //MAX_DATA_LENGTH (not available)
                    trow.addToColumnValue(new TCell().setIntVal(0)); //INDEX_LENGTH (not available)
                    trow.addToColumnValue(new TCell().setIntVal(0)); //DATA_FREE (not available)
                    trow.addToColumnValue(new TCell().setStringVal("NULL")); //CREATE_TIME (not available)
                    trow.addToColumnValue(new TCell().setStringVal(
                            TimeUtils.longToTimeString(partition.getVisibleVersionTime()))); //UPDATE_TIME
                    trow.addToColumnValue(new TCell().setStringVal("NULL")); // CHECK_TIME (not available)
                    trow.addToColumnValue(new TCell().setIntVal(0)); //CHECKSUM (not available)
                    trow.addToColumnValue(new TCell().setStringVal("")); // PARTITION_COMMENT (not available)
                    trow.addToColumnValue(new TCell().setStringVal("")); // NODEGROUP (not available)
                    trow.addToColumnValue(new TCell().setStringVal("")); // TABLESPACE_NAME (not available)
                    dataBatch.add(trow);
                }
            } finally {
                olapTable.readUnlock();
            }
        } // for table
    }

    private static void partitionsForExternalCatalog(UserIdentity currentUserIdentity,
            CatalogIf catalog, DatabaseIf database, List<TableIf> tables, List<TRow> dataBatch) {
        for (TableIf table : tables) {
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(currentUserIdentity, catalog.getName(),
                    database.getFullName(), table.getName(), PrivPredicate.SHOW)) {
                continue;
            }
            // TODO
        } // for table
    }

    private static TFetchSchemaTableDataResult partitionsMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }

        if (!params.isSetDbId()) {
            return errorResult("current db id is not set.");
        }

        if (!params.isSetCatalog()) {
            return errorResult("current catalog is not set.");
        }

        TUserIdentity tcurrentUserIdentity = params.getCurrentUserIdent();
        UserIdentity currentUserIdentity = UserIdentity.fromThrift(tcurrentUserIdentity);
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        Long dbId = params.getDbId();
        String clg = params.getCatalog();
        List<TRow> dataBatch = Lists.newArrayList();
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(clg);
        if (catalog == null) {
            // catalog is NULL  let return empty to BE
            result.setDataBatch(dataBatch);
            result.setStatus(new TStatus(TStatusCode.OK));
            return result;
        }
        DatabaseIf database = catalog.getDbNullable(dbId);
        if (database == null) {
            // BE gets the database id list from FE and then invokes this interface
            // per database. there is a chance that in between database can be dropped.
            // so need to handle database not exist case and return ok so that BE continue the
            // loop with next database.
            result.setDataBatch(dataBatch);
            result.setStatus(new TStatus(TStatusCode.OK));
            return result;
        }
        List<TableIf> tables = database.getTables();
        if (catalog instanceof InternalCatalog) {
            // only olap tables
            partitionsForInternalCatalog(currentUserIdentity, catalog, database, tables, dataBatch);
        } else if (catalog instanceof ExternalCatalog) {
            partitionsForExternalCatalog(currentUserIdentity, catalog, database, tables, dataBatch);
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static void fillBatch(List<TRow> dataBatch, Map<String, Map<String, String>> stats,
            String catalogName) {
        for (Map.Entry<String, Map<String, String>> entry : stats.entrySet()) {
            String cacheName = entry.getKey();
            Map<String, String> cacheStats = entry.getValue();
            for (Map.Entry<String, String> cacheStatsEntry : cacheStats.entrySet()) {
                String metricName = cacheStatsEntry.getKey();
                String metricValue = cacheStatsEntry.getValue();
                TRow trow = new TRow();
                trow.addToColumnValue(new TCell().setStringVal(catalogName)); // CATALOG_NAME
                trow.addToColumnValue(new TCell().setStringVal(cacheName)); // CACHE_NAME
                trow.addToColumnValue(new TCell().setStringVal(metricName)); // METRIC_NAME
                trow.addToColumnValue(new TCell().setStringVal(metricValue)); // METRIC_VALUE
                dataBatch.add(trow);
            }
        }
    }

    private static TFetchSchemaTableDataResult partitionValuesMetadataResult(TMetadataTableRequestParams params) {
        if (!params.isSetPartitionValuesMetadataParams()) {
            return errorResult("partition values metadata params is not set.");
        }

        TPartitionValuesMetadataParams metaParams = params.getPartitionValuesMetadataParams();
        String ctlName = metaParams.getCatalog();
        String dbName = metaParams.getDatabase();
        String tblName = metaParams.getTable();
        List<TRow> dataBatch;
        try {
            TableIf table = PartitionValuesTableValuedFunction.analyzeAndGetTable(ctlName, dbName, tblName, false);
            TableType tableType = table.getType();
            switch (tableType) {
                case HMS_EXTERNAL_TABLE:
                    dataBatch = partitionValuesMetadataResultForHmsTable((HMSExternalTable) table,
                            params.getColumnsName());
                    break;
                default:
                    return errorResult("not support table type " + tableType.name());
            }
            TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
            result.setDataBatch(dataBatch);
            result.setStatus(new TStatus(TStatusCode.OK));
            return result;
        } catch (Throwable t) {
            LOG.warn("error when get partition values metadata. {}.{}.{}", ctlName, dbName, tblName, t);
            return errorResult("error when get partition values metadata: " + Util.getRootCauseMessage(t));
        }
    }

    private static List<TRow> partitionValuesMetadataResultForHmsTable(HMSExternalTable tbl, List<String> colNames)
            throws AnalysisException {
        List<Column> partitionCols = tbl.getPartitionColumns();
        List<Integer> colIdxs = Lists.newArrayList();
        List<Type> types = Lists.newArrayList();
        for (String colName : colNames) {
            for (int i = 0; i < partitionCols.size(); ++i) {
                if (partitionCols.get(i).getName().equalsIgnoreCase(colName)) {
                    colIdxs.add(i);
                    types.add(partitionCols.get(i).getType());
                }
            }
        }
        if (colIdxs.size() != colNames.size()) {
            throw new AnalysisException(
                    "column " + colNames + " does not match partition columns of table " + tbl.getName());
        }

        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) tbl.getCatalog());
        HiveMetaStoreCache.HivePartitionValues hivePartitionValues = cache.getPartitionValues(
                tbl.getDbName(), tbl.getName(), tbl.getPartitionColumnTypes());
        Map<Long, List<String>> valuesMap = hivePartitionValues.getPartitionValuesMap();
        List<TRow> dataBatch = Lists.newArrayList();
        for (Map.Entry<Long, List<String>> entry : valuesMap.entrySet()) {
            TRow trow = new TRow();
            List<String> values = entry.getValue();
            if (values.size() != partitionCols.size()) {
                continue;
            }

            for (int i = 0; i < colIdxs.size(); ++i) {
                int idx = colIdxs.get(i);
                String partitionValue = values.get(idx);
                if (partitionValue == null || partitionValue.equals(TablePartitionValues.HIVE_DEFAULT_PARTITION)) {
                    trow.addToColumnValue(new TCell().setIsNull(true));
                } else {
                    Type type = types.get(i);
                    switch (type.getPrimitiveType()) {
                        case BOOLEAN:
                            trow.addToColumnValue(new TCell().setBoolVal(Boolean.valueOf(partitionValue)));
                            break;
                        case TINYINT:
                        case SMALLINT:
                        case INT:
                            trow.addToColumnValue(new TCell().setIntVal(Integer.valueOf(partitionValue)));
                            break;
                        case BIGINT:
                            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(partitionValue)));
                            break;
                        case FLOAT:
                            trow.addToColumnValue(new TCell().setDoubleVal(Float.valueOf(partitionValue)));
                            break;
                        case DOUBLE:
                            trow.addToColumnValue(new TCell().setDoubleVal(Double.valueOf(partitionValue)));
                            break;
                        case VARCHAR:
                        case CHAR:
                        case STRING:
                            trow.addToColumnValue(new TCell().setStringVal(partitionValue));
                            break;
                        case DATE:
                        case DATEV2:
                            trow.addToColumnValue(
                                    new TCell().setLongVal(TimeUtils.convertStringToDateV2(partitionValue)));
                            break;
                        case DATETIME:
                        case DATETIMEV2:
                            trow.addToColumnValue(
                                    new TCell().setLongVal(TimeUtils.convertStringToDateTimeV2(partitionValue,
                                            ((ScalarType) type).getScalarScale())));
                            break;
                        default:
                            throw new AnalysisException(
                                    "Unsupported partition column type for $partitions sys table " + type);
                    }
                }
            }
            dataBatch.add(trow);
        }
        return dataBatch;
    }

}

