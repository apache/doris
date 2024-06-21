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
import org.apache.doris.catalog.SchemaTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.FrontendsProcNode;
import org.apache.doris.common.proc.PartitionsProcDir;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergMetadataCache;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.extensions.mtmv.MTMVJob;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.plsql.metastore.PlsqlManager;
import org.apache.doris.plsql.metastore.PlsqlProcedureKey;
import org.apache.doris.plsql.metastore.PlsqlStoredProcedure;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QeProcessorImpl.QueryInfo;
import org.apache.doris.resource.workloadgroup.QueueToken.TokenState;
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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
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
        switch (request.getMetadaTableParams().getMetadataType()) {
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
            default:
                return errorResult("Metadata table params is not set.");
        }
        if (result.getStatus().getStatusCode() == TStatusCode.OK) {
            filterColumns(result, params.getColumnsName(), params.getMetadataType(), params);
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
                    long encodedDatetime = convertToDateTimeV2(committedAt.getYear(), committedAt.getMonthValue(),
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
        List<CatalogIf> info = Env.getCurrentEnv().getCatalogMgr().listCatalogs();
        List<TRow> dataBatch = Lists.newArrayList();

        for (CatalogIf catalog : info) {
            TRow trow = new TRow();
            trow.addToColumnValue(new TCell().setLongVal(catalog.getId()));
            trow.addToColumnValue(new TCell().setStringVal(catalog.getName()));
            trow.addToColumnValue(new TCell().setStringVal(catalog.getType()));

            Map<String, String> properties = catalog.getProperties();

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
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(15))); // running query num
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(16))); // waiting query num
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

            TokenState tokenState = queryInfo.getQueueStatus();
            if (tokenState == null) {
                trow.addToColumnValue(new TCell());
            } else if (tokenState == TokenState.READY_TO_RUN) {
                trow.addToColumnValue(new TCell().setStringVal("RUNNING"));
            } else {
                trow.addToColumnValue(new TCell().setStringVal("QUEUED"));
            }

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

    private static long convertToDateTimeV2(
            int year, int month, int day, int hour, int minute, int second, int microsecond) {
        return (long) microsecond | (long) second << 20 | (long) minute << 26 | (long) hour << 32
                | (long) day << 37 | (long) month << 42 | (long) year << 46;
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
                    LOG.debug("mv: " + mv.toInfoString());
                }
                TRow trow = new TRow();
                trow.addToColumnValue(new TCell().setLongVal(mv.getId()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getName()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getJobInfo().getJobName()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getStatus().getState().name()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getStatus().getSchemaChangeDetail()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getStatus().getRefreshState().name()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getRefreshInfo().toString()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getQuerySql()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getEnvInfo().toString()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getMvProperties().toString()));
                trow.addToColumnValue(new TCell().setStringVal(mv.getMvPartitionInfo().toNameString()));
                trow.addToColumnValue(new TCell().setBoolVal(MTMVPartitionUtil.isMTMVSync(mv)));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("mvend: " + mv.getName());
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

    private static TFetchSchemaTableDataResult tableOptionsMetadataResult(TSchemaTableRequestParams params) {
        if (!params.isSetCurrentUserIdent()) {
            return errorResult("current user ident is not set.");
        }

        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<TRow> dataBatch = Lists.newArrayList();
        List<Long> catalogIds = Env.getCurrentEnv().getCatalogMgr().getCatalogIds();
        for (Long catalogId : catalogIds) {
            CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
            List<Long> dbIds = catalog.getDbIds();
            for (Long dbId : dbIds) {
                DatabaseIf database = catalog.getDbNullable(dbId);
                List<TableIf> tables = database.getTables();
                for (TableIf table : tables) {
                    if (!(table instanceof OlapTable)) {
                        continue;
                    }
                    OlapTable olapTable = (OlapTable) table;
                    TRow trow = new TRow();
                    trow.addToColumnValue(new TCell().setStringVal(table.getName())); // TABLE_NAME
                    trow.addToColumnValue(new TCell().setStringVal(catalog.getName())); // TABLE_CATALOG
                    trow.addToColumnValue(new TCell().setStringVal(database.getFullName())); // TABLE_SCHEMA
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
                    TableProperty property = olapTable.getTableProperty();
                    if (property == null) {
                        trow.addToColumnValue(new TCell().setStringVal("")); // PROPERTIES
                    } else {
                        try {
                            trow.addToColumnValue(
                                    new TCell().setStringVal(property.getPropertiesString())); // PROPERTIES
                        } catch (IOException e) {
                            return errorResult(e.getMessage());
                        }
                    }
                    dataBatch.add(trow);
                } // for table
            } // for db
        } // for catalog
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }
}
