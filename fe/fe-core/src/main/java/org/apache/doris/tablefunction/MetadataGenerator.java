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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.FrontendsProcNode;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergMetadataCache;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.extensions.mtmv.MTMVJob;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QeProcessorImpl.QueryInfo;
import org.apache.doris.resource.workloadgroup.QueueToken.TokenState;
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
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TSchemaTableRequestParams;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTasksMetadataParams;
import org.apache.doris.thrift.TUserIdentity;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetadataGenerator {
    private static final Logger LOG = LogManager.getLogger(MetadataGenerator.class);

    private static final ImmutableList<Column> ACTIVE_QUERIES_SCHEMA = ImmutableList.of(
            new Column("QUERY_ID", ScalarType.createStringType()),
            new Column("QUERY_START_TIME", ScalarType.createStringType()),
            new Column("QUERY_TIME_MS", PrimitiveType.BIGINT),
            new Column("WORKLOAD_GROUP_ID", PrimitiveType.BIGINT),
            new Column("DATABASE", ScalarType.createStringType()),
            new Column("FRONTEND_INSTANCE", ScalarType.createStringType()),
            new Column("QUEUE_START_TIME", ScalarType.createStringType()),
            new Column("QUEUE_END_TIME", ScalarType.createStringType()),
            new Column("QUERY_STATUS", ScalarType.createStringType()),
            new Column("SQL", ScalarType.createStringType()));

    private static final ImmutableMap<String, Integer> ACTIVE_QUERIES_COLUMN_TO_INDEX;


    private static final ImmutableList<Column> WORKLOAD_GROUPS_SCHEMA = ImmutableList.of(
            new Column("ID", ScalarType.BIGINT),
            new Column("NAME", ScalarType.createStringType()),
            new Column("CPU_SHARE", PrimitiveType.BIGINT),
            new Column("MEMORY_LIMIT", ScalarType.createStringType()),
            new Column("ENABLE_MEMORY_OVERCOMMIT", ScalarType.createStringType()),
            new Column("MAX_CONCURRENCY", PrimitiveType.BIGINT),
            new Column("MAX_QUEUE_SIZE", PrimitiveType.BIGINT),
            new Column("QUEUE_TIMEOUT", PrimitiveType.BIGINT),
            new Column("CPU_HARD_LIMIT", PrimitiveType.BIGINT),
            new Column("SCAN_THREAD_NUM", PrimitiveType.BIGINT),
            new Column("MAX_REMOTE_SCAN_THREAD_NUM", PrimitiveType.BIGINT),
            new Column("MIN_REMOTE_SCAN_THREAD_NUM", PrimitiveType.BIGINT));

    private static final ImmutableMap<String, Integer> WORKLOAD_GROUPS_COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> activeQueriesbuilder = new ImmutableMap.Builder();
        for (int i = 0; i < ACTIVE_QUERIES_SCHEMA.size(); i++) {
            activeQueriesbuilder.put(ACTIVE_QUERIES_SCHEMA.get(i).getName().toLowerCase(), i);
        }
        ACTIVE_QUERIES_COLUMN_TO_INDEX = activeQueriesbuilder.build();

        ImmutableMap.Builder<String, Integer> workloadGroupsBuilder = new ImmutableMap.Builder();
        for (int i = 0; i < WORKLOAD_GROUPS_SCHEMA.size(); i++) {
            workloadGroupsBuilder.put(WORKLOAD_GROUPS_SCHEMA.get(i).getName().toLowerCase(), i);
        }
        WORKLOAD_GROUPS_COLUMN_TO_INDEX = workloadGroupsBuilder.build();
    }

    public static TFetchSchemaTableDataResult getMetadataTable(TFetchSchemaTableDataRequest request) throws TException {
        if (!request.isSetMetadaTableParams() || !request.getMetadaTableParams().isSetMetadataType()) {
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
            case JOBS:
                result = jobMetadataResult(params);
                break;
            case TASKS:
                result = taskMetadataResult(params);
                break;
            case WORKLOAD_SCHED_POLICY:
                result = workloadSchedPolicyMetadataResult(params);
                break;
            default:
                return errorResult("Metadata table params is not set.");
        }
        if (result.getStatus().getStatusCode() == TStatusCode.OK) {
            filterColumns(result, params.getColumnsName(), params.getMetadataType(), params);
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

        TIcebergMetadataParams icebergMetadataParams =  params.getIcebergMetadataParams();
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
        List<CatalogIf> info  = Env.getCurrentEnv().getCatalogMgr().listCatalogs();
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
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(0))));  // id
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(1)));             // name
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(2)))); // cpu_share
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(3)));             // mem_limit
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(4)));             // mem overcommit
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(5)))); // max concurrent
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(6)))); // max queue size
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(7)))); // queue timeout
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(8)));             // cpu hard limit
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(9)))); // scan thread num
            // max remote scan thread num
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(10))));
            // min remote scan thread num
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(11))));
            dataBatch.add(trow);
        }

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    private static TFetchSchemaTableDataResult workloadSchedPolicyMetadataResult(TMetadataTableRequestParams params) {
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
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(policyRow.get(0))));    // id
            trow.addToColumnValue(new TCell().setStringVal(policyRow.get(1)));                // name
            trow.addToColumnValue(new TCell().setStringVal(policyRow.get(2)));                // condition
            trow.addToColumnValue(new TCell().setStringVal(policyRow.get(3)));                // action
            trow.addToColumnValue(new TCell().setIntVal(Integer.valueOf(policyRow.get(4))));  // priority
            trow.addToColumnValue(new TCell().setBoolVal(Boolean.valueOf(policyRow.get(5)))); // enabled
            trow.addToColumnValue(new TCell().setIntVal(Integer.valueOf(policyRow.get(6)))); // version
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
        if (!params.isSetMaterializedViewsMetadataParams()) {
            return errorResult("MaterializedViews metadata params is not set.");
        }

        TMaterializedViewsMetadataParams mtmvMetadataParams = params.getMaterializedViewsMetadataParams();
        String dbName = mtmvMetadataParams.getDatabase();
        TUserIdentity currentUserIdent = mtmvMetadataParams.getCurrentUserIdent();
        UserIdentity userIdentity = UserIdentity.fromThrift(currentUserIdent);
        List<TRow> dataBatch = Lists.newArrayList();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<Table> tables;
        try {
            tables = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalogOrAnalysisException(InternalCatalog.INTERNAL_CATALOG_NAME)
                    .getDbOrAnalysisException(dbName).getTables();
        } catch (AnalysisException e) {
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
                dataBatch.add(trow);
            }
        }
        result.setDataBatch(dataBatch);
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
                TRow tvfInfo = task.getTvfInfo();
                if (tvfInfo != null) {
                    dataBatch.add(tvfInfo);
                }
            }
        }
        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }
}
