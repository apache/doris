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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
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
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.extensions.mtmv.MTMVJob;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.external.iceberg.IcebergMetadataCache;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QeProcessorImpl.QueryInfo;
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
import org.apache.doris.thrift.TQueriesMetadataParams;
import org.apache.doris.thrift.TQueryStatistics;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTasksMetadataParams;
import org.apache.doris.thrift.TUserIdentity;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class MetadataGenerator {
    private static final Logger LOG = LogManager.getLogger(MetadataGenerator.class);

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
            case WORKLOAD_GROUPS:
                result = workloadGroupsMetadataResult(params);
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
            case QUERIES:
                result = queriesMetadataResult(params, request);
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
        LOG.debug("backends proc get tablet num cost: {}, total cost: {}",
                watch.elapsed(TimeUnit.MILLISECONDS), (System.currentTimeMillis() - start));

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

    private static TFetchSchemaTableDataResult workloadGroupsMetadataResult(TMetadataTableRequestParams params) {
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
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(4)));             //mem overcommit
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(5)))); // max concurrent
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(6)))); // max queue size
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(7)))); // queue timeout
            trow.addToColumnValue(new TCell().setStringVal(rGroupsInfo.get(8)));             // cpu hard limit
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(9)))); // running query num
            trow.addToColumnValue(new TCell().setLongVal(Long.valueOf(rGroupsInfo.get(10)))); // waiting query num
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

    private static TRow makeQueryStatisticsTRow(SimpleDateFormat sdf, String queryId, Backend be,
            String selfNode, QueryInfo queryInfo, TQueryStatistics qs) {
        TRow trow = new TRow();
        if (be != null) {
            trow.addToColumnValue(new TCell().setStringVal(be.getHost()));
            trow.addToColumnValue(new TCell().setLongVal(be.getBePort()));
        } else {
            trow.addToColumnValue(new TCell().setStringVal("invalid host"));
            trow.addToColumnValue(new TCell().setLongVal(-1));
        }
        trow.addToColumnValue(new TCell().setStringVal(queryId));

        String strDate = sdf.format(new Date(queryInfo.getStartExecTime()));
        trow.addToColumnValue(new TCell().setStringVal(strDate));
        trow.addToColumnValue(new TCell().setLongVal(System.currentTimeMillis() - queryInfo.getStartExecTime()));

        if (qs != null) {
            trow.addToColumnValue(new TCell().setLongVal(qs.workload_group_id));
            trow.addToColumnValue(new TCell().setLongVal(qs.cpu_ms));
            trow.addToColumnValue(new TCell().setLongVal(qs.scan_rows));
            trow.addToColumnValue(new TCell().setLongVal(qs.scan_bytes));
            trow.addToColumnValue(new TCell().setLongVal(qs.max_peak_memory_bytes));
            trow.addToColumnValue(new TCell().setLongVal(qs.current_used_memory_bytes));
        } else {
            trow.addToColumnValue(new TCell().setLongVal(0L));
            trow.addToColumnValue(new TCell().setLongVal(0L));
            trow.addToColumnValue(new TCell().setLongVal(0L));
            trow.addToColumnValue(new TCell().setLongVal(0L));
            trow.addToColumnValue(new TCell().setLongVal(0L));
            trow.addToColumnValue(new TCell().setLongVal(0L));
        }

        if (queryInfo.getConnectContext() != null) {
            trow.addToColumnValue(new TCell().setStringVal(queryInfo.getConnectContext().getDatabase()));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(""));
        }
        trow.addToColumnValue(new TCell().setStringVal(selfNode));
        trow.addToColumnValue(new TCell().setStringVal(queryInfo.getSql()));

        return trow;
    }

    private static TFetchSchemaTableDataResult queriesMetadataResult(TMetadataTableRequestParams params,
                                                                     TFetchSchemaTableDataRequest parentRequest) {
        if (!params.isSetQueriesMetadataParams()) {
            return errorResult("queries metadata param is not set.");
        }

        TQueriesMetadataParams queriesMetadataParams = params.getQueriesMetadataParams();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();

        String selfNode = Env.getCurrentEnv().getSelfNode().getHost();
        if (ConnectContext.get() != null && !Strings.isNullOrEmpty(ConnectContext.get().getCurrentConnectedFEIp())) {
            selfNode = ConnectContext.get().getCurrentConnectedFEIp();
        }
        selfNode = NetUtils.getHostnameByIp(selfNode);

        // get query
        Map<Long, Map<String, TQueryStatistics>> beQsMap = Env.getCurrentEnv().getWorkloadRuntimeStatusMgr()
                .getBeQueryStatsMap();
        Set<Long> beIdSet = beQsMap.keySet();

        List<TRow> dataBatch = Lists.newArrayList();
        Map<String, QueryInfo> queryInfoMap = QeProcessorImpl.INSTANCE.getQueryInfoMap();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (Long beId : beIdSet) {
            Map<String, TQueryStatistics> qsMap = beQsMap.get(beId);
            if (qsMap == null) {
                continue;
            }
            Set<String> queryIdSet = qsMap.keySet();
            for (String queryId : queryIdSet) {
                QueryInfo queryInfo = queryInfoMap.get(queryId);
                if (queryInfo == null) {
                    continue;
                }
                //todo(wb) add connect context for insert select
                if (queryInfo.getConnectContext() != null && !Env.getCurrentEnv().getAccessManager()
                        .checkDbPriv(queryInfo.getConnectContext(), queryInfo.getConnectContext().getDatabase(),
                                PrivPredicate.SELECT)) {
                    continue;
                }
                TQueryStatistics qs = qsMap.get(queryId);
                Backend be = Env.getCurrentEnv().getClusterInfo().getBackend(beId);
                TRow tRow = makeQueryStatisticsTRow(sdf, queryId, be, selfNode, queryInfo, qs);
                dataBatch.add(tRow);
            }
        }

        /* Get the query results from other FE also */
        if (queriesMetadataParams.isRelayToOtherFe()) {
            TFetchSchemaTableDataRequest relayRequest = new TFetchSchemaTableDataRequest(parentRequest);
            TMetadataTableRequestParams relayParams = new TMetadataTableRequestParams(params);
            TQueriesMetadataParams relayQueryParams = new TQueriesMetadataParams(queriesMetadataParams);

            relayQueryParams.setRelayToOtherFe(false);
            relayParams.setQueriesMetadataParams(relayQueryParams);
            relayRequest.setMetadaTableParams(relayParams);

            List<TFetchSchemaTableDataResult> relayResults = forwardToOtherFrontends(relayRequest);
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
                trow.addToColumnValue(new TCell().setBoolVal(MTMVUtil.isMTMVSync(mv)));
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

