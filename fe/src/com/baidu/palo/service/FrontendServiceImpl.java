// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.service;

import com.baidu.palo.analysis.SetType;
import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.catalog.UserPropertyMgr;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.AuditLog;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.PatternMatcher;
import com.baidu.palo.common.ThriftServerContext;
import com.baidu.palo.common.ThriftServerEventProcessor;
import com.baidu.palo.load.EtlStatus;
import com.baidu.palo.load.LoadJob;
import com.baidu.palo.load.MiniEtlTaskInfo;
import com.baidu.palo.master.MasterImpl;
import com.baidu.palo.mysql.MysqlPassword;
import com.baidu.palo.qe.AuditBuilder;
import com.baidu.palo.qe.ConnectContext;
import com.baidu.palo.qe.ConnectProcessor;
import com.baidu.palo.qe.QeProcessor;
import com.baidu.palo.qe.VariableMgr;
import com.baidu.palo.system.Frontend;
import com.baidu.palo.system.SystemInfoService;
import com.baidu.palo.thrift.FrontendService;
import com.baidu.palo.thrift.FrontendServiceVersion;
import com.baidu.palo.thrift.TColumnDef;
import com.baidu.palo.thrift.TColumnDesc;
import com.baidu.palo.thrift.TDescribeTableParams;
import com.baidu.palo.thrift.TDescribeTableResult;
import com.baidu.palo.thrift.TFeResult;
import com.baidu.palo.thrift.TFetchResourceResult;
import com.baidu.palo.thrift.TFinishTaskRequest;
import com.baidu.palo.thrift.TGetDbsParams;
import com.baidu.palo.thrift.TGetDbsResult;
import com.baidu.palo.thrift.TGetTablesParams;
import com.baidu.palo.thrift.TGetTablesResult;
import com.baidu.palo.thrift.TListTableStatusResult;
import com.baidu.palo.thrift.TLoadCheckRequest;
import com.baidu.palo.thrift.TMasterOpRequest;
import com.baidu.palo.thrift.TMasterOpResult;
import com.baidu.palo.thrift.TMasterResult;
import com.baidu.palo.thrift.TMiniLoadEtlStatusResult;
import com.baidu.palo.thrift.TMiniLoadRequest;
import com.baidu.palo.thrift.TNetworkAddress;
import com.baidu.palo.thrift.TReportExecStatusParams;
import com.baidu.palo.thrift.TReportExecStatusResult;
import com.baidu.palo.thrift.TReportRequest;
import com.baidu.palo.thrift.TShowVariableRequest;
import com.baidu.palo.thrift.TShowVariableResult;
import com.baidu.palo.thrift.TStatus;
import com.baidu.palo.thrift.TStatusCode;
import com.baidu.palo.thrift.TTableStatus;
import com.baidu.palo.thrift.TUniqueId;
import com.baidu.palo.thrift.TUpdateExportTaskStatusRequest;
import com.baidu.palo.thrift.TUpdateMiniEtlTaskStatusRequest;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

// Frontend service used to serve all request for this frontend through
// thrift protocol
public class FrontendServiceImpl implements FrontendService.Iface {
    private static final Logger LOG = LogManager.getLogger(MasterImpl.class);
    private QeProcessor qeProcessor;
    private MasterImpl masterImpl;
    private ExecuteEnv exeEnv;

    public FrontendServiceImpl(ExecuteEnv exeEnv) {
        qeProcessor = new QeProcessor();
        masterImpl = new MasterImpl();
        this.exeEnv = exeEnv;
    }

    @Override
    public TGetDbsResult getDbNames(TGetDbsParams params) throws TException {
        TGetDbsResult result = new TGetDbsResult();
        List<String> dbs = Lists.newArrayList();
        List<String> dbNames = Catalog.getInstance().getDbNames();
        UserPropertyMgr userPropertyMgr = Catalog.getInstance().getUserMgr();
        PatternMatcher matcher = null;
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern());
            } catch (AnalysisException e) {
                throw new TException("Pattern is in bad format " + params.getPattern());
            }
        }
        for (String fullName : dbNames) {
            final String db = ClusterNamespace.getNameFromFullName(fullName);
            if (matcher != null && !matcher.match(db)) {
                continue;
            }
            if (userPropertyMgr.checkAccess(params.user, fullName, AccessPrivilege.READ_ONLY)) {
                dbs.add(fullName);
            }
        }
        result.setDbs(dbs);
        return result;
    }

    @Override
    public TGetTablesResult getTableNames(TGetTablesParams params) throws TException {
        TGetTablesResult result = new TGetTablesResult();
        List<String> tablesResult = Lists.newArrayList();
        result.setTables(tablesResult);
        PatternMatcher matcher = null;
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern());
            } catch (AnalysisException e) {
                throw new TException("Pattern is in bad format " + params.getPattern());
            }
        }
        Database db = Catalog.getInstance().getDb(params.db);
        if (db != null) {
            for (String tableName : db.getTableNamesWithLock()) {
                if (matcher != null && !matcher.match(tableName)) {
                    continue;
                }
                tablesResult.add(tableName);
            }
        }
        return result;
    }

    @Override
    public TListTableStatusResult listTableStatus(TGetTablesParams params) throws TException {
        TListTableStatusResult result = new TListTableStatusResult();
        List<TTableStatus> tablesResult = Lists.newArrayList();
        result.setTables(tablesResult);
        PatternMatcher matcher = null;
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern());
            } catch (AnalysisException e) {
                throw new TException("Pattern is in bad format " + params.getPattern());
            }
        }
        Database db = Catalog.getInstance().getDb(params.db);
        if (db != null) {
            db.readLock();
            try {
                for (Table table : db.getTables()) {
                    if (matcher != null && !matcher.match(table.getName())) {
                        continue;
                    }
                    TTableStatus status = new TTableStatus();
                    status.setName(table.getName());
                    status.setType(table.getMysqlType());
                    status.setEngine(table.getEngine());
                    status.setComment(table.getComment());

                    tablesResult.add(status);
                }
            } finally {
                db.readUnlock();
            }
        }
        return result;
    }

    @Override
    public TFeResult updateExportTaskStatus(TUpdateExportTaskStatusRequest request) throws TException {
        TStatus status = new TStatus(TStatusCode.OK);
        TFeResult result = new TFeResult(FrontendServiceVersion.V1, status);

        return result;
    }

    @Override
    public TDescribeTableResult describeTable(TDescribeTableParams params) throws TException {
        TDescribeTableResult result = new TDescribeTableResult();
        List<TColumnDef> columns = Lists.newArrayList();
        result.setColumns(columns);
        Database db = Catalog.getInstance().getDb(params.db);
        if (db != null) {
            db.readLock();
            try {
                Table table = db.getTable(params.getTable_name());
                if (table != null) {
                    for (Column column : table.getBaseSchema()) {
                        TColumnDef colDef = new TColumnDef(
                                new TColumnDesc(column.getName(), column.getDataType().toThrift()));
                        columns.add(colDef);
                    }
                }
            } finally {
                db.readUnlock();
            }
        }
        return result;
    }

    @Override
    public TShowVariableResult showVariables(TShowVariableRequest params) throws TException {
        TShowVariableResult result = new TShowVariableResult();
        Map<String, String> map = Maps.newHashMap();
        result.setVariables(map);
        // Find connect
        ConnectContext ctx = exeEnv.getScheduler().getContext(params.getThreadId());
        if (ctx == null) {
            return result;
        }
        List<List<String>> rows = VariableMgr.dump(SetType.fromThrift(params.getVarType()), ctx.getSessionVariable(),
                null);
        for (List<String> row : rows) {
            map.put(row.get(0), row.get(1));
        }
        return result;
    }

    @Override
    public TReportExecStatusResult reportExecStatus(TReportExecStatusParams params) throws TException {
        return qeProcessor.reportExecStatus(params);
    }

    @Override
    public TMasterResult finishTask(TFinishTaskRequest request) throws TException {
        return masterImpl.finishTask(request);
    }

    @Override
    public TMasterResult report(TReportRequest request) throws TException {
        return masterImpl.report(request);
    }

    @Override
    public TFetchResourceResult fetchResource() throws TException {
        return masterImpl.fetchResource();
    }

    @Override
    public TFeResult miniLoad(TMiniLoadRequest request) throws TException {
        LOG.info("mini load request is {}", request);

        ConnectContext context = new ConnectContext(null);
        String cluster;
        String user;
        if (request.isSetCluster()) {
            cluster = request.cluster;
        } else {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        final String userFullName = Catalog.getInstance().getUserMgr().isAdmin(request.user) ? request.user :
            ClusterNamespace.getFullName(cluster, request.user);
        final String dbFullName = ClusterNamespace.getFullName(cluster, request.db);
        request.setUser(userFullName);
        request.setDb(dbFullName);
        context.setCluster(cluster);
        context.setDatabase(ClusterNamespace.getFullName(cluster, request.db));
        context.setUser(ClusterNamespace.getFullName(cluster, request.user));
        context.setCatalog(Catalog.getInstance());
        context.getState().reset();
        context.setThreadLocalInfo();

        TStatus status = new TStatus(TStatusCode.OK);
        TFeResult result = new TFeResult(FrontendServiceVersion.V1, status);
        try {
            if (request.isSetSubLabel()) {
                ExecuteEnv.getInstance().getMultiLoadMgr().load(request);
            } else {
                if (!Catalog.getInstance().getLoadInstance().addLoadJob(request)) {
                    return result;
                }

                try {
                    // gen mini load audit log
                    logMiniLoadStmt(request);
                } catch (Exception e) {
                    LOG.warn("failed log mini load stmt", e);
                }
            }
        } catch (DdlException e) {
            LOG.error("add mini load error", e);
            status.setStatus_code(TStatusCode.ANALYSIS_ERROR);
            status.setError_msgs(Lists.newArrayList(e.getMessage()));
        }

        ConnectContext.remove();
        return result;
    }

    public static String getMiniLoadStmt(TMiniLoadRequest request) throws UnknownHostException {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("curl --location-trusted -u user:passwd -T ");

        if (request.files.size() == 1) {
            stringBuilder.append(request.files.get(0));
        } else if (request.files.size() > 1) {
            stringBuilder.append("\"{").append(Joiner.on(",").join(request.files)).append("}\"");
        }

        InetAddress masterAddress = FrontendOptions.getLocalHost();
        stringBuilder.append(" http://").append(masterAddress.getHostAddress()).append(":");
        stringBuilder.append(Config.http_port).append("/api/").append(request.db).append("/");
        stringBuilder.append(request.tbl).append("/_load?label=").append(request.label);

        if (!request.properties.isEmpty()) {
            stringBuilder.append("&");
            List<String> props = Lists.newArrayList();
            for (Map.Entry<String, String> entry : request.properties.entrySet()) {
                String prop = entry.getKey() + "=" + entry.getValue();
                props.add(prop);
            }
            stringBuilder.append(Joiner.on("&").join(props));
        }

        return stringBuilder.toString();
    }

    private void logMiniLoadStmt(TMiniLoadRequest request) throws UnknownHostException {
        String stmt = getMiniLoadStmt(request);
        AuditBuilder auditBuilder = new AuditBuilder();
        auditBuilder.put("client", request.getBackend().toString());
        auditBuilder.put("user", request.user);
        auditBuilder.put("db", request.db);
        auditBuilder.put("query", stmt);
        auditBuilder.put("state", TStatusCode.OK);
        auditBuilder.put("time", "0");

        AuditLog.getQueryAudit().log(auditBuilder.toString());
    }

    @Override
    public TFeResult updateMiniEtlTaskStatus(TUpdateMiniEtlTaskStatusRequest request) throws TException {
        TFeResult result = new TFeResult();
        result.setProtocolVersion(FrontendServiceVersion.V1);
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        // get job task info
        TUniqueId etlTaskId = request.getEtlTaskId();
        long jobId = etlTaskId.getHi();
        long taskId = etlTaskId.getLo();
        LoadJob job = Catalog.getInstance().getLoadInstance().getLoadJob(jobId);
        if (job == null) {
            String failMsg = "job does not exist. id: " + jobId;
            LOG.warn(failMsg);
            status.setStatus_code(TStatusCode.CANCELLED);
            status.setError_msgs(Lists.newArrayList(failMsg));
            return result;
        }

        MiniEtlTaskInfo taskInfo = job.getMiniEtlTask(taskId);
        if (taskInfo == null) {
            String failMsg = "task info does not exist. task id: " + taskId + ", job id: " + jobId;
            LOG.warn(failMsg);
            status.setStatus_code(TStatusCode.CANCELLED);
            status.setError_msgs(Lists.newArrayList(failMsg));
            return result;
        }

        // update etl task status
        TMiniLoadEtlStatusResult statusResult = request.getEtlTaskStatus();
        LOG.info("load job id: {}, etl task id: {}, status: {}", jobId, taskId, statusResult);
        EtlStatus taskStatus = taskInfo.getTaskStatus();
        if (taskStatus.setState(statusResult.getEtl_state())) {
            if (statusResult.isSetCounters()) {
                taskStatus.setCounters(statusResult.getCounters());
            }
            if (statusResult.isSetTracking_url()) {
                taskStatus.setTrackingUrl(statusResult.getTracking_url());
            }
            if (statusResult.isSetFile_map()) {
                taskStatus.setFileMap(statusResult.getFile_map());
            }
        }
        return result;
    }

    @Override
    public TMasterOpResult forward(TMasterOpRequest params) throws TException {
        ThriftServerContext connectionContext = ThriftServerEventProcessor.getConnectionContext();
        // For NonBlockingServer, we can not get client ip.
        if (connectionContext != null) {
            TNetworkAddress clientAddress = connectionContext.getClient();
            LOG.debug("debug: client address in forward: {}", clientAddress);

            Frontend fe = Catalog.getInstance().checkFeExist(
                    clientAddress.getHostname(),
                    clientAddress.getPort());
            if (fe == null) {
                throw new TException("request from invalid host, reject.");
            }
        }

        ConnectContext context = new ConnectContext(null);
        ConnectProcessor processor = new ConnectProcessor(context);
        TMasterOpResult result = processor.proxyExecute(params);
        ConnectContext.remove();
        return result;
    }

    @Override
    public TFeResult loadCheck(TLoadCheckRequest request) throws TException {
        LOG.info("Load check request is {}", request);

        TStatus status = new TStatus(TStatusCode.OK);
        TFeResult result = new TFeResult(FrontendServiceVersion.V1, status);
        String cluster;
        if (request.isSetCluster()) {
            cluster = request.cluster;
        } else {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }
        final String userFullName = Catalog.getInstance().getUserMgr().isAdmin(request.user) ? request.user :
            ClusterNamespace.getFullName(cluster, request.user);
        final String dbFullName = ClusterNamespace.getFullName(cluster, request.db);
        request.setUser(userFullName);
        request.setDb(dbFullName);
        // Check user and password
        byte[] passwd = Catalog.getInstance().getUserMgr().getPassword(userFullName);
        if (passwd == null) {
            // No such user
            status.setStatus_code(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList("No such user(" + userFullName + ")"));
            return result;
        }
        if (!MysqlPassword.checkPlainPass(passwd, request.passwd)) {
            status.setStatus_code(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList("Wrong password."));
            return result;
        }
        if (!Catalog.getInstance().getUserMgr().checkAccess(userFullName, dbFullName, AccessPrivilege.READ_WRITE)) {
            status.setStatus_code(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(
                    Lists.newArrayList("Have no privilege to write this database(" + request.getDb() + ")"));
            return result;
        }
        if (request.isSetLabel()) {
            // Only single table will be set label
            try {
                if (request.isSetTimestamp()) {
                    Catalog.getInstance().getLoadInstance().checkLabelUsed(
                        dbFullName, request.getLabel(), request.getTimestamp());
                } else {
                    Catalog.getInstance().getLoadInstance().checkLabelUsed(
                        dbFullName, request.getLabel(), 0);
                }
            } catch (DdlException e) {
                status.setStatus_code(TStatusCode.INTERNAL_ERROR);
                status.setError_msgs(Lists.newArrayList(e.getMessage()));
                return result;
            }
        }

        return result;
    }
}
