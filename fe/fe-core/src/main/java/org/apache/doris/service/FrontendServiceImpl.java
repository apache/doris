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

package org.apache.doris.service;

import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.ThriftServerContext;
import org.apache.doris.common.ThriftServerEventProcessor;
import org.apache.doris.common.UserException;
import org.apache.doris.common.Version;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.MiniEtlTaskInfo;
import org.apache.doris.master.MasterImpl;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.StreamLoadPlanner;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditEvent.AuditEventBuilder;
import org.apache.doris.plugin.AuditEvent.EventType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.StreamLoadTask;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.FrontendServiceVersion;
import org.apache.doris.thrift.TColumnDef;
import org.apache.doris.thrift.TColumnDesc;
import org.apache.doris.thrift.TDescribeTableParams;
import org.apache.doris.thrift.TDescribeTableResult;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TFeResult;
import org.apache.doris.thrift.TFetchResourceResult;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TFrontendPingFrontendRequest;
import org.apache.doris.thrift.TFrontendPingFrontendResult;
import org.apache.doris.thrift.TFrontendPingFrontendStatusCode;
import org.apache.doris.thrift.TGetDbsParams;
import org.apache.doris.thrift.TGetDbsResult;
import org.apache.doris.thrift.TGetTablesParams;
import org.apache.doris.thrift.TGetTablesResult;
import org.apache.doris.thrift.TIsMethodSupportedRequest;
import org.apache.doris.thrift.TListTableStatusResult;
import org.apache.doris.thrift.TListPrivilegesResult;
import org.apache.doris.thrift.TLoadCheckRequest;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TLoadTxnCommitRequest;
import org.apache.doris.thrift.TLoadTxnCommitResult;
import org.apache.doris.thrift.TLoadTxnRollbackRequest;
import org.apache.doris.thrift.TLoadTxnRollbackResult;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TMasterResult;
import org.apache.doris.thrift.TMiniLoadBeginRequest;
import org.apache.doris.thrift.TMiniLoadBeginResult;
import org.apache.doris.thrift.TMiniLoadEtlStatusResult;
import org.apache.doris.thrift.TMiniLoadRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TReportExecStatusResult;
import org.apache.doris.thrift.TReportRequest;
import org.apache.doris.thrift.TShowVariableRequest;
import org.apache.doris.thrift.TShowVariableResult;
import org.apache.doris.thrift.TSnapshotLoaderReportRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TStreamLoadPutResult;
import org.apache.doris.thrift.TTableStatus;
import org.apache.doris.thrift.TPrivilegeStatus;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUpdateExportTaskStatusRequest;
import org.apache.doris.thrift.TUpdateMiniEtlTaskStatusRequest;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TxnCommitAttachment;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.doris.thrift.TStatusCode.NOT_IMPLEMENTED_ERROR;

// Frontend service used to serve all request for this frontend through
// thrift protocol
public class FrontendServiceImpl implements FrontendService.Iface {
    private static final Logger LOG = LogManager.getLogger(FrontendServiceImpl.class);
    private MasterImpl masterImpl;
    private ExecuteEnv exeEnv;

    public FrontendServiceImpl(ExecuteEnv exeEnv) {
        masterImpl = new MasterImpl();
        this.exeEnv = exeEnv;
    }

    @Override
    public TGetDbsResult getDbNames(TGetDbsParams params) throws TException {
        LOG.debug("get db request: {}", params);
        TGetDbsResult result = new TGetDbsResult();

        List<String> dbs = Lists.newArrayList();
        PatternMatcher matcher = null;
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(),
                                                            CaseSensibility.DATABASE.getCaseSensibility());
            } catch (AnalysisException e) {
                throw new TException("Pattern is in bad format: " + params.getPattern());
            }
        }

        Catalog catalog = Catalog.getCurrentCatalog();
        List<String> dbNames = catalog.getDbNames();
        LOG.debug("get db names: {}", dbNames);
        
        UserIdentity currentUser = null;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        for (String fullName : dbNames) {
            if (!catalog.getAuth().checkDbPriv(currentUser, fullName, PrivPredicate.SHOW)) {
                continue;
            }

            final String db = ClusterNamespace.getNameFromFullName(fullName);
            if (matcher != null && !matcher.match(db)) {
                continue;
            }

            dbs.add(fullName);
        }
        result.setDbs(dbs);
        return result;
    }

    @Override
    public TGetTablesResult getTableNames(TGetTablesParams params) throws TException {
        LOG.debug("get table name request: {}", params);
        TGetTablesResult result = new TGetTablesResult();
        List<String> tablesResult = Lists.newArrayList();
        result.setTables(tablesResult);
        PatternMatcher matcher = null;
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(),
                                                            CaseSensibility.TABLE.getCaseSensibility());
            } catch (AnalysisException e) {
                throw new TException("Pattern is in bad format: " + params.getPattern());
            }
        }

        // database privs should be checked in analysis phrase

        Database db = Catalog.getCurrentCatalog().getDb(params.db);
        UserIdentity currentUser = null;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        if (db != null) {
            for (String tableName : db.getTableNamesWithLock()) {
                LOG.debug("get table: {}, wait to check", tableName);
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(currentUser, params.db,
                                                                        tableName, PrivPredicate.SHOW)) {
                    continue;
                }

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
        LOG.debug("get list table request: {}", params);
        TListTableStatusResult result = new TListTableStatusResult();
        List<TTableStatus> tablesResult = Lists.newArrayList();
        result.setTables(tablesResult);
        PatternMatcher matcher = null;
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(),
                                                            CaseSensibility.TABLE.getCaseSensibility());
            } catch (AnalysisException e) {
                throw new TException("Pattern is in bad format " + params.getPattern());
            }
        }

        // database privs should be checked in analysis phrase

        Database db = Catalog.getCurrentCatalog().getDb(params.db);
        UserIdentity currentUser = null;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        if (db != null) {
            List<Table> tables = null;
            if (!params.isSetType() || params.getType() == null || params.getType().isEmpty()) {
                tables = db.getTables();
            } else {
                switch (params.getType()) {
                    case "VIEW":
                        tables = db.getViews();
                        break;
                    default:
                        tables = db.getTables();
                }
            }
            for (Table table : tables) {
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(currentUser, params.db,
                        table.getName(), PrivPredicate.SHOW)) {
                    continue;
                }

                table.readLock();
                try {
                    if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(currentUser, params.db,
                            table.getName(), PrivPredicate.SHOW)) {
                        continue;
                    }

                    if (matcher != null && !matcher.match(table.getName())) {
                        continue;
                    }
                    TTableStatus status = new TTableStatus();
                    status.setName(table.getName());
                    status.setType(table.getMysqlType());
                    status.setEngine(table.getEngine());
                    status.setComment(table.getComment());
                    status.setCreateTime(table.getCreateTime());
                    status.setLastCheckTime(table.getLastCheckTime());
                    tablesResult.add(status);
                } finally {
                    table.readUnlock();
                }
            }
        }
        return result;
    }

    @Override
    public TListPrivilegesResult listTablePrivilegeStatus(TGetTablesParams params) throws TException {
        LOG.debug("get list table privileges request: {}", params);
        TListPrivilegesResult result = new TListPrivilegesResult();
        List<TPrivilegeStatus> tblPrivResult = Lists.newArrayList();
        result.setPrivileges(tblPrivResult);

        UserIdentity currentUser = null;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        Catalog.getCurrentCatalog().getAuth().getTablePrivStatus(tblPrivResult, currentUser);
        return result;
    }

    @Override
    public TListPrivilegesResult listSchemaPrivilegeStatus(TGetTablesParams params) throws TException {
        LOG.debug("get list schema privileges request: {}", params);
        TListPrivilegesResult result = new TListPrivilegesResult();
        List<TPrivilegeStatus> tblPrivResult = Lists.newArrayList();
        result.setPrivileges(tblPrivResult);

        UserIdentity currentUser = null;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        Catalog.getCurrentCatalog().getAuth().getSchemaPrivStatus(tblPrivResult, currentUser);
        return result;
    }

    @Override
    public TListPrivilegesResult listUserPrivilegeStatus(TGetTablesParams params) throws TException {
        LOG.debug("get list user privileges request: {}", params);
        TListPrivilegesResult result = new TListPrivilegesResult();
        List<TPrivilegeStatus> userPrivResult = Lists.newArrayList();
        result.setPrivileges(userPrivResult);

        UserIdentity currentUser = null;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        Catalog.getCurrentCatalog().getAuth().getGlobalPrivStatus(userPrivResult, currentUser);
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
        LOG.debug("get desc table request: {}", params);
        TDescribeTableResult result = new TDescribeTableResult();
        List<TColumnDef> columns = Lists.newArrayList();
        result.setColumns(columns);

        // database privs should be checked in analysis phrase
        UserIdentity currentUser = null;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(currentUser, params.db,
                params.getTableName(), PrivPredicate.SHOW)) {
            return result;
        }

        Database db = Catalog.getCurrentCatalog().getDb(params.db);
        if (db != null) {
            Table table = db.getTable(params.getTableName());
            if (table != null) {
                table.readLock();
                try {
                    for (Column column : table.getBaseSchema()) {
                        final TColumnDesc desc = new TColumnDesc(column.getName(), column.getDataType().toThrift());
                        final Integer precision = column.getOriginType().getPrecision();
                        if (precision != null) {
                            desc.setColumnPrecision(precision);
                        }
                        final Integer columnLength = column.getOriginType().getColumnSize();
                        if (columnLength != null) {
                            desc.setColumnLength(columnLength);
                        }
                        final Integer decimalDigits = column.getOriginType().getDecimalDigits();
                        if (decimalDigits != null) {
                            desc.setColumnScale(decimalDigits);
                        }
                        desc.setIsAllowNull(column.isAllowNull());
                        final TColumnDef colDef = new TColumnDef(desc);
                        final String comment = column.getComment();
                        if(comment != null) {
                            colDef.setComment(comment);
                        }
                        columns.add(colDef);
                    }
                } finally {
                    table.readUnlock();
                }
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
        return QeProcessorImpl.INSTANCE.reportExecStatus(params, getClientAddr());
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

    @Deprecated
    @Override
    public TFeResult miniLoad(TMiniLoadRequest request) throws TException {
        LOG.debug("receive mini load request: label: {}, db: {}, tbl: {}, backend: {}",
                request.getLabel(), request.getDb(), request.getTbl(), request.getBackend());

        ConnectContext context = new ConnectContext(null);
        String cluster = SystemInfoService.DEFAULT_CLUSTER;
        if (request.isSetCluster()) {
            cluster = request.cluster;
        }

        final String fullDbName = ClusterNamespace.getFullName(cluster, request.db);
        request.setDb(fullDbName);
        context.setCluster(cluster);
        context.setDatabase(ClusterNamespace.getFullName(cluster, request.db));
        context.setQualifiedUser(ClusterNamespace.getFullName(cluster, request.user));
        context.setCatalog(Catalog.getCurrentCatalog());
        context.getState().reset();
        context.setThreadLocalInfo();

        TStatus status = new TStatus(TStatusCode.OK);
        TFeResult result = new TFeResult(FrontendServiceVersion.V1, status);
        try {
            if (request.isSetSubLabel()) {
                ExecuteEnv.getInstance().getMultiLoadMgr().load(request);
            } else {
                // try to add load job, label will be checked here.
                if (Catalog.getCurrentCatalog().getLoadManager().createLoadJobV1FromRequest(request)) {
                    try {
                        // generate mini load audit log
                        logMiniLoadStmt(request);
                    } catch (Exception e) {
                        LOG.warn("failed log mini load stmt", e);
                    }
                }
            }
        } catch (UserException e) {
            LOG.warn("add mini load error: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("unexpected exception when adding mini load", e);
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
        } finally {
            ConnectContext.remove();
        }

        LOG.debug("mini load result: {}", result);
        return result;
    }

    private void logMiniLoadStmt(TMiniLoadRequest request) throws UnknownHostException {
        String stmt = getMiniLoadStmt(request);
        AuditEvent auditEvent = new AuditEventBuilder().setEventType(EventType.AFTER_QUERY)
                .setClientIp(request.user_ip + ":0")
                .setUser(request.user)
                .setDb(request.db)
                .setState(TStatusCode.OK.name())
                .setQueryTime(0)
                .setStmt(stmt).build();
        
        Catalog.getCurrentAuditEventProcessor().handleAuditEvent(auditEvent);
    }

    private String getMiniLoadStmt(TMiniLoadRequest request) throws UnknownHostException {
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
        LoadJob job = Catalog.getCurrentCatalog().getLoadInstance().getLoadJob(jobId);
        if (job == null) {
            String failMsg = "job does not exist. id: " + jobId;
            LOG.warn(failMsg);
            status.setStatusCode(TStatusCode.CANCELLED);
            status.addToErrorMsgs(failMsg);
            return result;
        }

        MiniEtlTaskInfo taskInfo = job.getMiniEtlTask(taskId);
        if (taskInfo == null) {
            String failMsg = "task info does not exist. task id: " + taskId + ", job id: " + jobId;
            LOG.warn(failMsg);
            status.setStatusCode(TStatusCode.CANCELLED);
            status.addToErrorMsgs(failMsg);
            return result;
        }

        // update etl task status
        TMiniLoadEtlStatusResult statusResult = request.getEtlTaskStatus();
        LOG.debug("load job id: {}, etl task id: {}, status: {}", jobId, taskId, statusResult);
        EtlStatus taskStatus = taskInfo.getTaskStatus();
        if (taskStatus.setState(statusResult.getEtlState())) {
            if (statusResult.isSetCounters()) {
                taskStatus.setCounters(statusResult.getCounters());
            }
            if (statusResult.isSetTrackingUrl()) {
                taskStatus.setTrackingUrl(statusResult.getTrackingUrl());
            }
            if (statusResult.isSetFileMap()) {
                taskStatus.setFileMap(statusResult.getFileMap());
            }
        }
        return result;
    }

    @Override
    public TMiniLoadBeginResult miniLoadBegin(TMiniLoadBeginRequest request) throws TException {
        LOG.debug("receive mini load begin request. label: {}, user: {}, ip: {}",
                 request.getLabel(), request.getUser(), request.getUserIp());

        TMiniLoadBeginResult result = new TMiniLoadBeginResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            String cluster = SystemInfoService.DEFAULT_CLUSTER;
            if (request.isSetCluster()) {
                cluster = request.cluster;
            }
            // step1: check password and privs
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                                  request.getTbl(), request.getUserIp(), PrivPredicate.LOAD);
            // step2: check label and record metadata in load manager
            if (request.isSetSubLabel()) {
                // TODO(ml): multi mini load
            } else {
                // add load metadata in loadManager
                result.setTxnId(Catalog.getCurrentCatalog().getLoadManager().createLoadJobFromMiniLoad(request));
            }
            return result;
        } catch (UserException e) {
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
            return result;
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
    }

    @Override
    public TFeResult isMethodSupported(TIsMethodSupportedRequest request) throws TException {
        TStatus status = new TStatus(TStatusCode.OK);
        TFeResult result = new TFeResult(FrontendServiceVersion.V1, status);
        switch (request.getFunctionName()){
            case "STREAMING_MINI_LOAD":
                break;
            default:
                status.setStatusCode(NOT_IMPLEMENTED_ERROR);
                break;
        }
        return result;
    }

    @Override
    public TMasterOpResult forward(TMasterOpRequest params) throws TException {
        TNetworkAddress clientAddr = getClientAddr();
        if (clientAddr != null) {
            Frontend fe = Catalog.getCurrentCatalog().getFeByHost(clientAddr.getHostname());
            if (fe == null) {
                LOG.warn("reject request from invalid host. client: {}", clientAddr);
                throw new TException("request from invalid host was rejected.");
            }
        }

        // add this log so that we can track this stmt
        LOG.debug("receive forwarded stmt {} from FE: {}", params.getStmtId(), clientAddr.getHostname());
        ConnectContext context = new ConnectContext(null);
        ConnectProcessor processor = new ConnectProcessor(context);
        TMasterOpResult result = processor.proxyExecute(params);
        ConnectContext.remove();
        return result;
    }

    private void checkPasswordAndPrivs(String cluster, String user, String passwd, String db, String tbl,
                                       String clientIp, PrivPredicate predicate) throws AuthenticationException {

        final String fullUserName = ClusterNamespace.getFullName(cluster, user);
        final String fullDbName = ClusterNamespace.getFullName(cluster, db);
        List<UserIdentity> currentUser = Lists.newArrayList();
        if (!Catalog.getCurrentCatalog().getAuth().checkPlainPassword(fullUserName, clientIp, passwd, currentUser)) {
            throw new AuthenticationException("Access denied for " + fullUserName + "@" + clientIp);
        }

        Preconditions.checkState(currentUser.size() == 1);
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(currentUser.get(0), fullDbName, tbl, predicate)) {
            throw new AuthenticationException(
                    "Access denied; you need (at least one of) the LOAD privilege(s) for this operation");
        }
    }

    @Override
    public TFeResult loadCheck(TLoadCheckRequest request) throws TException {
        LOG.debug("receive load check request. label: {}, user: {}, ip: {}",
                 request.getLabel(), request.getUser(), request.getUserIp());

        TStatus status = new TStatus(TStatusCode.OK);
        TFeResult result = new TFeResult(FrontendServiceVersion.V1, status);
        try {
            String cluster = SystemInfoService.DEFAULT_CLUSTER;
            if (request.isSetCluster()) {
                cluster = request.cluster;
            }

            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                                  request.getTbl(), request.getUserIp(), PrivPredicate.LOAD);
        } catch (UserException e) {
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
            return result;
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        return result;
    }

    @Override
    public TLoadTxnBeginResult loadTxnBegin(TLoadTxnBeginRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn begin request: {}, backend: {}", request, clientAddr);

        TLoadTxnBeginResult result = new TLoadTxnBeginResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            result.setTxnId(loadTxnBeginImpl(request, clientAddr));
        } catch (DuplicatedRequestException e) {
            // this is a duplicate request, just return previous txn id
            LOG.warn("duplicate request for stream load. request id: {}, txn: {}", e.getDuplicatedRequestId(), e.getTxnId());
            result.setTxnId(e.getTxnId());
        } catch (LabelAlreadyUsedException e) {
            status.setStatusCode(TStatusCode.LABEL_ALREADY_EXISTS);
            status.addToErrorMsgs(e.getMessage());
            result.setJobStatus(e.getJobStatus());
        } catch (UserException e) {
            LOG.warn("failed to begin: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    private long loadTxnBeginImpl(TLoadTxnBeginRequest request, String clientIp) throws UserException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                              request.getTbl(), request.getUserIp(), PrivPredicate.LOAD);

        // check label
        if (Strings.isNullOrEmpty(request.getLabel())) {
            throw new UserException("empty label in begin request");
        }
        // check database
        Catalog catalog = Catalog.getCurrentCatalog();
        String fullDbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db = catalog.getDb(fullDbName);
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }

        Table table = db.getTableOrThrowException(request.tbl, TableType.OLAP);
        // begin
        long timeoutSecond = request.isSetTimeout() ? request.getTimeout() : Config.stream_load_default_timeout_second;
        MetricRepo.COUNTER_LOAD_ADD.increase(1L);
        return Catalog.getCurrentGlobalTransactionMgr().beginTransaction(
                db.getId(), Lists.newArrayList(table.getId()), request.getLabel(), request.getRequestId(),
                new TxnCoordinator(TxnSourceType.BE, clientIp),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, -1, timeoutSecond);
    }

    @Override
    public TLoadTxnCommitResult loadTxnCommit(TLoadTxnCommitRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn commit request: {}, backend: {}", request, clientAddr);

        TLoadTxnCommitResult result = new TLoadTxnCommitResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            if (!loadTxnCommitImpl(request)) {
                // committed success but not visible
                status.setStatusCode(TStatusCode.PUBLISH_TIMEOUT);
                status.addToErrorMsgs("transaction commit successfully, BUT data will be visible later");
            }
        } catch (UserException e) {
            LOG.warn("failed to commit txn: {}: {}", request.getTxnId(), e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    // return true if commit success and publish success, return false if publish timeout
    private boolean loadTxnCommitImpl(TLoadTxnCommitRequest request) throws UserException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        if (request.isSetAuthCode()) {
            // TODO(cmy): find a way to check
        } else {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUserIp(), PrivPredicate.LOAD);
        }

        // get database
        Catalog catalog = Catalog.getCurrentCatalog();
        String fullDbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db = catalog.getDb(fullDbName);
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }
        long timeoutMs = request.isSetThriftRpcTimeoutMs() ? request.getThriftRpcTimeoutMs() : 5000;
        boolean ret = Catalog.getCurrentGlobalTransactionMgr().commitAndPublishTransaction(
                        db, request.getTxnId(),
                        TabletCommitInfo.fromThrift(request.getCommitInfos()),
                        timeoutMs, TxnCommitAttachment.fromThrift(request.txnCommitAttachment));
        if (ret) {
            // if commit and publish is success, load can be regarded as success
            MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
        }
        return ret;
    }

    @Override
    public TLoadTxnRollbackResult loadTxnRollback(TLoadTxnRollbackRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn rollback request: {}, backend: {}", request, clientAddr);
        TLoadTxnRollbackResult result = new TLoadTxnRollbackResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            loadTxnRollbackImpl(request);
        } catch (UserException e) {
            LOG.warn("failed to rollback txn {}: {}", request.getTxnId(), e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        return result;
    }

    private void loadTxnRollbackImpl(TLoadTxnRollbackRequest request) throws UserException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        if (request.isSetAuthCode()) {
            // TODO(cmy): find a way to check
        } else {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUserIp(), PrivPredicate.LOAD);
        }
        String dbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            throw new MetaNotFoundException("db " + request.getDb() + " does not exist");
        }
        long dbId = db.getId();
        Catalog.getCurrentGlobalTransactionMgr().abortTransaction(dbId, request.getTxnId(),
                                                                  request.isSetReason() ? request.getReason() : "system cancel",
                                                                  TxnCommitAttachment.fromThrift(request.getTxnCommitAttachment()));
    }

    @Override
    public TStreamLoadPutResult streamLoadPut(TStreamLoadPutRequest request) {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive stream load put request: {}, backend: {}", request, clientAddr);

        TStreamLoadPutResult result = new TStreamLoadPutResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            result.setParams(streamLoadPutImpl(request));
        } catch (UserException e) {
            LOG.warn("failed to get stream load plan: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    private TExecPlanFragmentParams streamLoadPutImpl(TStreamLoadPutRequest request) throws UserException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        Catalog catalog = Catalog.getCurrentCatalog();
        String fullDbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db = catalog.getDb(fullDbName);
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }
        long timeoutMs = request.isSetThriftRpcTimeoutMs() ? request.getThriftRpcTimeoutMs() : 5000;
        Table table = db.getTableOrThrowException(request.getTbl(), TableType.OLAP);
        table.readLock();
        if (!table.tryReadLock(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new UserException("get table read lock timeout, database=" + fullDbName + ",table=" + table.getName());
        }
        try {
            StreamLoadTask streamLoadTask = StreamLoadTask.fromTStreamLoadPutRequest(request);
            StreamLoadPlanner planner = new StreamLoadPlanner(db, (OlapTable) table, streamLoadTask);
            TExecPlanFragmentParams plan = planner.plan(streamLoadTask.getId());
            // add table indexes to transaction state
            TransactionState txnState = Catalog.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(), request.getTxnId());
            if (txnState == null) {
                throw new UserException("txn does not exist: " + request.getTxnId());
            }
            txnState.addTableIndexes((OlapTable) table);
            return plan;
        } finally {
            table.readUnlock();
        }
    }

    @Override
    public TStatus snapshotLoaderReport(TSnapshotLoaderReportRequest request) throws TException {
        if (Catalog.getCurrentCatalog().getBackupHandler().report(request.getTaskType(), request.getJobId(),
                request.getTaskId(), request.getFinishedNum(), request.getTotalNum())) {
            return new TStatus(TStatusCode.OK);
        }
        return new TStatus(TStatusCode.CANCELLED);
    }

    @Override
    public TFrontendPingFrontendResult ping(TFrontendPingFrontendRequest request) throws TException {
        boolean isReady = Catalog.getCurrentCatalog().isReady();
        TFrontendPingFrontendResult result = new TFrontendPingFrontendResult();
        result.setStatus(TFrontendPingFrontendStatusCode.OK);
        if (isReady) {
            if (request.getClusterId() != Catalog.getCurrentCatalog().getClusterId()) {
                result.setStatus(TFrontendPingFrontendStatusCode.FAILED);
                result.setMsg("invalid cluster id: " + Catalog.getCurrentCatalog().getClusterId());
            }

            if (result.getStatus() == TFrontendPingFrontendStatusCode.OK) {
                if (!request.getToken().equals(Catalog.getCurrentCatalog().getToken())) {
                    result.setStatus(TFrontendPingFrontendStatusCode.FAILED);
                    result.setMsg("invalid token: " + Catalog.getCurrentCatalog().getToken());
                }
            }

            if (result.status == TFrontendPingFrontendStatusCode.OK) {
                // cluster id and token are valid, return replayed journal id
                long replayedJournalId = Catalog.getCurrentCatalog().getReplayedJournalId();
                result.setMsg("success");
                result.setReplayedJournalId(replayedJournalId);
                result.setQueryPort(Config.query_port);
                result.setRpcPort(Config.rpc_port);
                result.setVersion(Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH);
            }
        } else {
            result.setStatus(TFrontendPingFrontendStatusCode.FAILED);
            result.setMsg("not ready");
        }
        return result;
    }

    private TNetworkAddress getClientAddr() {
        ThriftServerContext connectionContext = ThriftServerEventProcessor.getConnectionContext();
        // For NonBlockingServer, we can not get client ip.
        if (connectionContext != null) {
            return connectionContext.getClient();
        }
        return null;
    }

    private String getClientAddrAsString() {
        TNetworkAddress addr = getClientAddr();
        return addr == null ? "unknown" : addr.hostname;
    }
}


