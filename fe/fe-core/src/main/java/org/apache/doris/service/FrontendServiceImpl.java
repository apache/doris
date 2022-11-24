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

import org.apache.doris.alter.DecommissionType;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.S3Resource;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.catalog.external.ExternalTable;
import org.apache.doris.cluster.Cluster;
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
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.master.MasterImpl;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.StreamLoadPlanner;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.StreamLoadTask;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.FrontendServiceVersion;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TColumnDef;
import org.apache.doris.thrift.TColumnDesc;
import org.apache.doris.thrift.TDescribeTableParams;
import org.apache.doris.thrift.TDescribeTableResult;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TFeResult;
import org.apache.doris.thrift.TFetchResourceResult;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TFrontendPingFrontendRequest;
import org.apache.doris.thrift.TFrontendPingFrontendResult;
import org.apache.doris.thrift.TFrontendPingFrontendStatusCode;
import org.apache.doris.thrift.TGetDbsParams;
import org.apache.doris.thrift.TGetDbsResult;
import org.apache.doris.thrift.TGetStoragePolicy;
import org.apache.doris.thrift.TGetStoragePolicyResult;
import org.apache.doris.thrift.TGetTablesParams;
import org.apache.doris.thrift.TGetTablesResult;
import org.apache.doris.thrift.TInitExternalCtlMetaRequest;
import org.apache.doris.thrift.TInitExternalCtlMetaResult;
import org.apache.doris.thrift.TListPrivilegesResult;
import org.apache.doris.thrift.TListTableStatusResult;
import org.apache.doris.thrift.TLoadTxn2PCRequest;
import org.apache.doris.thrift.TLoadTxn2PCResult;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TLoadTxnCommitRequest;
import org.apache.doris.thrift.TLoadTxnCommitResult;
import org.apache.doris.thrift.TLoadTxnRollbackRequest;
import org.apache.doris.thrift.TLoadTxnRollbackResult;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TMasterResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPrivilegeStatus;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TReportExecStatusResult;
import org.apache.doris.thrift.TReportRequest;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TS3StorageParam;
import org.apache.doris.thrift.TShowVariableRequest;
import org.apache.doris.thrift.TShowVariableResult;
import org.apache.doris.thrift.TSnapshotLoaderReportRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TStreamLoadPutResult;
import org.apache.doris.thrift.TTableStatus;
import org.apache.doris.thrift.TUpdateExportTaskStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;
import org.apache.doris.transaction.DatabaseTransactionMgr;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TxnCommitAttachment;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
        List<String> catalogs = Lists.newArrayList();
        PatternMatcher matcher = null;
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(),
                        CaseSensibility.DATABASE.getCaseSensibility());
            } catch (AnalysisException e) {
                throw new TException("Pattern is in bad format: " + params.getPattern());
            }
        }

        Env env = Env.getCurrentEnv();
        List<CatalogIf> catalogIfs = env.getCatalogMgr().listCatalogs();
        for (CatalogIf catalog : catalogIfs) {
            List<String> dbNames = catalog.getDbNames();
            LOG.debug("get db names: {}, in catalog: {}", dbNames, catalog.getName());

            UserIdentity currentUser = null;
            if (params.isSetCurrentUserIdent()) {
                currentUser = UserIdentity.fromThrift(params.current_user_ident);
            } else {
                currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
            }
            for (String fullName : dbNames) {
                if (!env.getAuth().checkDbPriv(currentUser, fullName, PrivPredicate.SHOW)) {
                    continue;
                }

                final String db = ClusterNamespace.getNameFromFullName(fullName);
                if (matcher != null && !matcher.match(db)) {
                    continue;
                }

                catalogs.add(catalog.getName());
                dbs.add(fullName);
            }
        }

        result.setDbs(dbs);
        result.setCatalogs(catalogs);
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
        UserIdentity currentUser;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        String catalogName = Strings.isNullOrEmpty(params.catalog) ? InternalCatalog.INTERNAL_CATALOG_NAME
                : params.catalog;
        DatabaseIf<TableIf> db = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrException(catalogName, catalog -> new TException("Unknown catalog " + catalog))
                .getDbNullable(params.db);
        if (db != null) {
            for (String tableName : db.getTableNamesWithLock()) {
                LOG.debug("get table: {}, wait to check", tableName);
                if (!Env.getCurrentEnv().getAuth()
                        .checkTblPriv(currentUser, params.db, tableName, PrivPredicate.SHOW)) {
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

        UserIdentity currentUser;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }

        String catalogName = InternalCatalog.INTERNAL_CATALOG_NAME;
        if (params.isSetCatalog()) {
            catalogName = params.catalog;
        }
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalog != null) {
            DatabaseIf db = catalog.getDbNullable(params.db);
            if (db != null) {
                List<TableIf> tables = null;
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
                for (TableIf table : tables) {
                    if (!Env.getCurrentEnv().getAuth().checkTblPriv(currentUser, params.db,
                            table.getName(), PrivPredicate.SHOW)) {
                        continue;
                    }
                    table.readLock();
                    try {
                        if (!Env.getCurrentEnv().getAuth().checkTblPriv(currentUser, params.db,
                                table.getName(), PrivPredicate.SHOW)) {
                            continue;
                        }

                        if (matcher != null && !matcher.match(table.getName())) {
                            continue;
                        }
                        long lastCheckTime = 0;
                        if (table instanceof Table) {
                            lastCheckTime = ((Table) table).getLastCheckTime();
                        } else {
                            lastCheckTime = ((ExternalTable) table).getLastCheckTime();
                        }
                        TTableStatus status = new TTableStatus();
                        status.setName(table.getName());
                        status.setType(table.getMysqlType());
                        status.setEngine(table.getEngine());
                        status.setComment(table.getComment());
                        status.setCreateTime(table.getCreateTime());
                        status.setLastCheckTime(lastCheckTime);
                        status.setUpdateTime(table.getUpdateTime() / 1000);
                        status.setCheckTime(lastCheckTime);
                        status.setCollation("utf-8");
                        status.setRows(table.getRowCount());
                        status.setDataLength(table.getDataLength());
                        status.setAvgRowLength(table.getAvgRowLength());
                        tablesResult.add(status);
                    } finally {
                        table.readUnlock();
                    }
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
        Env.getCurrentEnv().getAuth().getTablePrivStatus(tblPrivResult, currentUser);
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
        Env.getCurrentEnv().getAuth().getSchemaPrivStatus(tblPrivResult, currentUser);
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
        Env.getCurrentEnv().getAuth().getGlobalPrivStatus(userPrivResult, currentUser);
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
        if (!Env.getCurrentEnv().getAuth()
                .checkTblPriv(currentUser, params.db, params.getTableName(), PrivPredicate.SHOW)) {
            return result;
        }

        String catalogName = Strings.isNullOrEmpty(params.catalog) ? InternalCatalog.INTERNAL_CATALOG_NAME
                : params.catalog;
        DatabaseIf<TableIf> db = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrException(catalogName, catalog -> new TException("Unknown catalog " + catalog))
                .getDbNullable(params.db);
        if (db != null) {
            TableIf table = db.getTableNullable(params.getTableName());
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
                        if (comment != null) {
                            colDef.setComment(comment);
                        }
                        if (column.isKey()) {
                            if (table instanceof OlapTable) {
                                desc.setColumnKey(((OlapTable) table).getKeysType().toMetadata());
                            }
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
        ConnectContext ctx = exeEnv.getScheduler().getContext((int) params.getThreadId());
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

    @Override
    public TMasterOpResult forward(TMasterOpRequest params) throws TException {
        TNetworkAddress clientAddr = getClientAddr();
        if (clientAddr != null) {
            Frontend fe = Env.getCurrentEnv().getFeByHost(clientAddr.getHostname());
            if (fe == null) {
                LOG.warn("reject request from invalid host. client: {}", clientAddr);
                throw new TException("request from invalid host was rejected.");
            }
        }

        // add this log so that we can track this stmt
        LOG.debug("receive forwarded stmt {} from FE: {}", params.getStmtId(), clientAddr.getHostname());
        ConnectContext context = new ConnectContext(null);
        // Set current connected FE to the client address, so that we can know where this request come from.
        context.setCurrentConnectedFEIp(clientAddr.getHostname());
        ConnectProcessor processor = new ConnectProcessor(context);
        TMasterOpResult result = processor.proxyExecute(params);
        if (QueryState.MysqlStateType.ERR.name().equalsIgnoreCase(result.getStatus())) {
            context.getState().setError(result.getStatus());
        } else {
            context.getState().setOk();
        }
        ConnectContext.remove();
        return result;
    }

    private void checkAuthCodeUuid(String dbName, long txnId, String authCodeUuid) throws AuthenticationException {
        DatabaseIf db = Env.getCurrentInternalCatalog()
                .getDbOrException(dbName, s -> new AuthenticationException("invalid db name: " + s));
        TransactionState transactionState = Env.getCurrentGlobalTransactionMgr()
                .getTransactionState(db.getId(), txnId);
        if (transactionState == null) {
            throw new AuthenticationException("invalid transactionState: " + txnId);
        }
        if (!authCodeUuid.equals(transactionState.getAuthCode())) {
            throw new AuthenticationException(
                    "Access denied; you need (at least one of) the LOAD privilege(s) for this operation");
        }
    }

    private void checkPasswordAndPrivs(String cluster, String user, String passwd, String db, String tbl,
            String clientIp, PrivPredicate predicate) throws AuthenticationException {

        final String fullUserName = ClusterNamespace.getFullName(cluster, user);
        final String fullDbName = ClusterNamespace.getFullName(cluster, db);
        List<UserIdentity> currentUser = Lists.newArrayList();
        Env.getCurrentEnv().getAuth().checkPlainPassword(fullUserName, clientIp, passwd, currentUser);

        Preconditions.checkState(currentUser.size() == 1);
        if (!Env.getCurrentEnv().getAuth().checkTblPriv(currentUser.get(0), fullDbName, tbl, predicate)) {
            throw new AuthenticationException(
                    "Access denied; you need (at least one of) the LOAD privilege(s) for this operation");
        }
    }

    @Override
    public TLoadTxnBeginResult loadTxnBegin(TLoadTxnBeginRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn begin request: {}, backend: {}", request, clientAddr);

        TLoadTxnBeginResult result = new TLoadTxnBeginResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            TLoadTxnBeginResult tmpRes = loadTxnBeginImpl(request, clientAddr);
            result.setTxnId(tmpRes.getTxnId()).setDbId(tmpRes.getDbId());
        } catch (DuplicatedRequestException e) {
            // this is a duplicate request, just return previous txn id
            LOG.warn("duplicate request for stream load. request id: {}, txn: {}", e.getDuplicatedRequestId(),
                    e.getTxnId());
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

    private TLoadTxnBeginResult loadTxnBeginImpl(TLoadTxnBeginRequest request, String clientIp) throws UserException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        if (Strings.isNullOrEmpty(request.getAuthCodeUuid())) {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(), request.getTbl(),
                    request.getUserIp(), PrivPredicate.LOAD);
        }

        // check label
        if (Strings.isNullOrEmpty(request.getLabel())) {
            throw new UserException("empty label in begin request");
        }
        // check database
        Env env = Env.getCurrentEnv();
        String fullDbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db = env.getInternalCatalog().getDbNullable(fullDbName);
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }

        OlapTable table = (OlapTable) db.getTableOrMetaException(request.tbl, TableType.OLAP);
        // begin
        long timeoutSecond = request.isSetTimeout() ? request.getTimeout() : Config.stream_load_default_timeout_second;
        long txnId = Env.getCurrentGlobalTransactionMgr().beginTransaction(
                db.getId(), Lists.newArrayList(table.getId()), request.getLabel(), request.getRequestId(),
                new TxnCoordinator(TxnSourceType.BE, clientIp),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, -1, timeoutSecond);
        if (!Strings.isNullOrEmpty(request.getAuthCodeUuid())) {
            Env.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(), txnId)
                    .setAuthCode(request.getAuthCodeUuid());
        }
        TLoadTxnBeginResult result = new TLoadTxnBeginResult();
        result.setTxnId(txnId).setDbId(db.getId());
        return result;
    }

    @Override
    public TLoadTxnCommitResult loadTxnPreCommit(TLoadTxnCommitRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn pre-commit request: {}, backend: {}", request, clientAddr);

        TLoadTxnCommitResult result = new TLoadTxnCommitResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            loadTxnPreCommitImpl(request);
        } catch (UserException e) {
            LOG.warn("failed to pre-commit txn: {}: {}", request.getTxnId(), e.getMessage());
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

    private void loadTxnPreCommitImpl(TLoadTxnCommitRequest request) throws UserException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        if (request.isSetAuthCode()) {
            // CHECKSTYLE IGNORE THIS LINE
        } else if (request.isSetAuthCodeUuid()) {
            checkAuthCodeUuid(request.getDb(), request.getTxnId(), request.getAuthCodeUuid());
        } else {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(), request.getTbl(),
                    request.getUserIp(), PrivPredicate.LOAD);
        }

        // get database
        Env env = Env.getCurrentEnv();
        String fullDbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db;
        if (request.isSetDbId() && request.getDbId() > 0) {
            db = env.getInternalCatalog().getDbNullable(request.getDbId());
        } else {
            db = env.getInternalCatalog().getDbNullable(fullDbName);
        }
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }

        long timeoutMs = request.isSetThriftRpcTimeoutMs() ? request.getThriftRpcTimeoutMs() / 2 : 5000;
        OlapTable table = (OlapTable) db.getTableOrMetaException(request.getTbl(), TableType.OLAP);
        Env.getCurrentGlobalTransactionMgr()
                .preCommitTransaction2PC(db, Lists.newArrayList(table), request.getTxnId(),
                        TabletCommitInfo.fromThrift(request.getCommitInfos()), timeoutMs,
                        TxnCommitAttachment.fromThrift(request.txnCommitAttachment));
    }

    @Override
    public TLoadTxn2PCResult loadTxn2PC(TLoadTxn2PCRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn 2PC request: {}, backend: {}", request, clientAddr);

        TLoadTxn2PCResult result = new TLoadTxn2PCResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            loadTxn2PCImpl(request);
        } catch (UserException e) {
            LOG.warn("failed to {} txn {}: {}", request.getOperation(), request.getTxnId(), e.getMessage());
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

    private void loadTxn2PCImpl(TLoadTxn2PCRequest request) throws UserException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        String dbName = request.getDb();
        if (Strings.isNullOrEmpty(dbName)) {
            throw new UserException("No database selected.");
        }

        String fullDbName = ClusterNamespace.getFullName(cluster, dbName);

        // get database
        Env env = Env.getCurrentEnv();
        Database database = env.getInternalCatalog().getDbNullable(fullDbName);
        if (database == null) {
            throw new UserException("unknown database, database=" + fullDbName);
        }

        DatabaseTransactionMgr dbTransactionMgr = Env.getCurrentGlobalTransactionMgr()
                .getDatabaseTransactionMgr(database.getId());
        TransactionState transactionState = dbTransactionMgr.getTransactionState(request.getTxnId());
        if (transactionState == null) {
            throw new UserException("transaction [" + request.getTxnId() + "] not found");
        }
        List<Long> tableIdList = transactionState.getTableIdList();
        List<Table> tableList = database.getTablesOnIdOrderOrThrowException(tableIdList);
        for (Table table : tableList) {
            // check auth
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(), table.getName(),
                    request.getUserIp(), PrivPredicate.LOAD);
        }

        String txnOperation = request.getOperation().trim();
        if (txnOperation.equalsIgnoreCase("commit")) {
            Env.getCurrentGlobalTransactionMgr()
                    .commitTransaction2PC(database, tableList, request.getTxnId(), 5000);
        } else if (txnOperation.equalsIgnoreCase("abort")) {
            Env.getCurrentGlobalTransactionMgr().abortTransaction2PC(database.getId(), request.getTxnId());
        } else {
            throw new UserException("transaction operation should be \'commit\' or \'abort\'");
        }
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
        } else if (request.isSetAuthCodeUuid()) {
            checkAuthCodeUuid(request.getDb(), request.getTxnId(), request.getAuthCodeUuid());
        } else {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(), request.getTbl(),
                    request.getUserIp(), PrivPredicate.LOAD);
        }

        // get database
        Env env = Env.getCurrentEnv();
        String fullDbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db;
        if (request.isSetDbId() && request.getDbId() > 0) {
            db = env.getInternalCatalog().getDbNullable(request.getDbId());
        } else {
            db = env.getInternalCatalog().getDbNullable(fullDbName);
        }
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }

        long timeoutMs = request.isSetThriftRpcTimeoutMs() ? request.getThriftRpcTimeoutMs() / 2 : 5000;
        Table table = db.getTableOrMetaException(request.getTbl(), TableType.OLAP);
        return Env.getCurrentGlobalTransactionMgr()
                .commitAndPublishTransaction(db, Lists.newArrayList(table), request.getTxnId(),
                        TabletCommitInfo.fromThrift(request.getCommitInfos()), timeoutMs,
                        TxnCommitAttachment.fromThrift(request.txnCommitAttachment));
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
        } else if (request.isSetAuthCodeUuid()) {
            checkAuthCodeUuid(request.getDb(), request.getTxnId(), request.getAuthCodeUuid());
        } else {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(), request.getTbl(),
                    request.getUserIp(), PrivPredicate.LOAD);
        }
        String dbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db;
        if (request.isSetDbId() && request.getDbId() > 0) {
            db = Env.getCurrentInternalCatalog().getDbNullable(request.getDbId());
        } else {
            db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
        }
        if (db == null) {
            throw new MetaNotFoundException("db " + request.getDb() + " does not exist");
        }
        long dbId = db.getId();
        Env.getCurrentGlobalTransactionMgr().abortTransaction(dbId, request.getTxnId(),
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

        Env env = Env.getCurrentEnv();
        String fullDbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db = env.getInternalCatalog().getDbNullable(fullDbName);
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }
        long timeoutMs = request.isSetThriftRpcTimeoutMs() ? request.getThriftRpcTimeoutMs() : 5000;
        Table table = db.getTableOrMetaException(request.getTbl(), TableType.OLAP);
        if (!table.tryReadLock(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new UserException(
                    "get table read lock timeout, database=" + fullDbName + ",table=" + table.getName());
        }
        try {
            StreamLoadTask streamLoadTask = StreamLoadTask.fromTStreamLoadPutRequest(request);
            StreamLoadPlanner planner = new StreamLoadPlanner(db, (OlapTable) table, streamLoadTask);
            TExecPlanFragmentParams plan = planner.plan(streamLoadTask.getId());
            // add table indexes to transaction state
            TransactionState txnState = Env.getCurrentGlobalTransactionMgr()
                    .getTransactionState(db.getId(), request.getTxnId());
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
        if (Env.getCurrentEnv().getBackupHandler().report(request.getTaskType(), request.getJobId(),
                request.getTaskId(), request.getFinishedNum(), request.getTotalNum())) {
            return new TStatus(TStatusCode.OK);
        }
        return new TStatus(TStatusCode.CANCELLED);
    }

    @Override
    public TFrontendPingFrontendResult ping(TFrontendPingFrontendRequest request) throws TException {
        boolean isReady = Env.getCurrentEnv().isReady();
        TFrontendPingFrontendResult result = new TFrontendPingFrontendResult();
        result.setStatus(TFrontendPingFrontendStatusCode.OK);
        if (isReady) {
            if (request.getClusterId() != Env.getCurrentEnv().getClusterId()) {
                result.setStatus(TFrontendPingFrontendStatusCode.FAILED);
                result.setMsg("invalid cluster id: " + Env.getCurrentEnv().getClusterId());
            }

            if (result.getStatus() == TFrontendPingFrontendStatusCode.OK) {
                if (!request.getToken().equals(Env.getCurrentEnv().getToken())) {
                    result.setStatus(TFrontendPingFrontendStatusCode.FAILED);
                    result.setMsg("invalid token: " + Env.getCurrentEnv().getToken());
                }
            }

            if (result.status == TFrontendPingFrontendStatusCode.OK) {
                // cluster id and token are valid, return replayed journal id
                long replayedJournalId = Env.getCurrentEnv().getReplayedJournalId();
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

    @Override
    public TFetchSchemaTableDataResult fetchSchemaTableData(TFetchSchemaTableDataRequest request) throws TException {
        switch (request.getSchemaTableName()) {
            case BACKENDS:
                return getBackendsSchemaTable(request);
            default:
                break;
        }
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        result.setStatus(new TStatus(TStatusCode.INTERNAL_ERROR));
        return result;
    }

    private TFetchSchemaTableDataResult getBackendsSchemaTable(TFetchSchemaTableDataRequest request) {
        final SystemInfoService clusterInfoService = Env.getCurrentSystemInfo();
        TFetchSchemaTableDataResult result = new TFetchSchemaTableDataResult();
        List<Long> backendIds = null;
        if (!Strings.isNullOrEmpty(request.cluster_name)) {
            final Cluster cluster = Env.getCurrentEnv().getCluster(request.cluster_name);
            // root not in any cluster
            if (null == cluster) {
                return result;
            }
            backendIds = cluster.getBackendIdList();
        } else {
            backendIds = clusterInfoService.getBackendIds(false);
            if (backendIds == null) {
                return result;
            }
        }

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
            trow.addToColumnValue(new TCell().setStringVal(backend.getHost()));
            if (Strings.isNullOrEmpty(request.cluster_name)) {
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
            dataBatch.add(trow);
        }

        // backends proc node get result too slow, add log to observer.
        LOG.debug("backends proc get tablet num cost: {}, total cost: {}",
                watch.elapsed(TimeUnit.MILLISECONDS), (System.currentTimeMillis() - start));

        result.setDataBatch(dataBatch);
        result.setStatus(new TStatus(TStatusCode.OK));
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

    @Override
    public TWaitingTxnStatusResult waitingTxnStatus(TWaitingTxnStatusRequest request) throws TException {
        TWaitingTxnStatusResult result = new TWaitingTxnStatusResult();
        result.setStatus(new TStatus());
        try {
            result = Env.getCurrentGlobalTransactionMgr().getWaitingTxnStatus(request);
            result.status.setStatusCode(TStatusCode.OK);
        } catch (TimeoutException e) {
            result.status.setStatusCode(TStatusCode.INCOMPLETE);
            result.status.addToErrorMsgs(e.getMessage());
        } catch (AnalysisException e) {
            result.status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            result.status.addToErrorMsgs(e.getMessage());
        }
        return result;
    }

    @Override
    public TGetStoragePolicyResult refreshStoragePolicy() throws TException {
        TGetStoragePolicyResult result = new TGetStoragePolicyResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        List<Policy> policyList = Env.getCurrentEnv().getPolicyMgr().getCopiedPoliciesByType(PolicyTypeEnum.STORAGE);
        policyList.stream().filter(p -> p instanceof StoragePolicy).map(p -> (StoragePolicy) p).forEach(
                iter -> {
                    // default policy not init.
                    if (iter.getStorageResource() == null) {
                        return;
                    }
                    TGetStoragePolicy rEntry = new TGetStoragePolicy();
                    rEntry.setPolicyName(iter.getPolicyName());
                    //java 8 not support ifPresentOrElse
                    final long[] ttlCoolDown = {-1};
                    Optional.ofNullable(iter.getCooldownTtl()).ifPresent(ttl -> ttlCoolDown[0] = Integer.parseInt(ttl));
                    rEntry.setCooldownTtl(ttlCoolDown[0]);

                    rEntry.setCooldownDatetime(
                            iter.getCooldownTimestampMs() == -1 ? -1 : iter.getCooldownTimestampMs() / 100);

                    Optional.ofNullable(iter.getMd5Checksum()).ifPresent(rEntry::setMd5Checksum);
                    TS3StorageParam s3Info = new TS3StorageParam();
                    Optional.ofNullable(iter.getStorageResource()).ifPresent(resource -> {
                        Map<String, String> storagePolicyProperties = Env.getCurrentEnv().getResourceMgr()
                                .getResource(resource).getCopiedProperties();
                        s3Info.setS3Endpoint(storagePolicyProperties.get(S3Resource.S3_ENDPOINT));
                        s3Info.setS3Region(storagePolicyProperties.get(S3Resource.S3_REGION));
                        s3Info.setRootPath(storagePolicyProperties.get(S3Resource.S3_ROOT_PATH));
                        s3Info.setS3Ak(storagePolicyProperties.get(S3Resource.S3_ACCESS_KEY));
                        s3Info.setS3Sk(storagePolicyProperties.get(S3Resource.S3_SECRET_KEY));
                        s3Info.setBucket(storagePolicyProperties.get(S3Resource.S3_BUCKET));
                        s3Info.setS3MaxConn(
                                Integer.parseInt(storagePolicyProperties.get(S3Resource.S3_MAX_CONNECTIONS)));
                        s3Info.setS3RequestTimeoutMs(
                                Integer.parseInt(storagePolicyProperties.get(S3Resource.S3_REQUEST_TIMEOUT_MS)));
                        s3Info.setS3ConnTimeoutMs(
                                Integer.parseInt(storagePolicyProperties.get(S3Resource.S3_CONNECTION_TIMEOUT_MS)));
                    });

                    rEntry.setS3StorageParam(s3Info);
                    result.addToResultEntrys(rEntry);
                }
        );
        if (!result.isSetResultEntrys()) {
            result.setResultEntrys(new ArrayList<>());
        }

        LOG.debug("refresh storage policy request: {}", result);
        return result;
    }

    @Override
    public TInitExternalCtlMetaResult initExternalCtlMeta(TInitExternalCtlMetaRequest request) throws TException {
        if (request.isSetCatalogId() && request.isSetDbId()) {
            return initDb(request.catalogId, request.dbId);
        } else if (request.isSetCatalogId()) {
            return initCatalog(request.catalogId);
        } else {
            throw new TException("Catalog name is not set. Init failed.");
        }
    }

    private TInitExternalCtlMetaResult initCatalog(long catalogId) throws TException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
        if (!(catalog instanceof ExternalCatalog)) {
            throw new TException("Only support forward ExternalCatalog init operation.");
        }
        ((ExternalCatalog) catalog).makeSureInitialized();
        TInitExternalCtlMetaResult result = new TInitExternalCtlMetaResult();
        result.setMaxJournalId(Env.getCurrentEnv().getMaxJournalId());
        result.setStatus("OK");
        return result;
    }

    private TInitExternalCtlMetaResult initDb(long catalogId, long dbId) throws TException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
        if (!(catalog instanceof ExternalCatalog)) {
            throw new TException("Only support forward ExternalCatalog init operation.");
        }
        DatabaseIf db = catalog.getDbNullable(dbId);
        if (db == null) {
            throw new TException("database " + dbId + " is null");
        }
        if (!(db instanceof ExternalDatabase)) {
            throw new TException("Only support forward ExternalDatabase init operation.");
        }
        ((ExternalDatabase) db).makeSureInitialized();
        TInitExternalCtlMetaResult result = new TInitExternalCtlMetaResult();
        result.setMaxJournalId(Env.getCurrentEnv().getMaxJournalId());
        result.setStatus("OK");
        return result;
    }
}
