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

import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.analysis.AddColumnsClause;
import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherException;
import org.apache.doris.common.ThriftServerContext;
import org.apache.doris.common.ThriftServerEventProcessor;
import org.apache.doris.common.UserException;
import org.apache.doris.common.Version;
import org.apache.doris.common.annotation.LogException;
import org.apache.doris.cooldown.CooldownDelete;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.master.MasterImpl;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.StreamLoadPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.query.QueryStats;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.tablefunction.MetadataGenerator;
import org.apache.doris.task.StreamLoadTask;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.FrontendServiceVersion;
import org.apache.doris.thrift.TAddColumnsRequest;
import org.apache.doris.thrift.TAddColumnsResult;
import org.apache.doris.thrift.TBeginTxnRequest;
import org.apache.doris.thrift.TBeginTxnResult;
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TCheckAuthRequest;
import org.apache.doris.thrift.TCheckAuthResult;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TColumnDef;
import org.apache.doris.thrift.TColumnDesc;
import org.apache.doris.thrift.TCommitTxnRequest;
import org.apache.doris.thrift.TCommitTxnResult;
import org.apache.doris.thrift.TConfirmUnusedRemoteFilesRequest;
import org.apache.doris.thrift.TConfirmUnusedRemoteFilesResult;
import org.apache.doris.thrift.TDescribeTableParams;
import org.apache.doris.thrift.TDescribeTableResult;
import org.apache.doris.thrift.TDescribeTablesParams;
import org.apache.doris.thrift.TDescribeTablesResult;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TFeResult;
import org.apache.doris.thrift.TFetchResourceResult;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TFrontendPingFrontendRequest;
import org.apache.doris.thrift.TFrontendPingFrontendResult;
import org.apache.doris.thrift.TFrontendPingFrontendStatusCode;
import org.apache.doris.thrift.TGetBinlogRequest;
import org.apache.doris.thrift.TGetBinlogResult;
import org.apache.doris.thrift.TGetDbsParams;
import org.apache.doris.thrift.TGetDbsResult;
import org.apache.doris.thrift.TGetQueryStatsRequest;
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
import org.apache.doris.thrift.TMySqlLoadAcquireTokenResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPrivilegeCtrl;
import org.apache.doris.thrift.TPrivilegeHier;
import org.apache.doris.thrift.TPrivilegeStatus;
import org.apache.doris.thrift.TPrivilegeType;
import org.apache.doris.thrift.TQueryStatsResult;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TReportExecStatusResult;
import org.apache.doris.thrift.TReportRequest;
import org.apache.doris.thrift.TRollbackTxnRequest;
import org.apache.doris.thrift.TRollbackTxnResult;
import org.apache.doris.thrift.TShowVariableRequest;
import org.apache.doris.thrift.TShowVariableResult;
import org.apache.doris.thrift.TSnapshotLoaderReportRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TStreamLoadPutResult;
import org.apache.doris.thrift.TTableIndexQueryStats;
import org.apache.doris.thrift.TTableQueryStats;
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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntSupplier;

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
    public TConfirmUnusedRemoteFilesResult confirmUnusedRemoteFiles(TConfirmUnusedRemoteFilesRequest request)
            throws TException {
        if (!Env.getCurrentEnv().isMaster()) {
            throw new TException("FE is not master");
        }
        TConfirmUnusedRemoteFilesResult res = new TConfirmUnusedRemoteFilesResult();
        if (!request.isSetConfirmList()) {
            throw new TException("confirm_list in null");
        }
        request.getConfirmList().forEach(info -> {
            if (!info.isSetCooldownMetaId()) {
                LOG.warn("cooldown_meta_id is null");
                return;
            }
            TabletMeta tabletMeta = Env.getCurrentEnv().getTabletInvertedIndex().getTabletMeta(info.tablet_id);
            if (tabletMeta == null) {
                LOG.warn("tablet {} not found", info.tablet_id);
                return;
            }
            Tablet tablet;
            int replicaNum;
            try {
                OlapTable table = (OlapTable) Env.getCurrentInternalCatalog().getDbNullable(tabletMeta.getDbId())
                        .getTable(tabletMeta.getTableId())
                        .get();
                table.readLock();
                replicaNum = table.getPartitionInfo().getReplicaAllocation(tabletMeta.getPartitionId())
                        .getTotalReplicaNum();
                try {
                    tablet = table.getPartition(tabletMeta.getPartitionId()).getIndex(tabletMeta.getIndexId())
                            .getTablet(info.tablet_id);
                } finally {
                    table.readUnlock();
                }
            } catch (RuntimeException e) {
                LOG.warn("tablet {} not found", info.tablet_id);
                return;
            }
            // check cooldownReplicaId
            Pair<Long, Long> cooldownConf = tablet.getCooldownConf();
            if (cooldownConf.first != info.cooldown_replica_id) {
                LOG.info("cooldown replica id not match({} vs {}), tablet={}", cooldownConf.first,
                        info.cooldown_replica_id, info.tablet_id);
                return;
            }
            // check cooldownMetaId of all replicas are the same
            List<Replica> replicas = Env.getCurrentEnv().getTabletInvertedIndex().getReplicas(info.tablet_id);
            // FIXME(plat1ko): We only delete remote files when tablet is under a stable state: enough replicas and
            //  all replicas are alive. Are these conditions really sufficient or necessary?
            if (replicas.size() < replicaNum) {
                LOG.info("num replicas are not enough, tablet={}", info.tablet_id);
                return;
            }
            for (Replica replica : replicas) {
                if (!replica.isAlive()) {
                    LOG.info("replica is not alive, tablet={}, replica={}", info.tablet_id, replica.getId());
                    return;
                }
                if (replica.getCooldownTerm() != cooldownConf.second) {
                    LOG.info("replica's cooldown term not match({} vs {}), tablet={}", cooldownConf.second,
                            replica.getCooldownTerm(), info.tablet_id);
                    return;
                }
                if (!info.cooldown_meta_id.equals(replica.getCooldownMetaId())) {
                    LOG.info("cooldown meta id are not same, tablet={}", info.tablet_id);
                    return;
                }
            }
            res.addToConfirmedTablets(info.tablet_id);
        });

        if (res.isSetConfirmedTablets() && !res.getConfirmedTablets().isEmpty()) {
            if (Env.getCurrentEnv().isMaster()) {
                // ensure FE is real master
                Env.getCurrentEnv().getEditLog().logCooldownDelete(new CooldownDelete());
            } else {
                throw new TException("FE is not master");
            }
        }

        return res;
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
            } catch (PatternMatcherException e) {
                throw new TException("Pattern is in bad format: " + params.getPattern());
            }
        }

        Env env = Env.getCurrentEnv();
        List<CatalogIf> catalogIfs = Lists.newArrayList();
        if (Strings.isNullOrEmpty(params.catalog)) {
            catalogIfs = env.getCatalogMgr().listCatalogs();
        } else {
            catalogIfs.add(env.getCatalogMgr()
                    .getCatalogOrException(params.catalog, catalog -> new TException("Unknown catalog " + catalog)));
        }
        for (CatalogIf catalog : catalogIfs) {
            List<String> dbNames;
            try {
                dbNames = catalog.getDbNamesOrEmpty();
            } catch (Exception e) {
                LOG.warn("failed to get database names for catalog {}", catalog.getName(), e);
                // Some external catalog may fail to get databases due to wrong connection info.
                // So continue here to get databases of other catalogs.
                continue;
            }
            LOG.debug("get db names: {}, in catalog: {}", dbNames, catalog.getName());

            UserIdentity currentUser = null;
            if (params.isSetCurrentUserIdent()) {
                currentUser = UserIdentity.fromThrift(params.current_user_ident);
            } else {
                currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
            }
            for (String fullName : dbNames) {
                if (!env.getAccessManager().checkDbPriv(currentUser, fullName, PrivPredicate.SHOW)) {
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

    private static ColumnDef initColumnfromThrift(TColumnDesc tColumnDesc, String comment) {
        TypeDef typeDef = TypeDef.createTypeDef(tColumnDesc);
        boolean isAllowNull = tColumnDesc.isIsAllowNull();
        ColumnDef.DefaultValue defaultVal = ColumnDef.DefaultValue.NOT_SET;
        // Dynamic table's Array default value should be '[]'
        if (typeDef.getType().isArrayType()) {
            defaultVal = ColumnDef.DefaultValue.ARRAY_EMPTY_DEFAULT_VALUE;
        }
        return new ColumnDef(tColumnDesc.getColumnName(), typeDef, false, null, isAllowNull, defaultVal,
                comment, true);
    }

    @Override
    public TAddColumnsResult addColumns(TAddColumnsRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("schema change clientAddr: {}, request: {}", clientAddr, request);

        TStatus status = new TStatus(TStatusCode.OK);
        List<TColumn> allColumns = new ArrayList<TColumn>();

        Env env = Env.getCurrentEnv();
        InternalCatalog catalog = env.getInternalCatalog();
        int schemaVersion = 0;
        try {
            if (!env.isMaster()) {
                status.setStatusCode(TStatusCode.ILLEGAL_STATE);
                status.addToErrorMsgs("retry rpc request to master.");
                TAddColumnsResult result = new TAddColumnsResult();
                result.setStatus(status);
                return result;
            }
            TableName tableName = new TableName("", request.getDbName(), request.getTableName());
            if (request.getTableId() > 0) {
                tableName = catalog.getTableNameByTableId(request.getTableId());
            }
            if (tableName == null) {
                throw new MetaNotFoundException("table_id " + request.getTableId() + " does not exist");
            }

            Database db = catalog.getDbNullable(tableName.getDb());
            if (db == null) {
                throw new MetaNotFoundException("db " + tableName.getDb() + " does not exist");
            }

            List<TColumnDef> addColumns = request.getAddColumns();
            boolean queryMode = false;
            if (addColumns == null || addColumns.size() == 0) {
                queryMode = true;
            }

            // rpc only olap table
            OlapTable olapTable = (OlapTable) db.getTableOrMetaException(tableName.getTbl(), TableType.OLAP);
            olapTable.writeLockOrMetaException();

            try {
                List<ColumnDef> columnDefs = new ArrayList<ColumnDef>();

                // prepare columnDefs
                for (TColumnDef tColumnDef : addColumns) {
                    if (request.isAllowTypeConflict()) {
                        // ignore column with same name
                        boolean hasSameNameColumn = false;
                        for (Column column : olapTable.getBaseSchema()) {
                            if (column.getName().equalsIgnoreCase(tColumnDef.getColumnDesc().getColumnName())) {
                                hasSameNameColumn = true;
                            }
                        }
                        // ignore this column
                        if (hasSameNameColumn) {
                            continue;
                        }
                    }
                    String comment = tColumnDef.getComment();
                    if (comment == null || comment.length() == 0) {
                        Instant ins = Instant.ofEpochSecond(System.currentTimeMillis() / 1000);
                        ZonedDateTime zdt = ins.atZone(ZoneId.systemDefault());
                        comment = "auto change " + zdt.toString();
                    }

                    TColumnDesc tColumnDesc = tColumnDef.getColumnDesc();
                    ColumnDef columnDef = initColumnfromThrift(tColumnDesc, comment);
                    columnDefs.add(columnDef);
                }

                if (!queryMode && !columnDefs.isEmpty()) {
                    // create AddColumnsClause
                    AddColumnsClause addColumnsClause = new AddColumnsClause(columnDefs, null, null);
                    addColumnsClause.analyze(null);

                    // index id -> index schema
                    Map<Long, LinkedList<Column>> indexSchemaMap = new HashMap<>();
                    //index id -> index col_unique_id supplier
                    Map<Long, IntSupplier> colUniqueIdSupplierMap = new HashMap<>();
                    for (Map.Entry<Long, List<Column>> entry : olapTable.getIndexIdToSchema(true).entrySet()) {
                        indexSchemaMap.put(entry.getKey(), new LinkedList<>(entry.getValue()));
                        IntSupplier colUniqueIdSupplier = null;
                        if (olapTable.getEnableLightSchemaChange()) {
                            colUniqueIdSupplier = new IntSupplier() {
                                public int pendingMaxColUniqueId = olapTable
                                        .getIndexMetaByIndexId(entry.getKey()).getMaxColUniqueId();

                                @Override
                                public int getAsInt() {
                                    pendingMaxColUniqueId++;
                                    return pendingMaxColUniqueId;
                                }
                            };
                        }
                        colUniqueIdSupplierMap.put(entry.getKey(), colUniqueIdSupplier);
                    }
                    //4. call schame change function, only for dynamic table feature.
                    SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();

                    boolean lightSchemaChange = schemaChangeHandler.processAddColumns(
                            addColumnsClause, olapTable, indexSchemaMap, true, colUniqueIdSupplierMap);
                    if (lightSchemaChange) {
                        //for schema change add column optimize, direct modify table meta.
                        List<Index> newIndexes = olapTable.getCopiedIndexes();
                        long jobId = Env.getCurrentEnv().getNextId();
                        Env.getCurrentEnv().getSchemaChangeHandler().modifyTableLightSchemaChange(
                                db, olapTable, indexSchemaMap, newIndexes, jobId, false);
                    } else {
                        throw new MetaNotFoundException("table_id "
                                + request.getTableId() + " cannot light schema change through rpc.");
                    }
                }

                //5. build all columns
                for (Column column : olapTable.getBaseSchema()) {
                    allColumns.add(column.toThrift());
                }
                schemaVersion = olapTable.getBaseSchemaVersion();
            } catch (Exception e) {
                LOG.warn("got exception add columns: ", e);
                status.setStatusCode(TStatusCode.INTERNAL_ERROR);
                status.addToErrorMsgs(e.getMessage());
            } finally {
                olapTable.writeUnlock();
            }
        } catch (MetaNotFoundException e) {
            status.setStatusCode(TStatusCode.NOT_FOUND);
            status.addToErrorMsgs(e.getMessage());
        } catch (UserException e) {
            status.setStatusCode(TStatusCode.INVALID_ARGUMENT);
            status.addToErrorMsgs(e.getMessage());
        } catch (Exception e) {
            LOG.warn("got exception add columns: ", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(e.getMessage());
        }

        TAddColumnsResult result = new TAddColumnsResult();
        result.setStatus(status);
        result.setTableId(request.getTableId());
        result.setAllColumns(allColumns);
        result.setSchemaVersion(schemaVersion);
        LOG.debug("result: {}", result);
        return result;
    }

    @LogException
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
            } catch (PatternMatcherException e) {
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
            Set<String> tableNames;
            try {
                tableNames = db.getTableNamesOrEmptyWithLock();
                for (String tableName : tableNames) {
                    LOG.debug("get table: {}, wait to check", tableName);
                    if (!Env.getCurrentEnv().getAccessManager()
                            .checkTblPriv(currentUser, params.db, tableName, PrivPredicate.SHOW)) {
                        continue;
                    }
                    if (matcher != null && !matcher.match(tableName)) {
                        continue;
                    }
                    tablesResult.add(tableName);
                }
            } catch (Exception e) {
                LOG.warn("failed to get table names for db {} in catalog {}", params.db, catalogName, e);
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
            } catch (PatternMatcherException e) {
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
                try {
                    List<TableIf> tables;
                    if (!params.isSetType() || params.getType() == null || params.getType().isEmpty()) {
                        tables = db.getTablesOrEmpty();
                    } else {
                        switch (params.getType()) {
                            case "VIEW":
                                tables = db.getViewsOrEmpty();
                                break;
                            default:
                                tables = db.getTablesOrEmpty();
                        }
                    }
                    for (TableIf table : tables) {
                        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(currentUser, params.db,
                                table.getName(), PrivPredicate.SHOW)) {
                            continue;
                        }
                        table.readLock();
                        try {
                            if (matcher != null && !matcher.match(table.getName())) {
                                continue;
                            }
                            long lastCheckTime = table.getLastCheckTime() <= 0 ? 0 : table.getLastCheckTime();
                            TTableStatus status = new TTableStatus();
                            status.setName(table.getName());
                            status.setType(table.getMysqlType());
                            status.setEngine(table.getEngine());
                            status.setComment(table.getComment());
                            status.setCreateTime(table.getCreateTime());
                            status.setLastCheckTime(lastCheckTime / 1000);
                            status.setUpdateTime(table.getUpdateTime() / 1000);
                            status.setCheckTime(lastCheckTime / 1000);
                            status.setCollation("utf-8");
                            status.setRows(table.getRowCount());
                            status.setDataLength(table.getDataLength());
                            status.setAvgRowLength(table.getAvgRowLength());
                            tablesResult.add(status);
                        } finally {
                            table.readUnlock();
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("failed to get tables for db {} in catalog {}", db.getFullName(), catalogName, e);
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
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(currentUser, params.db, params.getTableName(), PrivPredicate.SHOW)) {
            return result;
        }

        String catalogName = Strings.isNullOrEmpty(params.catalog) ? InternalCatalog.INTERNAL_CATALOG_NAME
                : params.catalog;
        DatabaseIf<TableIf> db = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrException(catalogName, catalog -> new TException("Unknown catalog " + catalog))
                .getDbNullable(params.db);
        if (db != null) {
            TableIf table = db.getTableNullableIfException(params.getTableName());
            if (table != null) {
                table.readLock();
                try {
                    List<Column> baseSchema = table.getBaseSchemaOrEmpty();
                    for (Column column : baseSchema) {
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
    public TDescribeTablesResult describeTables(TDescribeTablesParams params) throws TException {
        LOG.debug("get desc tables request: {}", params);
        TDescribeTablesResult result = new TDescribeTablesResult();
        List<TColumnDef> columns = Lists.newArrayList();
        List<Integer> tablesOffset = Lists.newArrayList();
        List<String> tables = params.getTablesName();
        result.setColumns(columns);
        result.setTablesOffset(tablesOffset);

        // database privs should be checked in analysis phrase
        UserIdentity currentUser = null;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        for (String tableName : tables) {
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(currentUser, params.db, tableName, PrivPredicate.SHOW)) {
                return result;
            }
        }

        String catalogName = Strings.isNullOrEmpty(params.catalog) ? InternalCatalog.INTERNAL_CATALOG_NAME
                : params.catalog;
        DatabaseIf<TableIf> db = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrException(catalogName, catalog -> new TException("Unknown catalog " + catalog))
                .getDbNullable(params.db);
        if (db != null) {
            for (String tableName : tables) {
                TableIf table = db.getTableNullableIfException(tableName);
                if (table != null) {
                    table.readLock();
                    try {
                        List<Column> baseSchema = table.getBaseSchemaOrEmpty();
                        for (Column column : baseSchema) {
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
                    tablesOffset.add(columns.size());
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

    // This interface is used for keeping backward compatible
    @Override
    public TFetchResourceResult fetchResource() throws TException {
        throw new TException("not supported");
    }

    @Override
    public TMasterOpResult forward(TMasterOpRequest params) throws TException {
        Frontend fe = Env.getCurrentEnv().checkFeExist(params.getClientNodeHost(), params.getClientNodePort());
        if (fe == null) {
            LOG.warn("reject request from invalid host. client: {}", params.getClientNodeHost());
            throw new TException("request from invalid host was rejected.");
        }

        // add this log so that we can track this stmt
        LOG.debug("receive forwarded stmt {} from FE: {}", params.getStmtId(), params.getClientNodeHost());
        ConnectContext context = new ConnectContext();
        // Set current connected FE to the client address, so that we can know where this request come from.
        context.setCurrentConnectedFEIp(params.getClientNodeHost());
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

    private void checkPasswordAndPrivs(String cluster, String user, String passwd, String db, String tbl,
                                       String clientIp, PrivPredicate predicate) throws AuthenticationException {
        checkPasswordAndPrivs(cluster, user, passwd, db, Lists.newArrayList(tbl), clientIp, predicate);
    }

    private void checkPasswordAndPrivs(String cluster, String user, String passwd, String db, List<String> tables,
                                       String clientIp, PrivPredicate predicate) throws AuthenticationException {

        final String fullUserName = ClusterNamespace.getFullName(cluster, user);
        final String fullDbName = ClusterNamespace.getFullName(cluster, db);
        List<UserIdentity> currentUser = Lists.newArrayList();
        Env.getCurrentEnv().getAuth().checkPlainPassword(fullUserName, clientIp, passwd, currentUser);

        Preconditions.checkState(currentUser.size() == 1);
        for (String tbl : tables) {
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(currentUser.get(0), fullDbName, tbl, predicate)) {
                throw new AuthenticationException(
                        "Access denied; you need (at least one of) the LOAD privilege(s) for this operation");
            }
        }
    }

    private void checkToken(String token) throws AuthenticationException {
        if (!Env.getCurrentEnv().getLoadManager().getTokenManager().checkAuthToken(token)) {
            throw new AuthenticationException("Un matched cluster token.");
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

        if (Strings.isNullOrEmpty(request.getToken())) {
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
        TLoadTxnBeginResult result = new TLoadTxnBeginResult();
        result.setTxnId(txnId).setDbId(db.getId());
        return result;
    }

    @Override
    public TBeginTxnResult beginTxn(TBeginTxnRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn begin request: {}, client: {}", request, clientAddr);

        TBeginTxnResult result = new TBeginTxnResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            TBeginTxnResult tmpRes = beginTxnImpl(request, clientAddr);
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

    private TBeginTxnResult beginTxnImpl(TBeginTxnRequest request, String clientIp) throws UserException {
        /// Check required arg: user, passwd, db, tables, label
        if (!request.isSetUser()) {
            throw new UserException("user is not set");
        }
        if (!request.isSetPasswd()) {
            throw new UserException("passwd is not set");
        }
        if (!request.isSetDb()) {
            throw new UserException("db is not set");
        }
        if (!request.isSetTables()) {
            throw new UserException("tables is not set");
        }
        if (!request.isSetLabel()) {
            throw new UserException("label is not set");
        }

        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        // step 1: check auth
        if (Strings.isNullOrEmpty(request.getToken())) {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(), request.getTables(),
                    request.getUserIp(), PrivPredicate.LOAD);
        }

        // step 2: check label
        if (Strings.isNullOrEmpty(request.getLabel())) {
            throw new UserException("empty label in begin request");
        }

        // step 3: check database
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

        // step 4: fetch all tableIds
        // lookup tables && convert into tableIdList
        List<Long> tableIdList = Lists.newArrayList();
        for (String tblName : request.getTables()) {
            Table table = db.getTableOrMetaException(tblName, TableType.OLAP);
            if (table == null) {
                throw new UserException("unknown table, table=" + tblName);
            }
            tableIdList.add(table.getId());
        }

        // step 5: get timeout
        long timeoutSecond = request.isSetTimeout() ? request.getTimeout() : Config.stream_load_default_timeout_second;

        // step 6: begin transaction
        long txnId = Env.getCurrentGlobalTransactionMgr().beginTransaction(
                db.getId(), tableIdList, request.getLabel(), request.getRequestId(),
                new TxnCoordinator(TxnSourceType.BE, clientIp),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, -1, timeoutSecond);

        // step 7: return result
        TBeginTxnResult result = new TBeginTxnResult();
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
        } else if (request.isSetToken()) {
            checkToken(request.getToken());
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
        String txnOperation = request.getOperation().trim();
        List<Table> tableList = new ArrayList<>();
        // if table was dropped, stream load must can abort.
        if (txnOperation.equalsIgnoreCase("abort")) {
            tableList = database.getTablesOnIdOrderIfExist(tableIdList);
        } else {
            tableList = database.getTablesOnIdOrderOrThrowException(tableIdList);
        }
        for (Table table : tableList) {
            // check auth
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(), table.getName(),
                    request.getUserIp(), PrivPredicate.LOAD);
        }

        if (txnOperation.equalsIgnoreCase("commit")) {
            Env.getCurrentGlobalTransactionMgr()
                    .commitTransaction2PC(database, tableList, request.getTxnId(), 5000);
        } else if (txnOperation.equalsIgnoreCase("abort")) {
            Env.getCurrentGlobalTransactionMgr().abortTransaction2PC(database.getId(), request.getTxnId(), tableList);
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
        } else if (request.isSetToken()) {
            checkToken(request.getToken());
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
    public TCommitTxnResult commitTxn(TCommitTxnRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn commit request: {}, client: {}", request, clientAddr);

        TCommitTxnResult result = new TCommitTxnResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            if (!commitTxnImpl(request)) {
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
    private boolean commitTxnImpl(TCommitTxnRequest request) throws UserException {
        /// Check required arg: user, passwd, db, txn_id, commit_infos
        if (!request.isSetUser()) {
            throw new UserException("user is not set");
        }
        if (!request.isSetPasswd()) {
            throw new UserException("passwd is not set");
        }
        if (!request.isSetDb()) {
            throw new UserException("db is not set");
        }
        if (!request.isSetTxnId()) {
            throw new UserException("txn_id is not set");
        }
        if (!request.isSetCommitInfos()) {
            throw new UserException("commit_infos is not set");
        }

        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        // Step 1: get && check database
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

        // Step 2: get tables
        DatabaseTransactionMgr dbTransactionMgr = Env.getCurrentGlobalTransactionMgr()
                .getDatabaseTransactionMgr(db.getId());
        TransactionState transactionState = dbTransactionMgr.getTransactionState(request.getTxnId());
        if (transactionState == null) {
            throw new UserException("transaction [" + request.getTxnId() + "] not found");
        }
        List<Long> tableIdList = transactionState.getTableIdList();
        List<Table> tableList = new ArrayList<>();
        List<String> tables = new ArrayList<>();
        // if table was dropped, transaction must be aborted
        tableList = db.getTablesOnIdOrderOrThrowException(tableIdList);
        for (Table table : tableList) {
            tables.add(table.getName());
        }

        // Step 3: check auth
        if (request.isSetAuthCode()) {
            // TODO(cmy): find a way to check
        } else if (request.isSetToken()) {
            checkToken(request.getToken());
        } else {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(), tables,
                    request.getUserIp(), PrivPredicate.LOAD);
        }

        // Step 4: get timeout
        long timeoutMs = request.isSetThriftRpcTimeoutMs() ? request.getThriftRpcTimeoutMs() / 2 : 5000;


        // Step 5: commit and publish
        return Env.getCurrentGlobalTransactionMgr()
                .commitAndPublishTransaction(db, tableList,
                        request.getTxnId(),
                        TabletCommitInfo.fromThrift(request.getCommitInfos()), timeoutMs,
                        TxnCommitAttachment.fromThrift(request.getTxnCommitAttachment()));
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
        } else if (request.isSetToken()) {
            checkToken(request.getToken());
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
        DatabaseTransactionMgr dbTransactionMgr = Env.getCurrentGlobalTransactionMgr().getDatabaseTransactionMgr(dbId);
        TransactionState transactionState = dbTransactionMgr.getTransactionState(request.getTxnId());
        if (transactionState == null) {
            throw new UserException("transaction [" + request.getTxnId() + "] not found");
        }
        List<Table> tableList = db.getTablesOnIdOrderIfExist(transactionState.getTableIdList());
        Env.getCurrentGlobalTransactionMgr().abortTransaction(dbId, request.getTxnId(),
                request.isSetReason() ? request.getReason() : "system cancel",
                TxnCommitAttachment.fromThrift(request.getTxnCommitAttachment()), tableList);
    }

    @Override
    public TRollbackTxnResult rollbackTxn(TRollbackTxnRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn rollback request: {}, client: {}", request, clientAddr);
        TRollbackTxnResult result = new TRollbackTxnResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            rollbackTxnImpl(request);
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

    private void rollbackTxnImpl(TRollbackTxnRequest request) throws UserException {
        /// Check required arg: user, passwd, db, txn_id
        if (!request.isSetUser()) {
            throw new UserException("user is not set");
        }
        if (!request.isSetPasswd()) {
            throw new UserException("passwd is not set");
        }
        if (!request.isSetDb()) {
            throw new UserException("db is not set");
        }
        if (!request.isSetTxnId()) {
            throw new UserException("txn_id is not set");
        }

        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        // Step 1: get && check database
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

        // Step 2: get tables
        DatabaseTransactionMgr dbTransactionMgr = Env.getCurrentGlobalTransactionMgr()
                .getDatabaseTransactionMgr(db.getId());
        TransactionState transactionState = dbTransactionMgr.getTransactionState(request.getTxnId());
        if (transactionState == null) {
            throw new UserException("transaction [" + request.getTxnId() + "] not found");
        }
        List<Long> tableIdList = transactionState.getTableIdList();
        List<Table> tableList = new ArrayList<>();
        List<String> tables = new ArrayList<>();
        tableList = db.getTablesOnIdOrderOrThrowException(tableIdList);
        for (Table table : tableList) {
            tables.add(table.getName());
        }

        // Step 3: check auth
        if (request.isSetAuthCode()) {
            // TODO(cmy): find a way to check
        } else if (request.isSetToken()) {
            checkToken(request.getToken());
        } else {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(), tables,
                    request.getUserIp(), PrivPredicate.LOAD);
        }

        // Step 4: abort txn
        Env.getCurrentGlobalTransactionMgr().abortTransaction(db.getId(), request.getTxnId(),
                request.isSetReason() ? request.getReason() : "system cancel",
                TxnCommitAttachment.fromThrift(request.getTxnCommitAttachment()), tableList);
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
            status.addToErrorMsgs(e.getClass().getSimpleName() + ": " + Strings.nullToEmpty(e.getMessage()));
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
            case METADATA_TABLE:
                return MetadataGenerator.getMetadataTable(request);
            default:
                break;
        }
        return MetadataGenerator.errorResult("Fetch schema table name is not set");
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

    @Override
    public TMySqlLoadAcquireTokenResult acquireToken() throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive acquire token request from client: {}", clientAddr);
        TMySqlLoadAcquireTokenResult result = new TMySqlLoadAcquireTokenResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            String token = Env.getCurrentEnv().getLoadManager().getTokenManager().acquireToken();
            result.setToken(token);
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        return result;
    }

    @Override
    public TCheckAuthResult checkAuth(TCheckAuthRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive auth request: {}, backend: {}", request, clientAddr);

        TCheckAuthResult result = new TCheckAuthResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        // check account and password
        final String fullUserName = ClusterNamespace.getFullName(cluster, request.getUser());
        List<UserIdentity> currentUser = Lists.newArrayList();
        try {
            Env.getCurrentEnv().getAuth().checkPlainPassword(fullUserName, request.getUserIp(), request.getPasswd(),
                    currentUser);
        } catch (AuthenticationException e) {
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        Preconditions.checkState(currentUser.size() == 1);
        PrivPredicate predicate = getPrivPredicate(request.getPrivType());
        if (predicate == null) {
            return result;
        }
        // check privilege
        AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
        TPrivilegeCtrl privCtrl = request.getPrivCtrl();
        TPrivilegeHier privHier = privCtrl.getPrivHier();
        if (privHier == TPrivilegeHier.GLOBAL) {
            if (!accessManager.checkGlobalPriv(currentUser.get(0), predicate)) {
                status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
                status.addToErrorMsgs("Global permissions error");
            }
        } else if (privHier == TPrivilegeHier.CATALOG) {
            if (!accessManager.checkCtlPriv(currentUser.get(0), privCtrl.getCtl(), predicate)) {
                status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
                status.addToErrorMsgs("Catalog permissions error");
            }
        } else if (privHier == TPrivilegeHier.DATABASE) {
            String fullDbName = ClusterNamespace.getFullName(cluster, privCtrl.getDb());
            if (!accessManager.checkDbPriv(currentUser.get(0), fullDbName, predicate)) {
                status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
                status.addToErrorMsgs("Database permissions error");
            }
        } else if (privHier == TPrivilegeHier.TABLE) {
            String fullDbName = ClusterNamespace.getFullName(cluster, privCtrl.getDb());
            if (!accessManager.checkTblPriv(currentUser.get(0), fullDbName, privCtrl.getTbl(), predicate)) {
                status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
                status.addToErrorMsgs("Table permissions error");
            }
        } else if (privHier == TPrivilegeHier.COLUMNS) {
            String fullDbName = ClusterNamespace.getFullName(cluster, privCtrl.getDb());

            try {
                accessManager.checkColumnsPriv(currentUser.get(0), fullDbName, privCtrl.getTbl(), privCtrl.getCols(),
                        predicate);
            } catch (UserException e) {
                status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
                status.addToErrorMsgs("Columns permissions error:" + e.getMessage());
            }
        } else if (privHier == TPrivilegeHier.RESOURSE) {
            if (!accessManager.checkResourcePriv(currentUser.get(0), privCtrl.getRes(), predicate)) {
                status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
                status.addToErrorMsgs("Resourse permissions error");
            }
        } else {
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs("Privilege control error");
        }
        return result;
    }

    private PrivPredicate getPrivPredicate(TPrivilegeType privType) {
        if (privType == null) {
            return null;
        }
        switch (privType) {
            case SHOW:
                return PrivPredicate.SHOW;
            case SHOW_RESOURCES:
                return PrivPredicate.SHOW_RESOURCES;
            case GRANT:
                return PrivPredicate.GRANT;
            case ADMIN:
                return PrivPredicate.ADMIN;
            case LOAD:
                return PrivPredicate.LOAD;
            case ALTER:
                return PrivPredicate.ALTER;
            case USAGE:
                return PrivPredicate.USAGE;
            case CREATE:
                return PrivPredicate.CREATE;
            case ALL:
                return PrivPredicate.ALL;
            case OPERATOR:
                return PrivPredicate.OPERATOR;
            case DROP:
                return PrivPredicate.DROP;
            default:
                return null;
        }
    }

    @Override
    public TQueryStatsResult getQueryStats(TGetQueryStatsRequest request) throws TException {
        TQueryStatsResult result = new TQueryStatsResult();
        result.setStatus(new TStatus(TStatusCode.OK));
        if (!request.isSetType()) {
            TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs("type is not set");
            result.setStatus(status);
            return result;
        }
        try {
            switch (request.getType()) {
                case CATALOG: {
                    if (!request.isSetCatalog()) {
                        TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
                        status.addToErrorMsgs("catalog is not set");
                        result.setStatus(status);
                    } else {
                        result.setSimpleResult(Env.getCurrentEnv().getQueryStats().getCatalogStats(request.catalog));
                    }
                    break;
                }
                case DATABASE: {
                    if (!request.isSetCatalog() || !request.isSetDb()) {
                        TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
                        status.addToErrorMsgs("catalog or db is not set");
                        result.setStatus(status);
                        return result;
                    } else {
                        result.setSimpleResult(
                                Env.getCurrentEnv().getQueryStats().getDbStats(request.catalog, request.db));
                    }
                    break;
                }
                case TABLE: {
                    if (!request.isSetCatalog() || !request.isSetDb() || !request.isSetTbl()) {
                        TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
                        status.addToErrorMsgs("catalog or db or table is not set");
                        result.setStatus(status);
                        return result;
                    } else {
                        Env.getCurrentEnv().getQueryStats().getTblStats(request.catalog, request.db, request.tbl)
                                .forEach((col, stat) -> {
                                    TTableQueryStats colunmStat = new TTableQueryStats();
                                    colunmStat.setField(col);
                                    colunmStat.setQueryStats(stat.first);
                                    colunmStat.setFilterStats(stat.second);
                                    result.addToTableStats(colunmStat);
                                });
                    }
                    break;
                }
                case TABLE_ALL: {
                    if (!request.isSetCatalog() || !request.isSetDb() || !request.isSetTbl()) {
                        TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
                        status.addToErrorMsgs("catalog or db or table is not set");
                        result.setStatus(status);
                    } else {
                        result.setSimpleResult(Env.getCurrentEnv().getQueryStats()
                                .getTblAllStats(request.catalog, request.db, request.tbl));
                    }
                    break;
                }
                case TABLE_ALL_VERBOSE: {
                    if (!request.isSetCatalog() || !request.isSetDb() || !request.isSetTbl()) {
                        TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
                        status.addToErrorMsgs("catalog or db or table is not set");
                        result.setStatus(status);
                    } else {
                        Env.getCurrentEnv().getQueryStats()
                                .getTblAllVerboseStats(request.catalog, request.db, request.tbl)
                                .forEach((indexName, indexStats) -> {
                                    TTableIndexQueryStats indexStat = new TTableIndexQueryStats();
                                    indexStat.setIndexName(indexName);
                                    indexStats.forEach((col, stat) -> {
                                        TTableQueryStats colunmStat = new TTableQueryStats();
                                        colunmStat.setField(col);
                                        colunmStat.setQueryStats(stat.first);
                                        colunmStat.setFilterStats(stat.second);
                                        indexStat.addToTableStats(colunmStat);
                                    });
                                    result.addToTableVerbosStats(indexStat);
                                });
                    }
                    break;
                }
                case TABLET: {
                    if (!request.isSetReplicaId()) {
                        TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
                        status.addToErrorMsgs("Replica Id is not set");
                        result.setStatus(status);
                    } else {
                        Map<Long, Long> tabletStats = new HashMap<>();
                        tabletStats.put(request.getReplicaId(),
                                Env.getCurrentEnv().getQueryStats().getStats(request.getReplicaId()));
                        result.setTabletStats(tabletStats);
                    }
                    break;
                }
                case TABLETS: {
                    if (!request.isSetReplicaIds()) {
                        TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
                        status.addToErrorMsgs("Replica Ids is not set");
                        result.setStatus(status);
                    } else {
                        Map<Long, Long> tabletStats = new HashMap<>();
                        QueryStats qs = Env.getCurrentEnv().getQueryStats();
                        for (long replicaId : request.getReplicaIds()) {
                            tabletStats.put(replicaId, qs.getStats(replicaId));
                        }
                        result.setTabletStats(tabletStats);
                    }
                    break;
                }
                default: {
                    TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
                    status.addToErrorMsgs("unknown type: " + request.getType());
                    result.setStatus(status);
                    break;
                }
            }
        } catch (UserException e) {
            TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
            result.setStatus(status);
        }
        return result;
    }

    public TGetBinlogResult getBinlog(TGetBinlogRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive get binlog request: {}", request);

        TGetBinlogResult result = new TGetBinlogResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            result = getBinlogImpl(request, clientAddr);
        } catch (UserException e) {
            LOG.warn("failed to get binlog: {}", e.getMessage());
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

    private TGetBinlogResult getBinlogImpl(TGetBinlogRequest request, String clientIp) throws UserException {
        /// Check all required arg: user, passwd, db, prev_commit_seq
        if (!request.isSetUser()) {
            throw new UserException("user is not set");
        }
        if (!request.isSetPasswd()) {
            throw new UserException("passwd is not set");
        }
        if (!request.isSetDb()) {
            throw new UserException("db is not set");
        }
        if (!request.isSetPrevCommitSeq()) {
            throw new UserException("prev_commit_seq is not set");
        }

        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        // step 1: check auth
        if (Strings.isNullOrEmpty(request.getToken())) {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(), request.getTable(),
                    request.getUserIp(), PrivPredicate.LOAD);
        }

        // step 3: check database
        Env env = Env.getCurrentEnv();
        String fullDbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db = env.getInternalCatalog().getDbNullable(fullDbName);
        long dbId = db.getId();
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }

        // step 4: fetch all tableIds
        // lookup tables && convert into tableIdList
        long tableId = -1;
        String tableName = request.getTable();
        if (!Strings.isNullOrEmpty(tableName)) {
            Table table = db.getTableOrMetaException(tableName, TableType.OLAP);
            if (table == null) {
                throw new UserException("unknown table, table=" + tableName);
            }
            tableId = table.getId();
        }

        // step 6: get binlog
        TGetBinlogResult result = new TGetBinlogResult();
        result.setStatus(new TStatus(TStatusCode.OK));
        long prevCommitSeq = request.getPrevCommitSeq();
        Pair<TStatus, TBinlog> statusBinlogPair = env.getBinlogManager().getBinlog(dbId, tableId, prevCommitSeq);
        TStatus status = statusBinlogPair.first;
        if (status != null && status.getStatusCode() != TStatusCode.OK) {
            result.setStatus(status);
            // TOO_OLD return first exist binlog
            if (status.getStatusCode() != TStatusCode.BINLOG_TOO_OLD_COMMIT_SEQ) {
                return result;
            }
        }
        TBinlog binlog = statusBinlogPair.second;
        if (binlog != null) {
            List<TBinlog> binlogs = Lists.newArrayList();
            binlogs.add(binlog);
            result.setBinlogs(binlogs);
        }
        return result;
    }
}
