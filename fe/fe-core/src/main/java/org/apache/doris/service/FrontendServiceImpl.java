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
import org.apache.doris.analysis.AbstractBackupTableRefClause;
import org.apache.doris.analysis.AddColumnsClause;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.RestoreStmt;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.backup.Snapshot;
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
import org.apache.doris.common.util.Util;
import org.apache.doris.cooldown.CooldownDelete;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.master.MasterImpl;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.planner.StreamLoadPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.DdlExecutor;
import org.apache.doris.qe.MasterCatalogExecutor;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.StatisticsCacheKey;
import org.apache.doris.statistics.query.QueryStats;
import org.apache.doris.system.Backend;
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
import org.apache.doris.thrift.TGetBinlogLagResult;
import org.apache.doris.thrift.TGetBinlogRequest;
import org.apache.doris.thrift.TGetBinlogResult;
import org.apache.doris.thrift.TGetDbsParams;
import org.apache.doris.thrift.TGetDbsResult;
import org.apache.doris.thrift.TGetMasterTokenRequest;
import org.apache.doris.thrift.TGetMasterTokenResult;
import org.apache.doris.thrift.TGetQueryStatsRequest;
import org.apache.doris.thrift.TGetSnapshotRequest;
import org.apache.doris.thrift.TGetSnapshotResult;
import org.apache.doris.thrift.TGetTablesParams;
import org.apache.doris.thrift.TGetTablesResult;
import org.apache.doris.thrift.TGetTabletReplicaInfosRequest;
import org.apache.doris.thrift.TGetTabletReplicaInfosResult;
import org.apache.doris.thrift.TInitExternalCtlMetaRequest;
import org.apache.doris.thrift.TInitExternalCtlMetaResult;
import org.apache.doris.thrift.TListPrivilegesResult;
import org.apache.doris.thrift.TListTableMetadataNameIdsResult;
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
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPrivilegeCtrl;
import org.apache.doris.thrift.TPrivilegeHier;
import org.apache.doris.thrift.TPrivilegeStatus;
import org.apache.doris.thrift.TPrivilegeType;
import org.apache.doris.thrift.TQueryStatsResult;
import org.apache.doris.thrift.TReplicaInfo;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TReportExecStatusResult;
import org.apache.doris.thrift.TReportRequest;
import org.apache.doris.thrift.TRestoreSnapshotRequest;
import org.apache.doris.thrift.TRestoreSnapshotResult;
import org.apache.doris.thrift.TRollbackTxnRequest;
import org.apache.doris.thrift.TRollbackTxnResult;
import org.apache.doris.thrift.TShowVariableRequest;
import org.apache.doris.thrift.TShowVariableResult;
import org.apache.doris.thrift.TSnapshotLoaderReportRequest;
import org.apache.doris.thrift.TSnapshotType;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStreamLoadMultiTablePutResult;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TStreamLoadPutResult;
import org.apache.doris.thrift.TTableIndexQueryStats;
import org.apache.doris.thrift.TTableMetadataNameIds;
import org.apache.doris.thrift.TTableQueryStats;
import org.apache.doris.thrift.TTableRef;
import org.apache.doris.thrift.TTableStatus;
import org.apache.doris.thrift.TUpdateExportTaskStatusRequest;
import org.apache.doris.thrift.TUpdateFollowerStatsCacheRequest;
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
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

// Frontend service used to serve all request for this frontend through
// thrift protocol
public class FrontendServiceImpl implements FrontendService.Iface {
    private static final Logger LOG = LogManager.getLogger(FrontendServiceImpl.class);

    private static final String NOT_MASTER_ERR_MSG = "FE is not master";

    private MasterImpl masterImpl;
    private ExecuteEnv exeEnv;
    // key is txn id,value is index of plan fragment instance, it's used by multi table request plan
    private ConcurrentHashMap<Long, Integer> multiTableFragmentInstanceIdIndexMap =
            new ConcurrentHashMap<>(64);

    private static TNetworkAddress getMasterAddress() {
        Env env = Env.getCurrentEnv();
        String masterHost = env.getMasterHost();
        int masterRpcPort = env.getMasterRpcPort();
        return new TNetworkAddress(masterHost, masterRpcPort);
    }

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

        List<String> dbNames = Lists.newArrayList();
        List<String> catalogNames = Lists.newArrayList();
        List<Long> dbIds = Lists.newArrayList();
        List<Long> catalogIds = Lists.newArrayList();

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
            Collection<DatabaseIf> dbs = new HashSet<DatabaseIf>();
            try {
                dbs = catalog.getAllDbs();
            } catch (Exception e) {
                LOG.warn("failed to get database names for catalog {}", catalog.getName(), e);
                // Some external catalog may fail to get databases due to wrong connection info.
            }
            LOG.debug("get db size: {}, in catalog: {}", dbs.size(), catalog.getName());
            if (dbs.isEmpty() && params.isSetGetNullCatalog() && params.get_null_catalog) {
                catalogNames.add(catalog.getName());
                dbNames.add("NULL");
                catalogIds.add(catalog.getId());
                dbIds.add(-1L);
                continue;
            }
            if (dbs.isEmpty()) {
                continue;
            }
            UserIdentity currentUser = null;
            if (params.isSetCurrentUserIdent()) {
                currentUser = UserIdentity.fromThrift(params.current_user_ident);
            } else {
                currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
            }
            for (DatabaseIf db : dbs) {
                String fullName = db.getFullName();
                if (!env.getAccessManager().checkDbPriv(currentUser, fullName, PrivPredicate.SHOW)) {
                    continue;
                }

                if (matcher != null && !matcher.match(ClusterNamespace.getNameFromFullName(fullName))) {
                    continue;
                }

                catalogNames.add(catalog.getName());
                dbNames.add(fullName);
                catalogIds.add(catalog.getId());
                dbIds.add(db.getId());
            }
        }

        result.setDbs(dbNames);
        result.setCatalogs(catalogNames);
        result.setCatalogIds(catalogIds);
        result.setDbIds(dbIds);
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
        return new ColumnDef(tColumnDesc.getColumnName(), typeDef, false, null, isAllowNull, false,
                defaultVal, comment, true);
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
                olapTable.checkNormalStateForAlter();
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
                                "",
                                db, olapTable, indexSchemaMap, newIndexes, null, false, jobId, false);
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

    public TListTableMetadataNameIdsResult listTableMetadataNameIds(TGetTablesParams params) throws TException {

        LOG.debug("get list simple table request: {}", params);

        TListTableMetadataNameIdsResult result = new TListTableMetadataNameIdsResult();
        List<TTableMetadataNameIds> tablesResult = Lists.newArrayList();
        result.setTables(tablesResult);

        UserIdentity currentUser;
        if (params.isSetCurrentUserIdent()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }

        String catalogName;
        if (params.isSetCatalog()) {
            catalogName = params.catalog;
        } else {
            catalogName = InternalCatalog.INTERNAL_CATALOG_NAME;
        }

        PatternMatcher matcher = null;
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(),
                    CaseSensibility.TABLE.getCaseSensibility());
            } catch (PatternMatcherException e) {
                throw new TException("Pattern is in bad format " + params.getPattern());
            }
        }
        PatternMatcher finalMatcher = matcher;


        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future = executor.submit(() -> {

            CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
            if (catalog != null) {
                DatabaseIf db = catalog.getDbNullable(params.db);
                if (db != null) {
                    List<TableIf> tables = db.getTables();
                    for (TableIf table : tables) {
                        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(currentUser, params.db,
                                table.getName(), PrivPredicate.SHOW)) {
                            continue;
                        }
                        table.readLock();
                        try {
                            if (finalMatcher != null && !finalMatcher.match(table.getName())) {
                                continue;
                            }
                            TTableMetadataNameIds status = new TTableMetadataNameIds();
                            status.setName(table.getName());
                            status.setId(table.getId());

                            tablesResult.add(status);
                        } finally {
                            table.readUnlock();
                        }
                    }
                }
            }
        });
        try {
            if (catalogName.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
                future.get();
            } else {
                future.get(Config.query_metadata_name_ids_timeout, TimeUnit.SECONDS);
            }
        } catch (TimeoutException e) {
            future.cancel(true);
            LOG.info("From catalog:{},db:{} get tables timeout.", catalogName, params.db);
        } catch (InterruptedException | ExecutionException e) {
            future.cancel(true);
        } finally {
            executor.shutdown();
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
        if (params.isSyncJournalOnly()) {
            final TMasterOpResult result = new TMasterOpResult();
            result.setMaxJournalId(Env.getCurrentEnv().getMaxJournalId());
            // just make the protocol happy
            result.setPacket("".getBytes());
            return result;
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

    private List<String> getTableNames(String cluster, String dbName, List<Long> tableIds) throws UserException {
        final String fullDbName = ClusterNamespace.getFullName(cluster, dbName);
        Database db = Env.getCurrentInternalCatalog().getDbNullable(fullDbName);
        if (db == null) {
            throw new UserException(String.format("can't find db named: %s", dbName));
        }
        List<String> tableNames = Lists.newArrayList();
        for (Long id : tableIds) {
            Table table = db.getTableNullable(id);
            if (table == null) {
                throw new UserException(String.format("can't find table id: %d in db: %s", id, dbName));
            }
            tableNames.add(table.getName());
        }

        return tableNames;
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

    private void checkPassword(String cluster, String user, String passwd, String clientIp)
            throws AuthenticationException {
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }
        final String fullUserName = ClusterNamespace.getFullName(cluster, user);
        List<UserIdentity> currentUser = Lists.newArrayList();
        Env.getCurrentEnv().getAuth().checkPlainPassword(fullUserName, clientIp, passwd, currentUser);
        Preconditions.checkState(currentUser.size() == 1);
    }

    @Override
    public TLoadTxnBeginResult loadTxnBegin(TLoadTxnBeginRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn begin request: {}, backend: {}", request, clientAddr);

        TLoadTxnBeginResult result = new TLoadTxnBeginResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.error("failed to loadTxnBegin:{}, request:{}, backend:{}",
                    NOT_MASTER_ERR_MSG, request, clientAddr);
            return result;
        }

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

        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("failed to get beginTxn: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

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
        if (!request.isSetTableIds()) {
            throw new UserException("table ids is not set");
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
            // lookup table ids && convert into tableNameList
            List<String> tableNameList = getTableNames(cluster, request.getDb(), request.getTableIds());
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(), tableNameList,
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
        // table ids is checked at step 1
        List<Long> tableIdList = request.getTableIds();

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
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.error("failed to loadTxnPreCommit:{}, request:{}, backend:{}",
                    NOT_MASTER_ERR_MSG, request, clientAddr);
            return result;
        }
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

    private List<Table> queryLoadCommitTables(TLoadTxnCommitRequest request, Database db) throws UserException {
        List<String> tbNames;
        //check has multi table
        if (CollectionUtils.isNotEmpty(request.getTbls())) {
            tbNames = request.getTbls();

        } else {
            tbNames = Collections.singletonList(request.getTbl());
        }
        List<Table> tables = new ArrayList<>(tbNames.size());
        for (String tbl : tbNames) {
            OlapTable table = (OlapTable) db.getTableOrMetaException(tbl, TableType.OLAP);
            tables.add(table);
        }
        //if it has multi table, use multi table and update multi table running transaction table ids
        if (CollectionUtils.isNotEmpty(request.getTbls())) {
            List<Long> multiTableIds = tables.stream().map(Table::getId).collect(Collectors.toList());
            Env.getCurrentGlobalTransactionMgr().getDatabaseTransactionMgr(db.getId())
                    .updateMultiTableRunningTransactionTableIds(request.getTxnId(), multiTableIds);
            LOG.debug("txn {} has multi table {}", request.getTxnId(), request.getTbls());
        }
        return tables;
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
            // refactoring it
            if (CollectionUtils.isNotEmpty(request.getTbls())) {
                for (String tbl : request.getTbls()) {
                    checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(), tbl,
                            request.getUserIp(), PrivPredicate.LOAD);
                }
            } else {
                checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                        request.getTbl(),
                        request.getUserIp(), PrivPredicate.LOAD);
            }
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
        List<Table> tables = queryLoadCommitTables(request, db);
        Env.getCurrentGlobalTransactionMgr()
                .preCommitTransaction2PC(db, tables, request.getTxnId(),
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
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.error("failed to loadTxn2PC:{}, request:{}, backend:{}",
                    NOT_MASTER_ERR_MSG, request, clientAddr);
            return result;
        }

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
        LOG.debug("txn {} has multi table {}", request.getTxnId(), transactionState.getTableIdList());
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
            long timeoutMs = request.isSetThriftRpcTimeoutMs() ? request.getThriftRpcTimeoutMs() / 2 : 5000;
            Env.getCurrentGlobalTransactionMgr()
                    .commitTransaction2PC(database, tableList, request.getTxnId(), timeoutMs);
        } else if (txnOperation.equalsIgnoreCase("abort")) {
            Env.getCurrentGlobalTransactionMgr().abortTransaction2PC(database.getId(), request.getTxnId(), tableList);
        } else {
            throw new UserException("transaction operation should be \'commit\' or \'abort\'");
        }
    }

    @Override
    public TLoadTxnCommitResult loadTxnCommit(TLoadTxnCommitRequest request) throws TException {
        multiTableFragmentInstanceIdIndexMap.remove(request.getTxnId());
        deleteMultiTableStreamLoadJobIndex(request.getTxnId());
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn commit request: {}, backend: {}", request, clientAddr);

        TLoadTxnCommitResult result = new TLoadTxnCommitResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.error("failed to loadTxnCommit:{}, request:{}, backend:{}",
                    NOT_MASTER_ERR_MSG, request, clientAddr);
            return result;
        }

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
            if (CollectionUtils.isNotEmpty(request.getTbls())) {
                checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                        request.getTbls(), request.getUserIp(), PrivPredicate.LOAD);
            } else {
                checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                        request.getTbl(), request.getUserIp(), PrivPredicate.LOAD);
            }
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
        List<Table> tables = queryLoadCommitTables(request, db);
        return Env.getCurrentGlobalTransactionMgr()
                .commitAndPublishTransaction(db, tables, request.getTxnId(),
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

        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("failed to get commitTxn: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

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
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            LOG.error("failed to loadTxnRollback:{}, request:{}, backend:{}",
                    NOT_MASTER_ERR_MSG, request, clientAddr);
            return result;
        }
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
            //multi table load
            if (CollectionUtils.isNotEmpty(request.getTbls())) {
                for (String tbl : request.getTbls()) {
                    checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(), tbl,
                            request.getUserIp(), PrivPredicate.LOAD);
                }
            } else {
                checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                        request.getTbl(),
                        request.getUserIp(), PrivPredicate.LOAD);
            }
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
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("failed to get rollbackTxn: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

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
            if (Config.enable_pipeline_load) {
                result.setPipelineParams(pipelineStreamLoadPutImpl(request));
            } else {
                result.setParams(streamLoadPutImpl(request));
            }
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

    /**
     * For first-class multi-table scenarios, we should store the mapping between Txn and data source type in a common
     * place. Since there is only Kafka now, we should do this first.
     */
    private void buildMultiTableStreamLoadTask(StreamLoadTask baseTaskInfo, long txnId) {
        try {
            RoutineLoadJob routineLoadJob = Env.getCurrentEnv().getRoutineLoadManager()
                    .getRoutineLoadJobByMultiLoadTaskTxnId(txnId);
            if (routineLoadJob == null) {
                return;
            }
            baseTaskInfo.setMultiTableBaseTaskInfo(routineLoadJob);
        } catch (Exception e) {
            LOG.warn("failed to build multi table stream load task: {}", e.getMessage());
        }
    }

    private void deleteMultiTableStreamLoadJobIndex(long txnId) {
        try {
            Env.getCurrentEnv().getRoutineLoadManager().removeMultiLoadTaskTxnIdToRoutineLoadJobId(txnId);
        } catch (Exception e) {
            LOG.warn("failed to delete multi table stream load job index: {}", e.getMessage());
        }
    }

    @Override
    public TStreamLoadMultiTablePutResult streamLoadMultiTablePut(TStreamLoadPutRequest request) {
        List<OlapTable> olapTables;
        Database db;
        String fullDbName;
        TStreamLoadMultiTablePutResult result = new TStreamLoadMultiTablePutResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        List<String> tableNames = request.getTableNames();
        try {
            if (CollectionUtils.isEmpty(tableNames)) {
                throw new MetaNotFoundException("table not found");
            }

            String cluster = request.getCluster();
            if (Strings.isNullOrEmpty(cluster)) {
                cluster = SystemInfoService.DEFAULT_CLUSTER;
            }
            fullDbName = ClusterNamespace.getFullName(cluster, request.getDb());
            db = Env.getCurrentEnv().getInternalCatalog().getDbNullable(fullDbName);
            if (db == null) {
                String dbName = fullDbName;
                if (Strings.isNullOrEmpty(request.getCluster())) {
                    dbName = request.getDb();
                }
                throw new UserException("unknown database, database=" + dbName);
            }
            // todo Whether there will be a large amount of data risk
            List<Table> tables = db.getTablesOrEmpty();
            if (CollectionUtils.isEmpty(tables)) {
                throw new MetaNotFoundException("table not found");
            }
            olapTables = new ArrayList<>(tableNames.size());
            Map<String, OlapTable> olapTableMap = tables.stream()
                    .filter(OlapTable.class::isInstance)
                    .map(OlapTable.class::cast)
                    .collect(Collectors.toMap(OlapTable::getName, olapTable -> olapTable));
            for (String tableName : tableNames) {
                if (null == olapTableMap.get(tableName)) {
                    throw new MetaNotFoundException("table not found, table name is " + tableName);
                }
                olapTables.add(olapTableMap.get(tableName));
            }
        } catch (UserException exception) {
            LOG.warn("failed to get stream load plan: {}", exception.getMessage());
            status = new TStatus(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(exception.getMessage());
            result.setStatus(status);
            return result;
        }
        long timeoutMs = request.isSetThriftRpcTimeoutMs() ? request.getThriftRpcTimeoutMs() : 5000;
        List planFragmentParamsList = new ArrayList<>(tableNames.size());
        List<Long> tableIds = olapTables.stream().map(OlapTable::getId).collect(Collectors.toList());
        // todo: if is multi table, we need consider the lock time and the timeout
        boolean enablePipelineLoad = Config.enable_pipeline_load;
        try {
            multiTableFragmentInstanceIdIndexMap.putIfAbsent(request.getTxnId(), 1);
            for (OlapTable table : olapTables) {
                int index = multiTableFragmentInstanceIdIndexMap.get(request.getTxnId());
                if (enablePipelineLoad) {
                    planFragmentParamsList.add(generatePipelineStreamLoadPut(request, db, fullDbName, table, timeoutMs,
                            index, true));
                } else {
                    TExecPlanFragmentParams planFragmentParams = generatePlanFragmentParams(request, db, fullDbName,
                            table, timeoutMs, index, true);

                    planFragmentParamsList.add(planFragmentParams);
                }
                multiTableFragmentInstanceIdIndexMap.put(request.getTxnId(), ++index);
            }
            Env.getCurrentGlobalTransactionMgr().getDatabaseTransactionMgr(db.getId())
                    .putTransactionTableNames(request.getTxnId(),
                            tableIds);
            LOG.debug("receive stream load multi table put request result: {}", result);
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(e.getClass().getSimpleName() + ": " + Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        if (enablePipelineLoad) {
            result.setPipelineParams(planFragmentParamsList);
            return result;
        }
        result.setParams(planFragmentParamsList);
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
        return generatePlanFragmentParams(request, db, fullDbName, (OlapTable) table, timeoutMs);
    }

    private TExecPlanFragmentParams generatePlanFragmentParams(TStreamLoadPutRequest request, Database db,
                                                               String fullDbName, OlapTable table,
                                                               long timeoutMs) throws UserException {
        return generatePlanFragmentParams(request, db, fullDbName, table, timeoutMs, 1, false);
    }

    private TExecPlanFragmentParams generatePlanFragmentParams(TStreamLoadPutRequest request, Database db,
                                                               String fullDbName, OlapTable table,
                                                               long timeoutMs, int multiTableFragmentInstanceIdIndex,
                                                               boolean isMultiTableRequest)
            throws UserException {
        if (!table.tryReadLock(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new UserException(
                    "get table read lock timeout, database=" + fullDbName + ",table=" + table.getName());
        }
        try {
            StreamLoadTask streamLoadTask = StreamLoadTask.fromTStreamLoadPutRequest(request);
            if (isMultiTableRequest) {
                buildMultiTableStreamLoadTask(streamLoadTask, request.getTxnId());
            }
            StreamLoadPlanner planner = new StreamLoadPlanner(db, table, streamLoadTask);
            TExecPlanFragmentParams plan = planner.plan(streamLoadTask.getId(), multiTableFragmentInstanceIdIndex);
            // add table indexes to transaction state
            TransactionState txnState = Env.getCurrentGlobalTransactionMgr()
                    .getTransactionState(db.getId(), request.getTxnId());
            if (txnState == null) {
                throw new UserException("txn does not exist: " + request.getTxnId());
            }
            txnState.addTableIndexes(table);
            plan.setTableName(table.getName());
            return plan;
        } finally {
            table.readUnlock();
        }
    }

    private TPipelineFragmentParams pipelineStreamLoadPutImpl(TStreamLoadPutRequest request) throws UserException {
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
        return this.generatePipelineStreamLoadPut(request, db, fullDbName, (OlapTable) table, timeoutMs,
                1, false);
    }

    private TPipelineFragmentParams generatePipelineStreamLoadPut(TStreamLoadPutRequest request, Database db,
                                                                  String fullDbName, OlapTable table,
                                                                  long timeoutMs,
                                                                  int multiTableFragmentInstanceIdIndex,
                                                                  boolean isMultiTableRequest)
            throws UserException {
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }
        if (!table.tryReadLock(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new UserException(
                    "get table read lock timeout, database=" + fullDbName + ",table=" + table.getName());
        }
        try {
            StreamLoadTask streamLoadTask = StreamLoadTask.fromTStreamLoadPutRequest(request);
            if (isMultiTableRequest) {
                buildMultiTableStreamLoadTask(streamLoadTask, request.getTxnId());
            }
            StreamLoadPlanner planner = new StreamLoadPlanner(db, table, streamLoadTask);
            TPipelineFragmentParams plan = planner.planForPipeline(streamLoadTask.getId(),
                    multiTableFragmentInstanceIdIndex);
            // add table indexes to transaction state
            TransactionState txnState = Env.getCurrentGlobalTransactionMgr()
                    .getTransactionState(db.getId(), request.getTxnId());
            if (txnState == null) {
                throw new UserException("txn does not exist: " + request.getTxnId());
            }
            txnState.addTableIndexes(table);
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
        TInitExternalCtlMetaResult result = new TInitExternalCtlMetaResult();
        try {
            ((ExternalCatalog) catalog).makeSureInitialized();
            result.setMaxJournalId(Env.getCurrentEnv().getMaxJournalId());
            result.setStatus(MasterCatalogExecutor.STATUS_OK);
        } catch (Throwable t) {
            LOG.warn("init catalog failed. catalog: {}", catalog.getName(), t);
            result.setStatus(Util.getRootCauseStack(t));
        }
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

        TInitExternalCtlMetaResult result = new TInitExternalCtlMetaResult();
        try {
            ((ExternalDatabase) db).makeSureInitialized();
            result.setMaxJournalId(Env.getCurrentEnv().getMaxJournalId());
            result.setStatus(MasterCatalogExecutor.STATUS_OK);
        } catch (Throwable t) {
            LOG.warn("init database failed. catalog.database: {}", catalog.getName(), db.getFullName(), t);
            result.setStatus(Util.getRootCauseStack(t));
        }
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

    public TGetTabletReplicaInfosResult getTabletReplicaInfos(TGetTabletReplicaInfosRequest request) {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive get replicas request: {}, backend: {}", request, clientAddr);
        TGetTabletReplicaInfosResult result = new TGetTabletReplicaInfosResult();
        List<Long> tabletIds = request.getTabletIds();
        Map<Long, List<TReplicaInfo>> tabletReplicaInfos = Maps.newHashMap();
        for (Long tabletId : tabletIds) {
            List<TReplicaInfo> replicaInfos = Lists.newArrayList();
            List<Replica> replicas = Env.getCurrentEnv().getCurrentInvertedIndex()
                    .getReplicasByTabletId(tabletId);
            for (Replica replica : replicas) {
                if (!replica.isNormal()) {
                    LOG.warn("replica {} not normal", replica.getId());
                    continue;
                }
                Backend backend = Env.getCurrentEnv().getCurrentSystemInfo().getBackend(replica.getBackendId());
                if (backend != null) {
                    TReplicaInfo replicaInfo = new TReplicaInfo();
                    replicaInfo.setHost(backend.getHost());
                    replicaInfo.setBePort(backend.getBePort());
                    replicaInfo.setHttpPort(backend.getHttpPort());
                    replicaInfo.setBrpcPort(backend.getBrpcPort());
                    replicaInfo.setReplicaId(replica.getId());
                    replicaInfos.add(replicaInfo);
                }
            }
            tabletReplicaInfos.put(tabletId, replicaInfos);
        }
        result.setTabletReplicaInfos(tabletReplicaInfos);
        result.setToken(Env.getCurrentEnv().getToken());
        result.setStatus(new TStatus(TStatusCode.OK));
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


        // step 1: check auth
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }
        if (Strings.isNullOrEmpty(request.getToken())) {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(), request.getTable(),
                    request.getUserIp(), PrivPredicate.SELECT);
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
        long tableId = -1;
        if (request.isSetTableId()) {
            tableId = request.getTableId();
        } else if (request.isSetTable()) {
            String tableName = request.getTable();
            Table table = db.getTableOrMetaException(tableName, TableType.OLAP);
            if (table == null) {
                throw new UserException("unknown table, table=" + tableName);
            }
            tableId = table.getId();
        }

        // step 6: get binlog
        long dbId = db.getId();
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

    // getSnapshot
    public TGetSnapshotResult getSnapshot(TGetSnapshotRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.trace("receive get snapshot info request: {}", request);

        TGetSnapshotResult result = new TGetSnapshotResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("failed to get getSnapshot: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

        try {
            result = getSnapshotImpl(request, clientAddr);
        } catch (UserException e) {
            LOG.warn("failed to get snapshot info: {}", e.getMessage());
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

    // getSnapshotImpl
    private TGetSnapshotResult getSnapshotImpl(TGetSnapshotRequest request, String clientIp)
            throws UserException {
        // Step 1: Check all required arg: user, passwd, db, label_name, snapshot_name, snapshot_type
        if (!request.isSetUser()) {
            throw new UserException("user is not set");
        }
        if (!request.isSetPasswd()) {
            throw new UserException("passwd is not set");
        }
        if (!request.isSetDb()) {
            throw new UserException("db is not set");
        }
        if (!request.isSetLabelName()) {
            throw new UserException("label_name is not set");
        }
        if (!request.isSetSnapshotName()) {
            throw new UserException("snapshot_name is not set");
        }
        if (!request.isSetSnapshotType()) {
            throw new UserException("snapshot_type is not set");
        } else if (request.getSnapshotType() != TSnapshotType.LOCAL) {
            throw new UserException("snapshot_type is not LOCAL");
        }

        // Step 2: check auth
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        LOG.info("get snapshot info, user: {}, db: {}, label_name: {}, snapshot_name: {}, snapshot_type: {}",
                request.getUser(), request.getDb(), request.getLabelName(), request.getSnapshotName(),
                request.getSnapshotType());
        if (Strings.isNullOrEmpty(request.getToken())) {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTable(), clientIp, PrivPredicate.LOAD);
        }

        // Step 3: get snapshot
        TGetSnapshotResult result = new TGetSnapshotResult();
        result.setStatus(new TStatus(TStatusCode.OK));
        Snapshot snapshot = Env.getCurrentEnv().getBackupHandler().getSnapshot(request.getLabelName());
        if (snapshot == null) {
            result.getStatus().setStatusCode(TStatusCode.SNAPSHOT_NOT_EXIST);
            result.getStatus().addToErrorMsgs("snapshot not exist");
        } else {
            result.setMeta(snapshot.getMeta());
            result.setJobInfo(snapshot.getJobInfo());
        }

        return result;
    }

    // Restore Snapshot
    @Override
    public TRestoreSnapshotResult restoreSnapshot(TRestoreSnapshotRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.trace("receive restore snapshot info request: {}", request);

        TRestoreSnapshotResult result = new TRestoreSnapshotResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("failed to get restoreSnapshot: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

        try {
            result = restoreSnapshotImpl(request, clientAddr);
        } catch (UserException e) {
            LOG.warn("failed to get snapshot info: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        } finally {
            ConnectContext.remove();
        }

        return result;
    }

    // restoreSnapshotImpl
    private TRestoreSnapshotResult restoreSnapshotImpl(TRestoreSnapshotRequest request, String clientIp)
            throws UserException {
        // Step 1: Check all required arg: user, passwd, db, label_name, repo_name, meta, info
        if (!request.isSetUser()) {
            throw new UserException("user is not set");
        }
        if (!request.isSetPasswd()) {
            throw new UserException("passwd is not set");
        }
        if (!request.isSetDb()) {
            throw new UserException("db is not set");
        }
        if (!request.isSetLabelName()) {
            throw new UserException("label_name is not set");
        }
        if (!request.isSetRepoName()) {
            throw new UserException("repo_name is not set");
        }
        if (!request.isSetMeta()) {
            throw new UserException("meta is not set");
        }
        if (!request.isSetJobInfo()) {
            throw new UserException("job_info is not set");
        }

        // Step 2: check auth
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        if (Strings.isNullOrEmpty(request.getToken())) {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTable(), clientIp, PrivPredicate.LOAD);
        }

        // Step 3: get snapshot
        TRestoreSnapshotResult result = new TRestoreSnapshotResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        LabelName label = new LabelName(request.getDb(), request.getLabelName());
        String repoName = request.getRepoName();
        Map<String, String> properties = request.getProperties();
        AbstractBackupTableRefClause restoreTableRefClause = null;
        if (request.isSetTableRefs()) {
            List<TableRef> tableRefs = new ArrayList<>();
            for (TTableRef tTableRef : request.getTableRefs()) {
                tableRefs.add(new TableRef(new TableName(tTableRef.getTable()), tTableRef.getAliasName()));
            }

            if (tableRefs.size() > 0) {
                boolean isExclude = false;
                restoreTableRefClause = new AbstractBackupTableRefClause(isExclude, tableRefs);
            }
        }
        RestoreStmt restoreStmt = new RestoreStmt(label, repoName, restoreTableRefClause, properties, request.getMeta(),
                request.getJobInfo());
        restoreStmt.setIsBeingSynced();
        LOG.trace("restore snapshot info, restoreStmt: {}", restoreStmt);
        try {
            ConnectContext ctx = ConnectContext.get();
            if (ctx == null) {
                ctx = new ConnectContext();
                ctx.setThreadLocalInfo();
            }
            ctx.setCluster(cluster);
            ctx.setQualifiedUser(request.getUser());
            UserIdentity currentUserIdentity = new UserIdentity(request.getUser(), "%");
            currentUserIdentity.setIsAnalyzed();
            ctx.setCurrentUserIdentity(currentUserIdentity);

            Analyzer analyzer = new Analyzer(ctx.getEnv(), ctx);
            restoreStmt.analyze(analyzer);
            DdlExecutor.execute(Env.getCurrentEnv(), restoreStmt);
        } catch (UserException e) {
            LOG.warn("failed to get snapshot info: {}", e.getMessage());
            status.setStatusCode(TStatusCode.ANALYSIS_ERROR);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
        }

        return result;
    }

    public TGetMasterTokenResult getMasterToken(TGetMasterTokenRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive get master token request: {}", request);

        TGetMasterTokenResult result = new TGetMasterTokenResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        if (!Env.getCurrentEnv().isMaster()) {
            status.setStatusCode(TStatusCode.NOT_MASTER);
            status.addToErrorMsgs(NOT_MASTER_ERR_MSG);
            result.setMasterAddress(getMasterAddress());
            LOG.error("failed to get getMasterToken: {}", NOT_MASTER_ERR_MSG);
            return result;
        }

        try {
            checkPassword(request.getCluster(), request.getUser(), request.getPassword(), clientAddr);
            result.setToken(Env.getCurrentEnv().getToken());
        } catch (AuthenticationException e) {
            LOG.warn("failed to get master token: {}", e.getMessage());
            status.setStatusCode(TStatusCode.NOT_AUTHORIZED);
            status.addToErrorMsgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatusCode(TStatusCode.INTERNAL_ERROR);
            status.addToErrorMsgs(Strings.nullToEmpty(e.getMessage()));
        }

        return result;
    }

    // getBinlogLag
    public TGetBinlogLagResult getBinlogLag(TGetBinlogRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive get binlog request: {}", request);

        TGetBinlogLagResult result = new TGetBinlogLagResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        try {
            result = getBinlogLagImpl(request, clientAddr);
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

    private TGetBinlogLagResult getBinlogLagImpl(TGetBinlogRequest request, String clientIp) throws UserException {
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


        // step 1: check auth
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }
        if (Strings.isNullOrEmpty(request.getToken())) {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(), request.getTable(),
                    request.getUserIp(), PrivPredicate.SELECT);
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
        long tableId = -1;
        if (request.isSetTableId()) {
            tableId = request.getTableId();
        } else if (request.isSetTable()) {
            String tableName = request.getTable();
            Table table = db.getTableOrMetaException(tableName, TableType.OLAP);
            if (table == null) {
                throw new UserException("unknown table, table=" + tableName);
            }
            tableId = table.getId();
        }

        // step 6: get binlog
        long dbId = db.getId();
        TGetBinlogLagResult result = new TGetBinlogLagResult();
        result.setStatus(new TStatus(TStatusCode.OK));
        long prevCommitSeq = request.getPrevCommitSeq();

        Pair<TStatus, Long> statusLagPair = env.getBinlogManager().getBinlogLag(dbId, tableId, prevCommitSeq);
        TStatus status = statusLagPair.first;
        if (status != null && status.getStatusCode() != TStatusCode.OK) {
            result.setStatus(status);
        }
        Long binlogLag = statusLagPair.second;
        if (binlogLag != null) {
            result.setLag(binlogLag);
        }

        return result;
    }

    @Override
    public TStatus updateStatsCache(TUpdateFollowerStatsCacheRequest request) throws TException {
        StatisticsCacheKey key = GsonUtils.GSON.fromJson(request.key, StatisticsCacheKey.class);
        /*
         TODO: Need to handle minExpr and maxExpr, so that we can generate the columnStatistic
          here and use putCache to update cached directly.
         ColumnStatistic columnStatistic = GsonUtils.GSON.fromJson(request.colStats, ColumnStatistic.class);
         Env.getCurrentEnv().getStatisticsCache().putCache(key, columnStatistic);
        */
        Env.getCurrentEnv().getStatisticsCache().refreshColStatsSync(key.tableId, key.idxId, key.colName);
        return new TStatus(TStatusCode.OK);
    }
}
