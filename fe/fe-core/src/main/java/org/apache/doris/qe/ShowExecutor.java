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

package org.apache.doris.qe;

import org.apache.doris.analysis.AdminDiagnoseTabletStmt;
import org.apache.doris.analysis.AdminShowConfigStmt;
import org.apache.doris.analysis.AdminShowReplicaDistributionStmt;
import org.apache.doris.analysis.AdminShowReplicaStatusStmt;
import org.apache.doris.analysis.AdminShowTabletStorageFormatStmt;
import org.apache.doris.analysis.DescribeStmt;
import org.apache.doris.analysis.HelpStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.ShowAlterStmt;
import org.apache.doris.analysis.ShowAuthorStmt;
import org.apache.doris.analysis.ShowBackendsStmt;
import org.apache.doris.analysis.ShowBackupStmt;
import org.apache.doris.analysis.ShowBrokerStmt;
import org.apache.doris.analysis.ShowClusterStmt;
import org.apache.doris.analysis.ShowCollationStmt;
import org.apache.doris.analysis.ShowColumnStatsStmt;
import org.apache.doris.analysis.ShowColumnStmt;
import org.apache.doris.analysis.ShowCreateDbStmt;
import org.apache.doris.analysis.ShowCreateFunctionStmt;
import org.apache.doris.analysis.ShowCreateRoutineLoadStmt;
import org.apache.doris.analysis.ShowCreateTableStmt;
import org.apache.doris.analysis.ShowDataSkewStmt;
import org.apache.doris.analysis.ShowDataStmt;
import org.apache.doris.analysis.ShowDbIdStmt;
import org.apache.doris.analysis.ShowDbStmt;
import org.apache.doris.analysis.ShowDeleteStmt;
import org.apache.doris.analysis.ShowDynamicPartitionStmt;
import org.apache.doris.analysis.ShowEncryptKeysStmt;
import org.apache.doris.analysis.ShowEnginesStmt;
import org.apache.doris.analysis.ShowExportStmt;
import org.apache.doris.analysis.ShowFrontendsStmt;
import org.apache.doris.analysis.ShowFunctionsStmt;
import org.apache.doris.analysis.ShowGrantsStmt;
import org.apache.doris.analysis.ShowIndexStmt;
import org.apache.doris.analysis.ShowLastInsertStmt;
import org.apache.doris.analysis.ShowLoadProfileStmt;
import org.apache.doris.analysis.ShowLoadStmt;
import org.apache.doris.analysis.ShowLoadWarningsStmt;
import org.apache.doris.analysis.ShowMigrationsStmt;
import org.apache.doris.analysis.ShowPartitionIdStmt;
import org.apache.doris.analysis.ShowPartitionsStmt;
import org.apache.doris.analysis.ShowPluginsStmt;
import org.apache.doris.analysis.ShowProcStmt;
import org.apache.doris.analysis.ShowProcesslistStmt;
import org.apache.doris.analysis.ShowQueryProfileStmt;
import org.apache.doris.analysis.ShowRepositoriesStmt;
import org.apache.doris.analysis.ShowResourcesStmt;
import org.apache.doris.analysis.ShowRestoreStmt;
import org.apache.doris.analysis.ShowRolesStmt;
import org.apache.doris.analysis.ShowRollupStmt;
import org.apache.doris.analysis.ShowRoutineLoadStmt;
import org.apache.doris.analysis.ShowRoutineLoadTaskStmt;
import org.apache.doris.analysis.ShowSmallFilesStmt;
import org.apache.doris.analysis.ShowSnapshotStmt;
import org.apache.doris.analysis.ShowSqlBlockRuleStmt;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.ShowStreamLoadStmt;
import org.apache.doris.analysis.ShowSyncJobStmt;
import org.apache.doris.analysis.ShowTableCreationStmt;
import org.apache.doris.analysis.ShowTableIdStmt;
import org.apache.doris.analysis.ShowTableStatsStmt;
import org.apache.doris.analysis.ShowTableStatusStmt;
import org.apache.doris.analysis.ShowTableStmt;
import org.apache.doris.analysis.ShowTabletStmt;
import org.apache.doris.analysis.ShowTransactionStmt;
import org.apache.doris.analysis.ShowTrashDiskStmt;
import org.apache.doris.analysis.ShowTrashStmt;
import org.apache.doris.analysis.ShowUserPropertyStmt;
import org.apache.doris.analysis.ShowVariablesStmt;
import org.apache.doris.analysis.ShowViewStmt;
import org.apache.doris.backup.AbstractJob;
import org.apache.doris.backup.BackupJob;
import org.apache.doris.backup.Repository;
import org.apache.doris.backup.RestoreJob;
import org.apache.doris.blockrule.SqlBlockRule;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.EncryptKey;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MetadataViewer;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.catalog.View;
import org.apache.doris.clone.DynamicPartitionScheduler;
import org.apache.doris.cluster.BaseParam;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.proc.BackendsProcDir;
import org.apache.doris.common.proc.FrontendsProcNode;
import org.apache.doris.common.proc.LoadProcDir;
import org.apache.doris.common.proc.PartitionsProcDir;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.RollupProcDir;
import org.apache.doris.common.proc.SchemaChangeProcDir;
import org.apache.doris.common.proc.TabletsProcDir;
import org.apache.doris.common.proc.TrashProcDir;
import org.apache.doris.common.proc.TrashProcNode;
import org.apache.doris.common.profile.ProfileTreeNode;
import org.apache.doris.common.profile.ProfileTreePrinter;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.external.iceberg.IcebergTableCreationRecord;
import org.apache.doris.load.DeleteHandler;
import org.apache.doris.load.ExportJob;
import org.apache.doris.load.ExportMgr;
import org.apache.doris.load.Load;
import org.apache.doris.load.LoadErrorHub;
import org.apache.doris.load.LoadErrorHub.HubType;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.LoadJob.JobState;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Diagnoser;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentClient;
import org.apache.doris.thrift.TCheckStorageFormatResult;
import org.apache.doris.thrift.TUnit;
import org.apache.doris.transaction.GlobalTransactionMgr;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

// Execute one show statement.
public class ShowExecutor {
    private static final Logger LOG = LogManager.getLogger(ShowExecutor.class);
    private static final List<List<String>> EMPTY_SET = Lists.newArrayList();

    private ConnectContext ctx;
    private ShowStmt stmt;
    private ShowResultSet resultSet;

    public ShowExecutor(ConnectContext ctx, ShowStmt stmt) {
        this.ctx = ctx;
        this.stmt = stmt;
        resultSet = null;
    }

    public ShowResultSet execute() throws AnalysisException {
        if (stmt instanceof ShowRollupStmt) {
            handleShowRollup();
        } else if (stmt instanceof ShowAuthorStmt) {
            handleShowAuthor();
        } else if (stmt instanceof ShowProcStmt) {
            handleShowProc();
        } else if (stmt instanceof HelpStmt) {
            handleHelp();
        } else if (stmt instanceof ShowDbStmt) {
            handleShowDb();
        } else if (stmt instanceof ShowDbIdStmt) {
            handleShowDbId();
        } else if (stmt instanceof ShowTableStmt) {
            handleShowTable();
        } else if (stmt instanceof ShowTableStatusStmt) {
            handleShowTableStatus();
        } else if (stmt instanceof ShowTableIdStmt) {
            handleShowTableId();
        } else if (stmt instanceof DescribeStmt) {
            handleDescribe();
        } else if (stmt instanceof ShowCreateTableStmt) {
            handleShowCreateTable();
        } else if (stmt instanceof ShowCreateDbStmt) {
            handleShowCreateDb();
        } else if (stmt instanceof ShowProcesslistStmt) {
            handleShowProcesslist();
        } else if (stmt instanceof ShowEnginesStmt) {
            handleShowEngines();
        } else if (stmt instanceof ShowFunctionsStmt) {
            handleShowFunctions();
        } else if (stmt instanceof ShowCreateFunctionStmt) {
            handleShowCreateFunction();
        } else if (stmt instanceof ShowEncryptKeysStmt) {
            handleShowEncryptKeys();
        } else if (stmt instanceof ShowVariablesStmt) {
            handleShowVariables();
        } else if (stmt instanceof ShowColumnStmt) {
            handleShowColumn();
        } else if (stmt instanceof ShowLoadStmt) {
            handleShowLoad();
        } else if (stmt instanceof ShowStreamLoadStmt) {
            handleShowStreamLoad();
        } else if (stmt instanceof ShowLoadWarningsStmt) {
            handleShowLoadWarnings();
        } else if (stmt instanceof ShowRoutineLoadStmt) {
            handleShowRoutineLoad();
        } else if (stmt instanceof ShowRoutineLoadTaskStmt) {
            handleShowRoutineLoadTask();
        } else if (stmt instanceof ShowCreateRoutineLoadStmt) {
            handleShowCreateRoutineLoad();
        } else if (stmt instanceof ShowDeleteStmt) {
            handleShowDelete();
        } else if (stmt instanceof ShowAlterStmt) {
            handleShowAlter();
        } else if (stmt instanceof ShowUserPropertyStmt) {
            handleShowUserProperty();
        } else if (stmt instanceof ShowDataStmt) {
            handleShowData();
        } else if (stmt instanceof ShowCollationStmt) {
            handleShowCollation();
        } else if (stmt instanceof ShowPartitionsStmt) {
            handleShowPartitions();
        } else if (stmt instanceof ShowPartitionIdStmt) {
            handleShowPartitionId();
        } else if (stmt instanceof ShowTabletStmt) {
            handleShowTablet();
        } else if (stmt instanceof ShowBackupStmt) {
            handleShowBackup();
        } else if (stmt instanceof ShowRestoreStmt) {
            handleShowRestore();
        } else if (stmt instanceof ShowClusterStmt) {
            handleShowCluster();
        } else if (stmt instanceof ShowMigrationsStmt) {
            handleShowMigrations();
        } else if (stmt instanceof ShowBrokerStmt) {
            handleShowBroker();
        } else if (stmt instanceof ShowResourcesStmt) {
            handleShowResources();
        } else if (stmt instanceof ShowExportStmt) {
            handleShowExport();
        } else if (stmt instanceof ShowBackendsStmt) {
            handleShowBackends();
        } else if (stmt instanceof ShowFrontendsStmt) {
            handleShowFrontends();
        } else if (stmt instanceof ShowRepositoriesStmt) {
            handleShowRepositories();
        } else if (stmt instanceof ShowSnapshotStmt) {
            handleShowSnapshot();
        } else if (stmt instanceof ShowGrantsStmt) {
            handleShowGrants();
        } else if (stmt instanceof ShowRolesStmt) {
            handleShowRoles();
        } else if (stmt instanceof ShowTrashStmt) {
            handleShowTrash();
        } else if (stmt instanceof ShowTrashDiskStmt) {
            handleShowTrashDisk();
        } else if (stmt instanceof AdminShowReplicaStatusStmt) {
            handleAdminShowTabletStatus();
        } else if (stmt instanceof AdminShowReplicaDistributionStmt) {
            handleAdminShowTabletDistribution();
        } else if (stmt instanceof AdminShowConfigStmt) {
            handleAdminShowConfig();
        } else if (stmt instanceof ShowSmallFilesStmt) {
            handleShowSmallFiles();
        } else if (stmt instanceof ShowDynamicPartitionStmt) {
            handleShowDynamicPartition();
        } else if (stmt instanceof ShowIndexStmt) {
            handleShowIndex();
        } else if (stmt instanceof ShowViewStmt) {
            handleShowView();
        } else if (stmt instanceof ShowTransactionStmt) {
            handleShowTransaction();
        } else if (stmt instanceof ShowPluginsStmt) {
            handleShowPlugins();
        } else if (stmt instanceof ShowQueryProfileStmt) {
            handleShowQueryProfile();
        } else if (stmt instanceof ShowLoadProfileStmt) {
            handleShowLoadProfile();
        } else if (stmt instanceof ShowDataSkewStmt) {
            handleShowDataSkew();
        } else if (stmt instanceof ShowSyncJobStmt) {
            handleShowSyncJobs();
        } else if (stmt instanceof ShowSqlBlockRuleStmt) {
            handleShowSqlBlockRule();
        } else if (stmt instanceof ShowTableStatsStmt) {
            handleShowTableStats();
        } else if (stmt instanceof ShowColumnStatsStmt) {
            handleShowColumnStats();
        } else if (stmt instanceof ShowTableCreationStmt) {
            handleShowTableCreation();
        } else if (stmt instanceof ShowLastInsertStmt) {
            handleShowLastInsert();
        } else if (stmt instanceof AdminShowTabletStorageFormatStmt) {
            handleAdminShowTabletStorageFormat();
        } else if (stmt instanceof AdminDiagnoseTabletStmt) {
            handleAdminDiagnoseTablet();
        } else {
            handleEmtpy();
        }

        return resultSet;
    }

    private void handleShowRollup() {
        // TODO: not implemented yet
        ShowRollupStmt showRollupStmt = (ShowRollupStmt) stmt;
        List<List<String>> rowSets = Lists.newArrayList();
        resultSet = new ShowResultSet(showRollupStmt.getMetaData(), rowSets);
    }

    // Handle show authors
    private void handleShowProcesslist() {
        ShowProcesslistStmt showStmt = (ShowProcesslistStmt) stmt;
        List<List<String>> rowSet = Lists.newArrayList();

        List<ConnectContext.ThreadInfo> threadInfos = ctx.getConnectScheduler().listConnection(ctx.getQualifiedUser());
        long nowMs = System.currentTimeMillis();
        for (ConnectContext.ThreadInfo info : threadInfos) {
            rowSet.add(info.toRow(nowMs));
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rowSet);
    }

    // Handle show authors
    private void handleEmtpy() {
        // Only success
        resultSet = new ShowResultSet(stmt.getMetaData(), EMPTY_SET);
    }

    // Handle show authors
    private void handleShowAuthor() {
        ShowAuthorStmt showAuthorStmt = (ShowAuthorStmt) stmt;
        List<List<String>> rowSet = Lists.newArrayList();
        // Only success
        resultSet = new ShowResultSet(showAuthorStmt.getMetaData(), rowSet);
    }

    // Handle show engines
    private void handleShowEngines() {
        ShowEnginesStmt showStmt = (ShowEnginesStmt) stmt;
        List<List<String>> rowSet = Lists.newArrayList();
        rowSet.add(Lists.newArrayList("Olap engine", "YES", "Default storage engine of palo", "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("MySQL", "YES", "MySQL server which data is in it", "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("ELASTICSEARCH", "YES", "ELASTICSEARCH cluster which data is in it", "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("HIVE", "YES", "HIVE database which data is in it", "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("ICEBERG", "YES", "ICEBERG data lake which data is in it", "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("ODBC", "YES", "ODBC driver which data we can connect", "NO", "NO", "NO"));
        
        // Only success
        resultSet = new ShowResultSet(showStmt.getMetaData(), rowSet);
    }

    // Handle show functions
    private void handleShowFunctions() throws AnalysisException {
        ShowFunctionsStmt showStmt = (ShowFunctionsStmt) stmt;
        Database db = ctx.getCatalog().getDbOrAnalysisException(showStmt.getDbName());
        List<Function> functions = showStmt.getIsBuiltin() ? ctx.getCatalog().getBuiltinFunctions() :
            db.getFunctions();

        List<List<Comparable>> rowSet = Lists.newArrayList();
        for (Function function : functions) {
            List<Comparable> row = function.getInfo(showStmt.getIsVerbose());
            // like predicate
            if (showStmt.getWild() == null || showStmt.like(function.functionName())) {
                rowSet.add(row);
            }
        }

        // sort function rows by first column asc
        ListComparator<List<Comparable>> comparator = null;
        OrderByPair orderByPair = new OrderByPair(0, false);
        comparator = new ListComparator<>(orderByPair);
        Collections.sort(rowSet, comparator);
        List<List<String>> resultRowSet = Lists.newArrayList();

        Set<String> functionNameSet = new HashSet<>();
        for (List<Comparable> row : rowSet) {
            List<String> resultRow = Lists.newArrayList();
            // if not verbose, remove duplicate function name
            if (functionNameSet.contains(row.get(0).toString())) {
                continue;
            }
            for (Comparable column : row) {
                resultRow.add(column.toString());
            }
            resultRowSet.add(resultRow);
            functionNameSet.add(resultRow.get(0));
        }

        // Only success
        ShowResultSetMetaData showMetaData = showStmt.getIsVerbose() ? showStmt.getMetaData() :
            ShowResultSetMetaData.builder()
                .addColumn(new Column("Function Name", ScalarType.createVarchar(256))).build();
        resultSet = new ShowResultSet(showMetaData, resultRowSet);
    }

    // Handle show create function
    private void handleShowCreateFunction() throws AnalysisException {
        ShowCreateFunctionStmt showCreateFunctionStmt = (ShowCreateFunctionStmt) stmt;
        Database db = ctx.getCatalog().getDbOrAnalysisException(showCreateFunctionStmt.getDbName());

        Function function = db.getFunction(showCreateFunctionStmt.getFunction());
        List<List<String>> resultRowSet = Lists.newArrayList();
        List<String> resultRow = Lists.newArrayList();
        resultRow.add(function.signatureString());
        resultRow.add(function.toSql(false));
        resultRowSet.add(resultRow);
        resultSet = new ShowResultSet(showCreateFunctionStmt.getMetaData(), resultRowSet);
    }

    // Handle show encryptkeys
    private void handleShowEncryptKeys() throws AnalysisException {
        ShowEncryptKeysStmt showStmt = (ShowEncryptKeysStmt) stmt;
        Database db = ctx.getCatalog().getDbOrAnalysisException(showStmt.getDbName());
        List<EncryptKey> encryptKeys = db.getEncryptKeys();

        List<List<Comparable>> rowSet = Lists.newArrayList();
        for (EncryptKey encryptKey : encryptKeys) {
            List<Comparable> row = encryptKey.getInfo();
            // like predicate
            if (showStmt.getWild() == null || showStmt.like(encryptKey.getEncryptKeyName().getKeyName())) {
                rowSet.add(row);
            }

        }

        // sort function rows by first column asc
        ListComparator<List<Comparable>> comparator = null;
        OrderByPair orderByPair = new OrderByPair(0, false);
        comparator = new ListComparator<>(orderByPair);
        Collections.sort(rowSet, comparator);
        List<List<String>> resultRowSet = Lists.newArrayList();

        Set<String> encryptKeyNameSet = new HashSet<>();
        for (List<Comparable> row : rowSet) {
            List<String> resultRow = Lists.newArrayList();
            for (Comparable column : row) {
                resultRow.add(column.toString());
            }
            resultRowSet.add(resultRow);
            encryptKeyNameSet.add(resultRow.get(0));
        }

        ShowResultSetMetaData showMetaData = showStmt.getMetaData();
        resultSet = new ShowResultSet(showMetaData, resultRowSet);
    }

    private void handleShowProc() throws AnalysisException {
        ShowProcStmt showProcStmt = (ShowProcStmt) stmt;
        ShowResultSetMetaData metaData = showProcStmt.getMetaData();
        ProcNodeInterface procNode = showProcStmt.getNode();

        List<List<String>> finalRows = procNode.fetchResult().getRows();
        // if this is superuser, hide ip and host info form backends info proc
        if (procNode instanceof BackendsProcDir) {
            if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(),
                                                                       PrivPredicate.OPERATOR)) {
                // hide host info
                for (List<String> row : finalRows) {
                    row.remove(BackendsProcDir.HOSTNAME_INDEX);
                }

                // mod meta data
                metaData.removeColumn(BackendsProcDir.HOSTNAME_INDEX);
            }
        }

        resultSet = new ShowResultSet(metaData, finalRows);
    }

    // Show clusters
    private void handleShowCluster() throws AnalysisException {
        final ShowClusterStmt showStmt = (ShowClusterStmt) stmt;
        final List<List<String>> rows = Lists.newArrayList();
        final List<String> clusterNames = ctx.getCatalog().getClusterNames();

        final Set<String> clusterNameSet = Sets.newTreeSet();
        for (String cluster : clusterNames) {
            clusterNameSet.add(cluster);
        }

        for (String clusterName : clusterNameSet) {
            rows.add(Lists.newArrayList(clusterName));
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show clusters
    private void handleShowMigrations() throws AnalysisException {
        final ShowMigrationsStmt showStmt = (ShowMigrationsStmt) stmt;
        final List<List<String>> rows = Lists.newArrayList();
        final Set<BaseParam> infos = ctx.getCatalog().getMigrations();

        for (BaseParam param : infos) {
            final int percent = (int) (param.getFloatParam(0) * 100f);
            rows.add(Lists.newArrayList(param.getStringParam(0), param.getStringParam(1), param.getStringParam(2),
                                        String.valueOf(percent + "%")));
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowDbId() throws AnalysisException {
        ShowDbIdStmt showStmt = (ShowDbIdStmt) stmt;
        long dbId = showStmt.getDbId();
        List<List<String>> rows = Lists.newArrayList();
        Catalog catalog = ctx.getCatalog();
        Database database = catalog.getDbNullable(dbId);
        if (database != null) {
            List<String> row = new ArrayList<>();
            row.add(database.getFullName());
            rows.add(row);
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowTableId() throws AnalysisException {
        ShowTableIdStmt showStmt = (ShowTableIdStmt) stmt;
        long tableId = showStmt.getTableId();
        List<List<String>> rows = Lists.newArrayList();
        Catalog catalog = ctx.getCatalog();
        List<Long> dbIds = catalog.getDbIds();
        // TODO should use inverted index
        for (long dbId : dbIds) {
            Database database = catalog.getDbNullable(dbId);
            if (database == null) {
                continue;
            }
            Table table = database.getTableNullable(tableId);
            if (table != null) {
                List<String> row = new ArrayList<>();
                row.add(database.getFullName());
                row.add(table.getName());
                row.add(String.valueOf(database.getId()));
                rows.add(row);
                break;
            }
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowPartitionId() throws AnalysisException {
        ShowPartitionIdStmt showStmt = (ShowPartitionIdStmt) stmt;
        long partitionId = showStmt.getPartitionId();
        List<List<String>> rows = Lists.newArrayList();
        Catalog catalog = ctx.getCatalog();
        List<Long> dbIds = catalog.getDbIds();
        // TODO should use inverted index
        for (long dbId : dbIds) {
            Database database = catalog.getDbNullable(dbId);
            if (database == null) {
                continue;
            }
            List<Table> tables = database.getTables();
            for (Table tbl : tables) {
                if (tbl instanceof OlapTable) {
                    tbl.readLock();
                    try {
                        Partition partition = ((OlapTable) tbl).getPartition(partitionId);
                        if (partition != null) {
                            List<String> row = new ArrayList<>();
                            row.add(database.getFullName());
                            row.add(tbl.getName());
                            row.add(partition.getName());
                            row.add(String.valueOf(database.getId()));
                            row.add(String.valueOf(tbl.getId()));
                            rows.add(row);
                            break;
                        }
                    } finally {
                        tbl.readUnlock();
                    }
                }
            }
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show databases statement
    private void handleShowDb() throws AnalysisException {
        ShowDbStmt showDbStmt = (ShowDbStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        List<String> dbNames = ctx.getCatalog().getClusterDbNames(ctx.getClusterName());
        PatternMatcher matcher = null;
        if (showDbStmt.getPattern() != null) {
            matcher = PatternMatcher.createMysqlPattern(showDbStmt.getPattern(),
                                                        CaseSensibility.DATABASE.getCaseSensibility());
        }
        Set<String> dbNameSet = Sets.newTreeSet();
        for (String fullName : dbNames) {
            final String db = ClusterNamespace.getNameFromFullName(fullName);
            // Filter dbname
            if (matcher != null && !matcher.match(db)) {
                continue;
            }

            if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(), fullName,
                                                                   PrivPredicate.SHOW)) {
                continue;
            }

            dbNameSet.add(db);
        }

        for (String dbName : dbNameSet) {
            rows.add(Lists.newArrayList(dbName));
        }

        resultSet = new ShowResultSet(showDbStmt.getMetaData(), rows);
    }

    // Show table statement.
    private void handleShowTable() throws AnalysisException {
        ShowTableStmt showTableStmt = (ShowTableStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        Database db = ctx.getCatalog().getDbOrAnalysisException(showTableStmt.getDb());
        PatternMatcher matcher = null;
        if (showTableStmt.getPattern() != null) {
            matcher = PatternMatcher.createMysqlPattern(showTableStmt.getPattern(),
                    CaseSensibility.TABLE.getCaseSensibility());
        }
        for (Table tbl : db.getTables()) {
            if (matcher != null && !matcher.match(tbl.getName())) {
                continue;
            }
            // check tbl privs
            if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(),
                    db.getFullName(), tbl.getName(),
                    PrivPredicate.SHOW)) {
                continue;
            }
            if (showTableStmt.isVerbose()) {
                String storageFormat = "NONE";
                if (tbl instanceof OlapTable) {
                    storageFormat = ((OlapTable) tbl).getStorageFormat().toString();
                }
                rows.add(Lists.newArrayList(tbl.getName(), tbl.getMysqlType(), storageFormat));
            } else {
                rows.add(Lists.newArrayList(tbl.getName()));
            }
        }
        resultSet = new ShowResultSet(showTableStmt.getMetaData(), rows);
    }

    // Show table status statement.
    private void handleShowTableStatus() throws AnalysisException {
        ShowTableStatusStmt showStmt = (ShowTableStatusStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        Database db = ctx.getCatalog().getDbNullable(showStmt.getDb());
        if (db != null) {
            PatternMatcher matcher = null;
            if (showStmt.getPattern() != null) {
                matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern(),
                        CaseSensibility.TABLE.getCaseSensibility());
            }
            for (Table table : db.getTables()) {
                if (matcher != null && !matcher.match(table.getName())) {
                    continue;
                }

                // check tbl privs
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(),
                        db.getFullName(), table.getName(),
                        PrivPredicate.SHOW)) {
                    continue;
                }
                List<String> row = Lists.newArrayList();
                // Name
                row.add(table.getName());
                // Engine
                row.add(table.getEngine());
                // version
                row.add(null);
                // Row_format
                row.add(null);
                // Rows
                row.add(String.valueOf(table.getRowCount()));
                // Avg_row_length
                row.add(String.valueOf(table.getAvgRowLength()));
                // Data_length
                row.add(String.valueOf(table.getDataLength()));
                // Max_data_length
                row.add(null);
                // Index_length
                row.add(null);
                // Data_free
                row.add(null);
                // Auto_increment
                row.add(null);
                // Create_time
                row.add(TimeUtils.longToTimeString(table.getCreateTime() * 1000));
                // Update_time
                if (table.getUpdateTime() > 0) {
                    row.add(TimeUtils.longToTimeString(table.getUpdateTime()));
                } else {
                    row.add(null);
                }
                // Check_time
                if (table.getLastCheckTime() > 0) {
                    row.add(TimeUtils.longToTimeString(table.getLastCheckTime()));
                } else {
                    row.add(null);
                }
                // Collation
                row.add("utf-8");
                // Checksum
                row.add(null);
                // Create_options
                row.add(null);

                row.add(table.getComment());
                rows.add(row);
            }
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show variables like
    private void handleShowVariables() throws AnalysisException {
        ShowVariablesStmt showStmt = (ShowVariablesStmt) stmt;
        PatternMatcher matcher = null;
        if (showStmt.getPattern() != null) {
            matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern(),
                                                        CaseSensibility.VARIABLES.getCaseSensibility());
        }
        List<List<String>> rows = VariableMgr.dump(showStmt.getType(), ctx.getSessionVariable(), matcher);
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show create database
    private void handleShowCreateDb() throws AnalysisException {
        ShowCreateDbStmt showStmt = (ShowCreateDbStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        Database db = ctx.getCatalog().getDbOrAnalysisException(showStmt.getDb());
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE DATABASE `").append(ClusterNamespace.getNameFromFullName(showStmt.getDb())).append("`");
        if (db.getDbProperties().getProperties().size() > 0) {
            sb.append("\nPROPERTIES (\n");
            sb.append(new PrintableMap<>(db.getDbProperties().getProperties(), "=", true, true, false));
            sb.append("\n)");
        }

        rows.add(Lists.newArrayList(ClusterNamespace.getNameFromFullName(showStmt.getDb()), sb.toString()));
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show create table
    private void handleShowCreateTable() throws AnalysisException {
        ShowCreateTableStmt showStmt = (ShowCreateTableStmt) stmt;
        Database db = ctx.getCatalog().getDbOrAnalysisException(showStmt.getDb());
        Table table = db.getTableOrAnalysisException(showStmt.getTable());
        List<List<String>> rows = Lists.newArrayList();

        table.readLock();
        try {
            List<String> createTableStmt = Lists.newArrayList();
            Catalog.getDdlStmt(table, createTableStmt, null, null, false, true /* hide password */);
            if (createTableStmt.isEmpty()) {
                resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
                return;
            }

            if (table instanceof View) {
                rows.add(Lists.newArrayList(table.getName(), createTableStmt.get(0), "utf8", "utf8_general_ci"));
                resultSet = new ShowResultSet(ShowCreateTableStmt.getViewMetaData(), rows);
            } else {
                if (showStmt.isView()) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_OBJECT, showStmt.getDb(),
                                                        showStmt.getTable(), "VIEW");
                }
                rows.add(Lists.newArrayList(table.getName(), createTableStmt.get(0)));
                resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
            }
        } finally {
            table.readUnlock();
        }
    }

    // Describe statement
    private void handleDescribe() throws AnalysisException {
        DescribeStmt describeStmt = (DescribeStmt) stmt;
        resultSet = new ShowResultSet(describeStmt.getMetaData(), describeStmt.getResultRows());
    }

    // Show column statement.
    private void handleShowColumn() throws AnalysisException {
        ShowColumnStmt showStmt = (ShowColumnStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        Database db = ctx.getCatalog().getDbOrAnalysisException(showStmt.getDb());
        Table table = db.getTableOrAnalysisException(showStmt.getTable());
        PatternMatcher matcher = null;
        if (showStmt.getPattern() != null) {
            matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern(),
                    CaseSensibility.COLUMN.getCaseSensibility());
        }
        table.readLock();
        try {
            List<Column> columns = table.getBaseSchema();
            for (Column col : columns) {
                if (matcher != null && !matcher.match(col.getName())) {
                    continue;
                }
                final String columnName = col.getName();
                final String columnType = col.getOriginType().toString();
                final String isAllowNull = col.isAllowNull() ? "YES" : "NO";
                final String isKey = col.isKey() ? "YES" : "NO";
                final String defaultValue = col.getDefaultValue();
                final String aggType = col.getAggregationType() == null ? "" : col.getAggregationType().toSql();
                if (showStmt.isVerbose()) {
                    // Field Type Collation Null Key Default Extra
                    // Privileges Comment
                    rows.add(Lists.newArrayList(columnName,
                            columnType,
                            "",
                            isAllowNull,
                            isKey,
                            defaultValue,
                            aggType,
                            "",
                            col.getComment()));
                } else {
                    // Field Type Null Key Default Extra
                    rows.add(Lists.newArrayList(columnName,
                            columnType,
                            isAllowNull,
                            isKey,
                            defaultValue,
                            aggType));
                }
            }
        } finally {
            table.readUnlock();
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show index statement.
    private void handleShowIndex() throws AnalysisException {
        ShowIndexStmt showStmt = (ShowIndexStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        Database db = ctx.getCatalog().getDbOrAnalysisException(showStmt.getDbName());

        OlapTable table = db.getOlapTableOrAnalysisException(showStmt.getTableName().getTbl());
        table.readLock();
        try {
            List<Index> indexes = table.getIndexes();
            for (Index index : indexes) {
                rows.add(Lists.newArrayList(showStmt.getTableName().toString(), "", index.getIndexName(),
                        "", String.join(",", index.getColumns()), "", "", "", "",
                        "", index.getIndexType().name(), index.getComment()));
            }
        } finally {
            table.readUnlock();
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    //Show view statement.
    private void handleShowView() {
        ShowViewStmt showStmt = (ShowViewStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        List<View> matchViews = showStmt.getMatchViews();
        for (View view : matchViews) {
            view.readLock();
            try {
                List<String> createViewStmt = Lists.newArrayList();
                Catalog.getDdlStmt(view, createViewStmt, null, null, false, true /* hide password */);
                if (!createViewStmt.isEmpty()) {
                    rows.add(Lists.newArrayList(view.getName(), createViewStmt.get(0)));
                }
            } finally {
                view.readUnlock();
            }
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Handle help statement.
    private void handleHelp() {
        HelpStmt helpStmt = (HelpStmt) stmt;
        String mark = helpStmt.getMask();
        HelpModule module = HelpModule.getInstance();

        // Get topic
        HelpTopic topic = module.getTopic(mark);
        // Get by Keyword
        if (topic == null) {
            List<String> topics = module.listTopicByKeyword(mark);
            if (topics.size() == 0) {
                // assign to avoid code style problem
                topic = null;
            } else if (topics.size() == 1) {
                topic = module.getTopic(topics.get(0));
            } else {
                // Send topic list and category list
                List<List<String>> rows = Lists.newArrayList();
                for (String str : topics) {
                    rows.add(Lists.newArrayList(str, "N"));
                }
                List<String> categories = module.listCategoryByName(mark);
                for (String str : categories) {
                    rows.add(Lists.newArrayList(str, "Y"));
                }
                resultSet = new ShowResultSet(helpStmt.getKeywordMetaData(), rows);
                return;
            }
        }
        if (topic != null) {
            resultSet = new ShowResultSet(helpStmt.getMetaData(), Lists.<List<String>>newArrayList(
                    Lists.newArrayList(topic.getName(), topic.getDescription(), topic.getExample())));
        } else {
            List<String> categories = module.listCategoryByName(mark);
            if (categories.isEmpty()) {
                // If no category match for this name, return
                resultSet = new ShowResultSet(helpStmt.getKeywordMetaData(), EMPTY_SET);
            } else if (categories.size() > 1) {
                // Send category list
                resultSet = new ShowResultSet(helpStmt.getCategoryMetaData(),
                                              Lists.<List<String>>newArrayList(categories));
            } else {
                // Send topic list and sub-category list
                List<List<String>> rows = Lists.newArrayList();
                List<String> topics = module.listTopicByCategory(categories.get(0));
                for (String str : topics) {
                    rows.add(Lists.newArrayList(str, "N"));
                }
                List<String> subCategories = module.listCategoryByCategory(categories.get(0));
                for (String str : subCategories) {
                    rows.add(Lists.newArrayList(str, "Y"));
                }
                resultSet = new ShowResultSet(helpStmt.getKeywordMetaData(), rows);
            }
        }
    }

    // Show load statement.
    private void handleShowLoad() throws AnalysisException {
        ShowLoadStmt showStmt = (ShowLoadStmt) stmt;

        Catalog catalog = Catalog.getCurrentCatalog();
        Database db = catalog.getDbOrAnalysisException(showStmt.getDbName());
        long dbId = db.getId();

        // combine the List<LoadInfo> of load(v1) and loadManager(v2)
        Load load = catalog.getLoadInstance();
        List<List<Comparable>> loadInfos = load.getLoadJobInfosByDb(dbId, db.getFullName(),
                                                                    showStmt.getLabelValue(),
                                                                    showStmt.isAccurateMatch(),
                                                                    showStmt.getStates());
        Set<String> statesValue = showStmt.getStates() == null ? null : showStmt.getStates().stream()
                .map(entity -> entity.name())
                .collect(Collectors.toSet());
        loadInfos.addAll(catalog.getLoadManager().getLoadJobInfosByDb(dbId, showStmt.getLabelValue(),
                                                                      showStmt.isAccurateMatch(),
                                                                      statesValue));

        // order the result of List<LoadInfo> by orderByPairs in show stmt
        List<OrderByPair> orderByPairs = showStmt.getOrderByPairs();
        ListComparator<List<Comparable>> comparator = null;
        if (orderByPairs != null) {
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<List<Comparable>>(orderByPairs.toArray(orderByPairArr));
        } else {
            // sort by id asc
            comparator = new ListComparator<List<Comparable>>(0);
        }
        Collections.sort(loadInfos, comparator);

        List<List<String>> rows = Lists.newArrayList();
        for (List<Comparable> loadInfo : loadInfos) {
            List<String> oneInfo = new ArrayList<String>(loadInfo.size());

            // replace QUORUM_FINISHED -> FINISHED
            if (loadInfo.get(LoadProcDir.STATE_INDEX).equals(JobState.QUORUM_FINISHED.name())) {
                loadInfo.set(LoadProcDir.STATE_INDEX, JobState.FINISHED.name());
            }

            for (Comparable element : loadInfo) {
                oneInfo.add(element.toString());
            }
            rows.add(oneInfo);
        }

        // filter by limit
        long limit = showStmt.getLimit();
        long offset = showStmt.getOffset() == -1L ? 0 : showStmt.getOffset();
        if (offset >= rows.size()) {
            rows = Lists.newArrayList();
        } else if (limit != -1L) {
            if ((limit + offset) < rows.size()) {
                rows = rows.subList((int) offset, (int) (limit + offset));
            } else {
                rows = rows.subList((int) offset, rows.size());
            }
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show stream load statement.
    private void handleShowStreamLoad() throws AnalysisException {
        ShowStreamLoadStmt showStmt = (ShowStreamLoadStmt) stmt;

        Catalog catalog = Catalog.getCurrentCatalog();
        Database db = catalog.getDbOrAnalysisException(showStmt.getDbName());
        long dbId = db.getId();

        List<List<Comparable>> streamLoadRecords = catalog.getStreamLoadRecordMgr().getStreamLoadRecordByDb(dbId, showStmt.getLabelValue(), showStmt.isAccurateMatch(), showStmt.getState());

        // order the result of List<StreamLoadRecord> by orderByPairs in show stmt
        List<OrderByPair> orderByPairs = showStmt.getOrderByPairs();
        if (orderByPairs == null) {
            orderByPairs = showStmt.getOrderByFinishTime();
        }
        ListComparator<List<Comparable>> comparator = null;
        if (orderByPairs != null) {
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<List<Comparable>>(orderByPairs.toArray(orderByPairArr));
        } else {
            // sort by id asc
            comparator = new ListComparator<List<Comparable>>(0);
        }
        Collections.sort(streamLoadRecords, comparator);

        List<List<String>> rows = Lists.newArrayList();
        for (List<Comparable> streamLoadRecord : streamLoadRecords) {
            List<String> oneInfo = new ArrayList<String>(streamLoadRecord.size());

            for (Comparable element : streamLoadRecord) {
                oneInfo.add(element.toString());
            }
            rows.add(oneInfo);
        }

        // filter by limit
        long limit = showStmt.getLimit();
        long offset = showStmt.getOffset() == -1L ? 0 : showStmt.getOffset();
        if (offset >= rows.size()) {
            rows = Lists.newArrayList();
        } else if (limit != -1L) {
            if ((limit + offset) < rows.size()) {
                rows = rows.subList((int) offset, (int) (limit + offset));
            } else {
                rows = rows.subList((int) offset, rows.size());
            }
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowLoadWarnings() throws AnalysisException {
        ShowLoadWarningsStmt showWarningsStmt = (ShowLoadWarningsStmt) stmt;

        if (showWarningsStmt.getURL() != null) {
            handleShowLoadWarningsFromURL(showWarningsStmt, showWarningsStmt.getURL());
            return;
        }

        Catalog catalog = Catalog.getCurrentCatalog();
        Database db = catalog.getDbOrAnalysisException(showWarningsStmt.getDbName());

        long dbId = db.getId();
        Load load = catalog.getLoadInstance();
        long jobId = 0;
        LoadJob job = null;
        String label = null;
        if (showWarningsStmt.isFindByLabel()) {
            label = showWarningsStmt.getLabel();
            jobId = load.getLatestJobIdByLabel(dbId, showWarningsStmt.getLabel());
            job = load.getLoadJob(jobId);
            if (job == null) {
                throw new AnalysisException("job is not exist.");
            }
        } else {
            LOG.debug("load_job_id={}", jobId);
            jobId = showWarningsStmt.getJobId();
            job = load.getLoadJob(jobId);
            if (job == null) {
                throw new AnalysisException("job is not exist.");
            }
            label = job.getLabel();
            LOG.info("label={}", label);
        }

        // check auth
        Set<String> tableNames = job.getTableNames();
        if (tableNames.isEmpty()) {
            // forward compatibility
            if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(), db.getFullName(),
                                                                   PrivPredicate.SHOW)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                                                    ConnectContext.get().getQualifiedUser(),
                                                    db.getFullName());
            }
        } else {
            for (String tblName : tableNames) {
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), db.getFullName(),
                                                                        tblName, PrivPredicate.SHOW)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                                                        "SHOW LOAD WARNING",
                                                        ConnectContext.get().getQualifiedUser(),
                                                        ConnectContext.get().getRemoteIP(),
                                                        db.getFullName() + ": " + tblName);
                }
            }
        }

        LoadErrorHub.Param param = load.getLoadErrorHubInfo();
        if (param == null || param.getType() == HubType.NULL_TYPE) {
            throw new AnalysisException("no load error hub be supplied.");
        }
        LoadErrorHub errorHub = LoadErrorHub.createHub(param);
        List<LoadErrorHub.ErrorMsg> errors = errorHub.fetchLoadError(jobId);
        errorHub.close();

        List<List<String>> rows = Lists.newArrayList();
        for (LoadErrorHub.ErrorMsg error : errors) {
            List<String> oneInfo = Lists.newArrayList();
            oneInfo.add(String.valueOf(jobId));
            oneInfo.add(label);
            oneInfo.add(error.getMsg());
            rows.add(oneInfo);
        }

        long limit = showWarningsStmt.getLimitNum();
        if (limit != -1L && limit < rows.size()) {
            rows = rows.subList(0, (int) limit);
        }

        resultSet = new ShowResultSet(showWarningsStmt.getMetaData(), rows);
    }

    private void handleShowLoadWarningsFromURL(ShowLoadWarningsStmt showWarningsStmt, URL url)
            throws AnalysisException {
        String host = url.getHost();
        int port = url.getPort();
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        Backend be = infoService.getBackendWithHttpPort(host, port);
        if (be == null) {
            throw new AnalysisException(host + ":" + port + " is not a valid backend");
        }
        if (!be.isAlive()) {
            throw new AnalysisException("Backend " + host + ":" + port + " is not alive");
        }

        if (!url.getPath().equals("/api/_load_error_log")) {
            throw new AnalysisException(
                    "Invalid error log path: " + url.getPath() + ". path should be: /api/_load_error_log");
        }

        List<List<String>> rows = Lists.newArrayList();
        try {
            URLConnection urlConnection = url.openConnection();
            InputStream inputStream = urlConnection.getInputStream();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                int limit = 100;
                while (reader.ready() && limit > 0) {
                    String line = reader.readLine();
                    rows.add(Lists.newArrayList("-1", FeConstants.null_string, line));
                    limit--;
                }
            }
        } catch (Exception e) {
            LOG.warn("failed to get error log from url: " + url, e);
            throw new AnalysisException(
                    "failed to get error log from url: " + url + ". reason: " + e.getMessage());
        }

        resultSet = new ShowResultSet(showWarningsStmt.getMetaData(), rows);
    }

    private void handleShowRoutineLoad() throws AnalysisException {
        ShowRoutineLoadStmt showRoutineLoadStmt = (ShowRoutineLoadStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        // if job exists
        List<RoutineLoadJob> routineLoadJobList;
        try {
            PatternMatcher matcher = null;
            if (showRoutineLoadStmt.getPattern() != null) {
                matcher = PatternMatcher.createMysqlPattern(showRoutineLoadStmt.getPattern(),
                        CaseSensibility.ROUTINE_LOAD.getCaseSensibility());
            }
            routineLoadJobList = Catalog.getCurrentCatalog().getRoutineLoadManager()
                    .getJob(showRoutineLoadStmt.getDbFullName(),
                            showRoutineLoadStmt.getName(),
                            showRoutineLoadStmt.isIncludeHistory(), matcher);
        } catch (MetaNotFoundException e) {
            LOG.warn(e.getMessage(), e);
            throw new AnalysisException(e.getMessage());
        }

        if (routineLoadJobList != null) {
            String dbFullName = showRoutineLoadStmt.getDbFullName();
            String tableName = null;
            for (RoutineLoadJob routineLoadJob : routineLoadJobList) {
                // check auth
                try {
                    tableName = routineLoadJob.getTableName();
                } catch (MetaNotFoundException e) {
                    LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                                     .add("error_msg", "The table metadata of job has been changed. "
                                             + "The job will be cancelled automatically")
                                     .build(), e);
                }
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(),
                                                                        dbFullName,
                                                                        tableName,
                                                                        PrivPredicate.LOAD)) {
                    LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                                     .add("operator", "show routine load job")
                                     .add("user", ConnectContext.get().getQualifiedUser())
                                     .add("remote_ip", ConnectContext.get().getRemoteIP())
                                     .add("db_full_name", dbFullName)
                                     .add("table_name", tableName)
                                     .add("error_msg", "The table access denied"));
                    continue;
                }

                // get routine load info
                rows.add(routineLoadJob.getShowInfo());
            }
        }

        if (!Strings.isNullOrEmpty(showRoutineLoadStmt.getName()) && rows.size() == 0) {
            // if the jobName has been specified
            throw new AnalysisException("There is no job named " + showRoutineLoadStmt.getName()
                                                + " in db " + showRoutineLoadStmt.getDbFullName()
                    + ". Include history? " + showRoutineLoadStmt.isIncludeHistory());
        }
        resultSet = new ShowResultSet(showRoutineLoadStmt.getMetaData(), rows);
    }

    private void handleShowRoutineLoadTask() throws AnalysisException {
        ShowRoutineLoadTaskStmt showRoutineLoadTaskStmt = (ShowRoutineLoadTaskStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        // if job exists
        RoutineLoadJob routineLoadJob;
        try {
            routineLoadJob = Catalog.getCurrentCatalog().getRoutineLoadManager().getJob(showRoutineLoadTaskStmt.getDbFullName(),
                                                                                        showRoutineLoadTaskStmt.getJobName());
        } catch (MetaNotFoundException e) {
            LOG.warn(e.getMessage(), e);
            throw new AnalysisException(e.getMessage());
        }
        if (routineLoadJob == null) {
            throw new AnalysisException("The job named " + showRoutineLoadTaskStmt.getJobName() + "does not exists "
                                                + "or job state is stopped or cancelled");
        }

        // check auth
        String dbFullName = showRoutineLoadTaskStmt.getDbFullName();
        String tableName;
        try {
            tableName = routineLoadJob.getTableName();
        } catch (MetaNotFoundException e) {
            throw new AnalysisException("The table metadata of job has been changed. The job will be cancelled automatically", e);
        }
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(),
                                                                dbFullName,
                                                                tableName,
                                                                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(),
                                                dbFullName + ": " + tableName);
        }

        // get routine load task info
        rows.addAll(routineLoadJob.getTasksShowInfo());
        resultSet = new ShowResultSet(showRoutineLoadTaskStmt.getMetaData(), rows);
    }

    // Show user property statement
    private void handleShowUserProperty() throws AnalysisException {
        ShowUserPropertyStmt showStmt = (ShowUserPropertyStmt) stmt;
        resultSet = new ShowResultSet(showStmt.getMetaData(), showStmt.getRows());
    }

    // Show delete statement.
    private void handleShowDelete() throws AnalysisException {
        ShowDeleteStmt showStmt = (ShowDeleteStmt) stmt;

        Catalog catalog = Catalog.getCurrentCatalog();
        Database db = catalog.getDbOrAnalysisException(showStmt.getDbName());
        long dbId = db.getId();

        DeleteHandler deleteHandler = catalog.getDeleteHandler();
        Load load = catalog.getLoadInstance();
        List<List<Comparable>> deleteInfos = deleteHandler.getDeleteInfosByDb(dbId);
        List<List<String>> rows = Lists.newArrayList();
        for (List<Comparable> deleteInfo : deleteInfos) {
            List<String> oneInfo = new ArrayList<String>(deleteInfo.size());
            for (Comparable element : deleteInfo) {
                oneInfo.add(element.toString());
            }
            rows.add(oneInfo);
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show alter statement.
    private void handleShowAlter() throws AnalysisException {
        ShowAlterStmt showStmt = (ShowAlterStmt) stmt;
        ProcNodeInterface procNodeI = showStmt.getNode();
        Preconditions.checkNotNull(procNodeI);
        List<List<String>> rows;
        //Only SchemaChangeProc support where/order by/limit syntax 
        if (procNodeI instanceof SchemaChangeProcDir) {
            rows = ((SchemaChangeProcDir) procNodeI).fetchResultByFilter(showStmt.getFilterMap(),
                    showStmt.getOrderPairs(), showStmt.getLimitElement()).getRows();
        } else if (procNodeI instanceof RollupProcDir) {
            rows = ((RollupProcDir) procNodeI).fetchResultByFilter(showStmt.getFilterMap(),
                    showStmt.getOrderPairs(), showStmt.getLimitElement()).getRows();
        } else {
            rows = procNodeI.fetchResult().getRows();
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show alter statement.
    private void handleShowCollation() throws AnalysisException {
        ShowCollationStmt showStmt = (ShowCollationStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        List<String> row = Lists.newArrayList();
        // | utf8_general_ci | utf8 | 33 | Yes | Yes | 1 |
        row.add("utf8_general_ci");
        row.add("utf8");
        row.add("33");
        row.add("Yes");
        row.add("Yes");
        row.add("1");
        rows.add(row);
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowData() throws AnalysisException {
        ShowDataStmt showStmt = (ShowDataStmt) stmt;
        resultSet = new ShowResultSet(showStmt.getMetaData(), showStmt.getResultRows());
    }

    private void handleShowPartitions() throws AnalysisException {
        ShowPartitionsStmt showStmt = (ShowPartitionsStmt) stmt;
        ProcNodeInterface procNodeI = showStmt.getNode();
        Preconditions.checkNotNull(procNodeI);
        List<List<String>> rows = ((PartitionsProcDir) procNodeI).fetchResultByFilter(showStmt.getFilterMap(),
            showStmt.getOrderByPairs(), showStmt.getLimitElement()).getRows();
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowTablet() throws AnalysisException {
        ShowTabletStmt showStmt = (ShowTabletStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();

        Catalog catalog = Catalog.getCurrentCatalog();
        if (showStmt.isShowSingleTablet()) {
            long tabletId = showStmt.getTabletId();
            TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
            TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
            Long dbId = tabletMeta != null ? tabletMeta.getDbId() : TabletInvertedIndex.NOT_EXIST_VALUE;
            String dbName = FeConstants.null_string;
            Long tableId = tabletMeta != null ? tabletMeta.getTableId() : TabletInvertedIndex.NOT_EXIST_VALUE;
            String tableName = FeConstants.null_string;
            Long partitionId = tabletMeta != null ? tabletMeta.getPartitionId() : TabletInvertedIndex.NOT_EXIST_VALUE;
            String partitionName = FeConstants.null_string;
            Long indexId = tabletMeta != null ? tabletMeta.getIndexId() : TabletInvertedIndex.NOT_EXIST_VALUE;
            String indexName = FeConstants.null_string;
            Boolean isSync = true;

            int tabletIdx = -1;
            // check real meta
            do {
                Database db = catalog.getDbNullable(dbId);
                if (db == null) {
                    isSync = false;
                    break;
                }
                dbName = db.getFullName();
                Table table = db.getTableNullable(tableId);
                if (table == null || !(table instanceof OlapTable)) {
                    isSync = false;
                    break;
                }

                table.readLock();
                try {
                    tableName = table.getName();
                    OlapTable olapTable = (OlapTable) table;
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null) {
                        isSync = false;
                        break;
                    }
                    partitionName = partition.getName();

                    MaterializedIndex index = partition.getIndex(indexId);
                    if (index == null) {
                        isSync = false;
                        break;
                    }
                    indexName = olapTable.getIndexNameById(indexId);

                    Tablet tablet = index.getTablet(tabletId);
                    if (tablet == null) {
                        isSync = false;
                        break;
                    }

                    tabletIdx = index.getTabletOrderIdx(tablet.getId());

                    List<Replica> replicas = tablet.getReplicas();
                    for (Replica replica : replicas) {
                        Replica tmp = invertedIndex.getReplica(tabletId, replica.getBackendId());
                        if (tmp == null) {
                            isSync = false;
                            break;
                        }
                        // use !=, not equals(), because this should be the same object.
                        if (tmp != replica) {
                            isSync = false;
                            break;
                        }
                    }

                } finally {
                    table.readUnlock();
                }
            } while (false);

            String detailCmd = String.format("SHOW PROC '/dbs/%d/%d/partitions/%d/%d/%d';",
                                             dbId, tableId, partitionId, indexId, tabletId);
            rows.add(Lists.newArrayList(dbName, tableName, partitionName, indexName,
                    dbId.toString(), tableId.toString(),
                    partitionId.toString(), indexId.toString(),
                    isSync.toString(), String.valueOf(tabletIdx), detailCmd));
        } else {
            Database db = catalog.getDbOrAnalysisException(showStmt.getDbName());
            OlapTable olapTable = db.getOlapTableOrAnalysisException(showStmt.getTableName());

            olapTable.readLock();
            try {
                long sizeLimit = -1;
                if (showStmt.hasOffset() && showStmt.hasLimit()) {
                    sizeLimit = showStmt.getOffset() + showStmt.getLimit();
                } else if (showStmt.hasLimit()) {
                    sizeLimit = showStmt.getLimit();
                }
                boolean stop = false;
                Collection<Partition> partitions = new ArrayList<Partition>();
                if (showStmt.hasPartition()) {
                    PartitionNames partitionNames = showStmt.getPartitionNames();
                    for (String partName : partitionNames.getPartitionNames()) {
                        Partition partition = olapTable.getPartition(partName, partitionNames.isTemp());
                        if (partition == null) {
                            throw new AnalysisException("Unknown partition: " + partName);
                        }
                        partitions.add(partition);
                    }
                } else {
                    partitions = olapTable.getPartitions();
                }
                List<List<Comparable>> tabletInfos =  new ArrayList<>();
                String indexName = showStmt.getIndexName();
                long indexId = -1;
                if (indexName != null) {
                    Long id = olapTable.getIndexIdByName(indexName);
                    if (id == null) {
                        // invalid indexName
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_TABLE, showStmt.getIndexName(),
                                showStmt.getDbName());
                    }
                    indexId = id;
                }
                for (Partition partition : partitions) {
                    if (stop) {
                        break;
                    }
                    for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                        if (indexId > -1 && index.getId() != indexId) {
                            continue;
                        }
                        TabletsProcDir procDir = new TabletsProcDir(olapTable, index);
                        tabletInfos.addAll(procDir.fetchComparableResult(
                                showStmt.getVersion(), showStmt.getBackendId(), showStmt.getReplicaState()));
                        if (sizeLimit > -1 && tabletInfos.size() >= sizeLimit) {
                            stop = true;
                            break;
                        }
                    }
                }
                if (sizeLimit > -1 && tabletInfos.size() < sizeLimit) {
                    tabletInfos.clear();
                } else if (sizeLimit > -1) {
                    tabletInfos = tabletInfos.subList((int)showStmt.getOffset(), (int)sizeLimit);
                }

                // order by
                List<OrderByPair> orderByPairs = showStmt.getOrderByPairs();
                ListComparator<List<Comparable>> comparator = null;
                if (orderByPairs != null) {
                    OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
                    comparator = new ListComparator<>(orderByPairs.toArray(orderByPairArr));
                } else {
                    // order by tabletId, replicaId
                    comparator = new ListComparator<>(0, 1);
                }
                Collections.sort(tabletInfos, comparator);

                for (List<Comparable> tabletInfo : tabletInfos) {
                    List<String> oneTablet = new ArrayList<String>(tabletInfo.size());
                    for (Comparable column : tabletInfo) {
                        oneTablet.add(column.toString());
                    }
                    rows.add(oneTablet);
                }
            } finally {
                olapTable.readUnlock();
            }
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Handle show brokers
    private void handleShowBroker() {
        ShowBrokerStmt showStmt = (ShowBrokerStmt) stmt;
        List<List<String>> brokersInfo = Catalog.getCurrentCatalog().getBrokerMgr().getBrokersInfo();
        for (List<String> row : brokersInfo) {
            row.remove(BrokerMgr.HOSTNAME_INDEX);
        }

        // Only success
        resultSet = new ShowResultSet(showStmt.getMetaData(), brokersInfo);
    }

    // Handle show resources
    private void handleShowResources() {
        ShowResourcesStmt showStmt = (ShowResourcesStmt) stmt;
        List<List<Comparable>> resourcesInfos = Catalog.getCurrentCatalog().getResourceMgr()
                .getResourcesInfo(showStmt.getNameValue(),
                                showStmt.isAccurateMatch(),
                                showStmt.getTypeSet());

        // order the result of List<LoadInfo> by orderByPairs in show stmt
        List<OrderByPair> orderByPairs = showStmt.getOrderByPairs();
        ListComparator<List<Comparable>> comparator = null;
        if (orderByPairs != null) {
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<List<Comparable>>(orderByPairs.toArray(orderByPairArr));
        } else {
            // sort by name asc
            comparator = new ListComparator<List<Comparable>>(0);
        }
        Collections.sort(resourcesInfos, comparator);

        List<List<String>> rows = Lists.newArrayList();
        for (List<Comparable> resourceInfo : resourcesInfos) {
            List<String> oneResource = new ArrayList<String>(resourceInfo.size());

            for (Comparable element : resourceInfo) {
                oneResource.add(element.toString());
            }
            rows.add(oneResource);
        }

        // filter by limit
        long limit = showStmt.getLimit();
        long offset = showStmt.getOffset() == -1L ? 0 : showStmt.getOffset();
        if (offset >= rows.size()) {
            rows = Lists.newArrayList();
        } else if (limit != -1L) {
            if ((limit + offset) < rows.size()) {
                rows = rows.subList((int) offset, (int) (limit + offset));
            } else {
                rows = rows.subList((int) offset, rows.size());
            }
        }

        // Only success
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowExport() throws AnalysisException {
        ShowExportStmt showExportStmt = (ShowExportStmt) stmt;
        Catalog catalog = Catalog.getCurrentCatalog();
        Database db = catalog.getDbOrAnalysisException(showExportStmt.getDbName());
        long dbId = db.getId();

        ExportMgr exportMgr = catalog.getExportMgr();

        Set<ExportJob.JobState> states = null;
        ExportJob.JobState state = showExportStmt.getJobState();
        if (state != null) {
            states = Sets.newHashSet(state);
        }
        List<List<String>> infos = exportMgr.getExportJobInfosByIdOrState(
                dbId, showExportStmt.getJobId(), showExportStmt.getLabel(),showExportStmt.isLabelUseLike(),  states,
                showExportStmt.getOrderByPairs(), showExportStmt.getLimit());

        resultSet = new ShowResultSet(showExportStmt.getMetaData(), infos);
    }

    private void handleShowBackends() {
        final ShowBackendsStmt showStmt = (ShowBackendsStmt) stmt;
        List<List<String>> backendInfos = BackendsProcDir.getClusterBackendInfos(showStmt.getClusterName());

        for (List<String> row : backendInfos) {
            row.remove(BackendsProcDir.HOSTNAME_INDEX);
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), backendInfos);
    }

    private void handleShowFrontends() {
        final ShowFrontendsStmt showStmt = (ShowFrontendsStmt) stmt;
        List<List<String>> infos = Lists.newArrayList();
        FrontendsProcNode.getFrontendsInfo(Catalog.getCurrentCatalog(), infos);

        for (List<String> row : infos) {
            row.remove(FrontendsProcNode.HOSTNAME_INDEX);
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowRepositories() {
        final ShowRepositoriesStmt showStmt = (ShowRepositoriesStmt) stmt;
        List<List<String>> repoInfos = Catalog.getCurrentCatalog().getBackupHandler().getRepoMgr().getReposInfo();
        resultSet = new ShowResultSet(showStmt.getMetaData(), repoInfos);
    }

    private void handleShowSnapshot() throws AnalysisException {
        final ShowSnapshotStmt showStmt = (ShowSnapshotStmt) stmt;
        Repository repo = Catalog.getCurrentCatalog().getBackupHandler().getRepoMgr().getRepo(showStmt.getRepoName());
        if (repo == null) {
            throw new AnalysisException("Repository " + showStmt.getRepoName() + " does not exist");
        }

        List<List<String>> snapshotInfos = repo.getSnapshotInfos(showStmt.getSnapshotName(), showStmt.getTimestamp());
        resultSet = new ShowResultSet(showStmt.getMetaData(), snapshotInfos);
    }

    private void handleShowBackup() throws AnalysisException {
        ShowBackupStmt showStmt = (ShowBackupStmt) stmt;
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(showStmt.getDbName());

        List<AbstractJob> jobs = Catalog.getCurrentCatalog().getBackupHandler().getJobs(db.getId(), showStmt.getLabelPredicate());

        List<BackupJob> backupJobs = jobs.stream().filter(job -> job instanceof BackupJob)
                .map(job -> (BackupJob) job).collect(Collectors.toList());

        List<List<String>> infos = backupJobs.stream().map(BackupJob::getInfo).collect(Collectors.toList());

        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowRestore() throws AnalysisException {
        ShowRestoreStmt showStmt = (ShowRestoreStmt) stmt;
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(showStmt.getDbName());

        List<AbstractJob> jobs = Catalog.getCurrentCatalog().getBackupHandler().getJobs(db.getId(), showStmt.getLabelPredicate());

        List<RestoreJob> restoreJobs = jobs.stream().filter(job -> job instanceof RestoreJob)
                .map(job -> (RestoreJob) job).collect(Collectors.toList());

        List<List<String>> infos = restoreJobs.stream().map(RestoreJob::getInfo).collect(Collectors.toList());

        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowSyncJobs() throws AnalysisException {
        ShowSyncJobStmt showStmt = (ShowSyncJobStmt) stmt;
        Catalog catalog = Catalog.getCurrentCatalog();
        Database db = catalog.getDbOrAnalysisException(showStmt.getDbName());

        List<List<Comparable>> syncInfos = catalog.getSyncJobManager().getSyncJobsInfoByDbId(db.getId());
        Collections.sort(syncInfos, new ListComparator<List<Comparable>>(0));

        List<List<String>> rows = Lists.newArrayList();
        for (List<Comparable> syncInfo : syncInfos) {
            List<String> row = new ArrayList<String>(syncInfo.size());

            for (Comparable element : syncInfo) {
                row.add(element.toString());
            }
            rows.add(row);
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowGrants() {
        ShowGrantsStmt showStmt = (ShowGrantsStmt) stmt;
        List<List<String>> infos = Catalog.getCurrentCatalog().getAuth().getAuthInfo(showStmt.getUserIdent());
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowRoles() {
        ShowRolesStmt showStmt = (ShowRolesStmt) stmt;
        List<List<String>> infos = Catalog.getCurrentCatalog().getAuth().getRoleInfo();
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }
    
    private void handleShowTrash() {
        ShowTrashStmt showStmt = (ShowTrashStmt) stmt;
        List<List<String>> infos = Lists.newArrayList();
        TrashProcDir.getTrashInfo(showStmt.getBackends(), infos);
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowTrashDisk() {
        ShowTrashDiskStmt showStmt = (ShowTrashDiskStmt) stmt;
        List<List<String>> infos = Lists.newArrayList();
        TrashProcNode.getTrashDiskInfo(showStmt.getBackend(), infos);
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleAdminShowTabletStatus() throws AnalysisException {
        AdminShowReplicaStatusStmt showStmt = (AdminShowReplicaStatusStmt) stmt;
        List<List<String>> results;
        try {
            results = MetadataViewer.getTabletStatus(showStmt);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), results);
    }

    private void handleAdminShowTabletDistribution() throws AnalysisException {
        AdminShowReplicaDistributionStmt showStmt = (AdminShowReplicaDistributionStmt) stmt;
        List<List<String>> results;
        try {
            results = MetadataViewer.getTabletDistribution(showStmt);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), results);
    }

    private void handleAdminShowConfig() throws AnalysisException {
        AdminShowConfigStmt showStmt = (AdminShowConfigStmt) stmt;
        List<List<String>> results;

        PatternMatcher matcher = null;
        if (showStmt.getPattern() != null) {
            matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern(),
                    CaseSensibility.CONFIG.getCaseSensibility());
        }
        results = ConfigBase.getConfigInfo(matcher);
        // Sort all configs by config key.
        results.sort(Comparator.comparing(o -> o.get(0)));
        resultSet = new ShowResultSet(showStmt.getMetaData(), results);
    }

    private void handleShowSmallFiles() throws AnalysisException {
        ShowSmallFilesStmt showStmt = (ShowSmallFilesStmt) stmt;
        List<List<String>> results;
        try {
            results = Catalog.getCurrentCatalog().getSmallFileMgr().getInfo(showStmt.getDbName());
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), results);
    }

    private void handleShowDynamicPartition() {
        ShowDynamicPartitionStmt showDynamicPartitionStmt = (ShowDynamicPartitionStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        Database db = ctx.getCatalog().getDbNullable(showDynamicPartitionStmt.getDb());
        if (db != null) {
            List<Table> tableList = db.getTables();
            for (Table tbl : tableList) {
                if (!(tbl instanceof OlapTable)) {
                    continue;
                }

                DynamicPartitionScheduler dynamicPartitionScheduler = Catalog.getCurrentCatalog().getDynamicPartitionScheduler();
                OlapTable olapTable = (OlapTable) tbl;
                olapTable.readLock();
                try {
                    if (!olapTable.dynamicPartitionExists()) {
                        dynamicPartitionScheduler.removeRuntimeInfo(olapTable.getId());
                        continue;
                    }

                    // check tbl privs
                    if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(),
                            db.getFullName(), olapTable.getName(),
                            PrivPredicate.SHOW)) {
                        continue;
                    }
                    DynamicPartitionProperty dynamicPartitionProperty = olapTable.getTableProperty().getDynamicPartitionProperty();
                    String tableName = olapTable.getName();
                    ReplicaAllocation replicaAlloc = dynamicPartitionProperty.getReplicaAllocation();
                    if (replicaAlloc.isNotSet()) {
                        replicaAlloc = olapTable.getDefaultReplicaAllocation();
                    }
                    String unsortedReservedHistoryPeriods = dynamicPartitionProperty.getReservedHistoryPeriods();
                    rows.add(Lists.newArrayList(
                            tableName,
                            String.valueOf(dynamicPartitionProperty.getEnable()),
                            dynamicPartitionProperty.getTimeUnit().toUpperCase(),
                            String.valueOf(dynamicPartitionProperty.getStart()),
                            String.valueOf(dynamicPartitionProperty.getEnd()),
                            dynamicPartitionProperty.getPrefix(),
                            String.valueOf(dynamicPartitionProperty.getBuckets()),
                            String.valueOf(replicaAlloc.getTotalReplicaNum()),
                            replicaAlloc.toCreateStmt(),
                            dynamicPartitionProperty.getStartOfInfo(),
                            dynamicPartitionScheduler.getRuntimeInfo(olapTable.getId(), DynamicPartitionScheduler.LAST_UPDATE_TIME),
                            dynamicPartitionScheduler.getRuntimeInfo(olapTable.getId(), DynamicPartitionScheduler.LAST_SCHEDULER_TIME),
                            dynamicPartitionScheduler.getRuntimeInfo(olapTable.getId(), DynamicPartitionScheduler.DYNAMIC_PARTITION_STATE),
                            dynamicPartitionScheduler.getRuntimeInfo(olapTable.getId(), DynamicPartitionScheduler.CREATE_PARTITION_MSG),
                            dynamicPartitionScheduler.getRuntimeInfo(olapTable.getId(), DynamicPartitionScheduler.DROP_PARTITION_MSG),
                            dynamicPartitionProperty.getSortedReservedHistoryPeriods(unsortedReservedHistoryPeriods, dynamicPartitionProperty.getTimeUnit().toUpperCase())));
                } catch (DdlException e) {
                    e.printStackTrace();
                } finally {
                    olapTable.readUnlock();
                }
            }

            resultSet = new ShowResultSet(showDynamicPartitionStmt.getMetaData(), rows);
        }
    }

    // Show transaction statement.
    private void handleShowTransaction() throws AnalysisException {
        ShowTransactionStmt showStmt = (ShowTransactionStmt) stmt;
        Database db = ctx.getCatalog().getDbOrAnalysisException(showStmt.getDbName());

        TransactionStatus status = showStmt.getStatus();
        GlobalTransactionMgr transactionMgr = Catalog.getCurrentGlobalTransactionMgr();
        if (status != TransactionStatus.UNKNOWN) {
            resultSet = new ShowResultSet(showStmt.getMetaData(), transactionMgr.getDbTransInfoByStatus(db.getId(), status));
        } else {
            Long txnId = showStmt.getTxnId();
            String label = showStmt.getLabel();
            if (!label.isEmpty()) {
                txnId = transactionMgr.getTransactionId(db.getId(), label);
                if (txnId == null) {
                    throw new AnalysisException("transaction with label " + label + " does not exist");
                }
            }
            resultSet = new ShowResultSet(showStmt.getMetaData(), transactionMgr.getSingleTranInfo(db.getId(), txnId));
        }
    }

    private void handleShowPlugins() throws AnalysisException {
        ShowPluginsStmt pluginsStmt = (ShowPluginsStmt) stmt;
        List<List<String>> rows = Catalog.getCurrentPluginMgr().getPluginShowInfos();
        resultSet = new ShowResultSet(pluginsStmt.getMetaData(), rows);
    }

    private void handleShowQueryProfile() throws AnalysisException {
        ShowQueryProfileStmt showStmt = (ShowQueryProfileStmt) stmt;
        ShowQueryProfileStmt.PathType pathType = showStmt.getPathType();
        List<List<String>> rows = Lists.newArrayList();
        switch (pathType) {
            case QUERY_IDS:
                rows = ProfileManager.getInstance().getQueryWithType(ProfileManager.ProfileType.QUERY);
                break;
            case FRAGMETNS: {
                ProfileTreeNode treeRoot = ProfileManager.getInstance().getFragmentProfileTree(showStmt.getQueryId(),
                        showStmt.getQueryId());
                if (treeRoot == null) {
                    throw new AnalysisException("Failed to get fragment tree for query: " + showStmt.getQueryId());
                }
                List<String> row = Lists.newArrayList(ProfileTreePrinter.printFragmentTree(treeRoot));
                rows.add(row);
                break;
            }
            case INSTANCES: {
                // For query profile, there should be only one execution profile,
                // And the execution id is same as query id
                List<Triple<String, String, Long>> instanceList
                        = ProfileManager.getInstance().getFragmentInstanceList(
                        showStmt.getQueryId(), showStmt.getQueryId(), showStmt.getFragmentId());
                if (instanceList == null) {
                    throw new AnalysisException("Failed to get instance list for fragment: " + showStmt.getFragmentId());
                }
                for (Triple<String, String, Long> triple : instanceList) {
                    List<String> row = Lists.newArrayList(triple.getLeft(), triple.getMiddle(),
                            RuntimeProfile.printCounter(triple.getRight(), TUnit.TIME_NS));
                    rows.add(row);
                }
                break;
            }
            case SINGLE_INSTANCE: {
                // For query profile, there should be only one execution profile,
                // And the execution id is same as query id
                ProfileTreeNode treeRoot = ProfileManager.getInstance().getInstanceProfileTree(showStmt.getQueryId(),
                        showStmt.getQueryId(), showStmt.getFragmentId(), showStmt.getInstanceId());
                if (treeRoot == null) {
                    throw new AnalysisException("Failed to get instance tree for instance: " + showStmt.getInstanceId());
                }
                List<String> row = Lists.newArrayList(ProfileTreePrinter.printInstanceTree(treeRoot));
                rows.add(row);
                break;
            }
            default:
                break;
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowLoadProfile() throws AnalysisException {
        ShowLoadProfileStmt showStmt = (ShowLoadProfileStmt) stmt;
        ShowLoadProfileStmt.PathType pathType = showStmt.getPathType();
        List<List<String>> rows = Lists.newArrayList();
        switch (pathType) {
            case JOB_IDS:
                rows = ProfileManager.getInstance().getQueryWithType(ProfileManager.ProfileType.LOAD);
                break;
            case TASK_IDS: {
                rows = ProfileManager.getInstance().getLoadJobTaskList(showStmt.getJobId());
                break;
            }
            case INSTANCES: {
                // For load profile, there should be only one fragment in each execution profile
                // And the fragment id is 0.
                List<Triple<String, String, Long>> instanceList
                        = ProfileManager.getInstance().getFragmentInstanceList(showStmt.getJobId(),
                        showStmt.getTaskId(), "0");
                if (instanceList == null) {
                    throw new AnalysisException("Failed to get instance list for task: " + showStmt.getTaskId());
                }
                for (Triple<String, String, Long> triple : instanceList) {
                    List<String> row = Lists.newArrayList(triple.getLeft(), triple.getMiddle(),
                            RuntimeProfile.printCounter(triple.getRight(), TUnit.TIME_NS));
                    rows.add(row);
                }
                break;
            }
            case SINGLE_INSTANCE: {
                // For load profile, there should be only one fragment in each execution profile.
                // And the fragment id is 0.
                ProfileTreeNode treeRoot = ProfileManager.getInstance().getInstanceProfileTree(showStmt.getJobId(),
                        showStmt.getTaskId(), "0", showStmt.getInstanceId());
                if (treeRoot == null) {
                    throw new AnalysisException("Failed to get instance tree for instance: " + showStmt.getInstanceId());
                }
                List<String> row = Lists.newArrayList(ProfileTreePrinter.printInstanceTree(treeRoot));
                rows.add(row);
                break;
            }
            default:
                break;
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowCreateRoutineLoad() throws AnalysisException {
        ShowCreateRoutineLoadStmt showCreateRoutineLoadStmt = (ShowCreateRoutineLoadStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        String dbName = showCreateRoutineLoadStmt.getDb();
        String labelName = showCreateRoutineLoadStmt.getLabel();
        // if include history return all create load
        if (showCreateRoutineLoadStmt.isIncludeHistory()) {
            List<RoutineLoadJob> routineLoadJobList = new ArrayList<>();
            try {
                routineLoadJobList = Catalog.getCurrentCatalog().getRoutineLoadManager().getJob(dbName, labelName, true, null);
            } catch (MetaNotFoundException e) {
                LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, labelName)
                        .add("error_msg", "Routine load cannot be found by this name")
                        .build(), e);
            }
            if (routineLoadJobList == null) {
                resultSet = new ShowResultSet(showCreateRoutineLoadStmt.getMetaData(), rows);
                return;
            }
            for (RoutineLoadJob job : routineLoadJobList) {
                String tableName = "";
                try {
                    tableName = job.getTableName();
                } catch (MetaNotFoundException e) {
                    LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, job.getId())
                            .add("error_msg", "The table name for this routine load does not exist")
                            .build(), e);
                }
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(),
                        dbName,
                        tableName,
                        PrivPredicate.LOAD)) {
                    resultSet = new ShowResultSet(showCreateRoutineLoadStmt.getMetaData(), rows);
                    continue;
                }
                rows.add(Lists.newArrayList(String.valueOf(job.getId()), showCreateRoutineLoadStmt.getLabel(), job.getShowCreateInfo()));
            }
        } else {
            // if job exists
            RoutineLoadJob routineLoadJob;
            try {
                routineLoadJob = Catalog.getCurrentCatalog().getRoutineLoadManager().checkPrivAndGetJob(dbName, labelName);
                // get routine load info
                rows.add(Lists.newArrayList(String.valueOf(routineLoadJob.getId()), showCreateRoutineLoadStmt.getLabel(), routineLoadJob.getShowCreateInfo()));
            } catch (MetaNotFoundException | DdlException e) {
                LOG.warn(e.getMessage(), e);
                throw new AnalysisException(e.getMessage());
            }
        }
        resultSet = new ShowResultSet(showCreateRoutineLoadStmt.getMetaData(), rows);
    }

    private void handleShowDataSkew() throws AnalysisException {
        ShowDataSkewStmt showStmt = (ShowDataSkewStmt) stmt;
        try {
            List<List<String>> results = MetadataViewer.getDataSkew(showStmt);
            resultSet = new ShowResultSet(showStmt.getMetaData(), results);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }

    }

    private void handleShowTableStats() throws AnalysisException {
        ShowTableStatsStmt showTableStatsStmt = (ShowTableStatsStmt) stmt;
        List<List<String>> results = Catalog.getCurrentCatalog().getStatisticsManager()
                .showTableStatsList(showTableStatsStmt.getDbName(), showTableStatsStmt.getTableName());
        resultSet = new ShowResultSet(showTableStatsStmt.getMetaData(), results);
    }

    private void handleShowColumnStats() throws AnalysisException {
        ShowColumnStatsStmt showColumnStatsStmt = (ShowColumnStatsStmt) stmt;
        List<List<String>> results = Catalog.getCurrentCatalog().getStatisticsManager()
                .showColumnStatsList(showColumnStatsStmt.getTableName());
        resultSet = new ShowResultSet(showColumnStatsStmt.getMetaData(), results);
    }

    public void handleShowSqlBlockRule() throws AnalysisException {
        ShowSqlBlockRuleStmt showStmt = (ShowSqlBlockRuleStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        List<SqlBlockRule> sqlBlockRules = Catalog.getCurrentCatalog().getSqlBlockRuleMgr().getSqlBlockRule(showStmt);
        sqlBlockRules.forEach(rule -> rows.add(rule.getShowInfo()));
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowTableCreation() throws AnalysisException {
        ShowTableCreationStmt showStmt = (ShowTableCreationStmt) stmt;
        String dbName = showStmt.getDbName();
        Database db = ctx.getCatalog().getDbOrAnalysisException(dbName);

        List<IcebergTableCreationRecord> records =
                ctx.getCatalog().getIcebergTableCreationRecordMgr().getTableCreationRecordByDbId(db.getId());

        List<List<Comparable>> rowSet = Lists.newArrayList();
        for (IcebergTableCreationRecord record : records) {
            List<Comparable> row = record.getTableCreationRecord();
            // like predicate
            if (Strings.isNullOrEmpty(showStmt.getWild()) || showStmt.like(record.getTable())) {
                rowSet.add(row);
            }
        }

        // sort function rows by fourth column (Create Time) asc
        ListComparator<List<Comparable>> comparator = null;
        OrderByPair orderByPair = new OrderByPair(3, false);
        comparator = new ListComparator<>(orderByPair);
        Collections.sort(rowSet, comparator);
        List<List<String>> resultRowSet = Lists.newArrayList();

        Set<String> keyNameSet = new HashSet<>();
        for (List<Comparable> row : rowSet) {
            List<String> resultRow = Lists.newArrayList();
            for (Comparable column : row) {
                resultRow.add(column.toString());
            }
            resultRowSet.add(resultRow);
            keyNameSet.add(resultRow.get(0));
        }

        ShowResultSetMetaData showMetaData = showStmt.getMetaData();
        resultSet = new ShowResultSet(showMetaData, resultRowSet);
    }

    private void handleShowLastInsert() {
        ShowLastInsertStmt showStmt = (ShowLastInsertStmt) stmt;
        List<List<String>> resultRowSet = Lists.newArrayList();
        if (ConnectContext.get() != null) {
            InsertResult insertResult = ConnectContext.get().getInsertResult();
            if (insertResult != null) {
                resultRowSet.add(insertResult.toRow());
            }
        }
        ShowResultSetMetaData showMetaData = showStmt.getMetaData();
        resultSet = new ShowResultSet(showMetaData, resultRowSet);
    }

    private void handleAdminShowTabletStorageFormat() throws AnalysisException {
        List<List<String>> resultRowSet = Lists.newArrayList();
        for (Backend be : Catalog.getCurrentSystemInfo().getIdToBackend().values()) {
            if (be.isQueryAvailable() && be.isLoadAvailable()) {
                AgentClient client = new AgentClient(be.getHost(), be.getBePort());
                TCheckStorageFormatResult result = client.checkStorageFormat();
                if (result == null) {
                    throw new AnalysisException("get tablet data from backend: " + be.getId() + "error.");
                }
                if (stmt.isVerbose()) {
                    for (long tabletId : result.getV1Tablets()) {
                        List<String> row = new ArrayList<>();
                        row.add(String.valueOf(be.getId()));
                        row.add(String.valueOf(tabletId));
                        row.add("V1");
                        resultRowSet.add(row);
                    }
                    for (long tabletId : result.getV2Tablets()) {
                        List<String> row = new ArrayList<>();
                        row.add(String.valueOf(be.getId()));
                        row.add(String.valueOf(tabletId));
                        row.add("V2");
                        resultRowSet.add(row);
                    }
                } else {
                    List<String> row = new ArrayList<>();
                    row.add(String.valueOf(be.getId()));
                    row.add(String.valueOf(result.getV1Tablets().size()));
                    row.add(String.valueOf(result.getV2Tablets().size()));
                    resultRowSet.add(row);
                }
            }
        }
        ShowResultSetMetaData showMetaData = stmt.getMetaData();
        resultSet = new ShowResultSet(showMetaData, resultRowSet);
    }

    private void handleAdminDiagnoseTablet() {
        AdminDiagnoseTabletStmt showStmt = (AdminDiagnoseTabletStmt) stmt;
        List<List<String>> resultRowSet = Diagnoser.diagnoseTablet(showStmt.getTabletId());
        ShowResultSetMetaData showMetaData = showStmt.getMetaData();
        resultSet = new ShowResultSet(showMetaData, resultRowSet);
    }

}
