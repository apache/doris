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

import org.apache.doris.analysis.AdminCopyTabletStmt;
import org.apache.doris.analysis.AdminDiagnoseTabletStmt;
import org.apache.doris.analysis.AdminShowConfigStmt;
import org.apache.doris.analysis.AdminShowReplicaDistributionStmt;
import org.apache.doris.analysis.AdminShowReplicaStatusStmt;
import org.apache.doris.analysis.AdminShowTabletStorageFormatStmt;
import org.apache.doris.analysis.DescribeStmt;
import org.apache.doris.analysis.HelpStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.ShowAlterStmt;
import org.apache.doris.analysis.ShowAnalyzeStmt;
import org.apache.doris.analysis.ShowAnalyzeTaskStatus;
import org.apache.doris.analysis.ShowAuthorStmt;
import org.apache.doris.analysis.ShowBackendsStmt;
import org.apache.doris.analysis.ShowBackupStmt;
import org.apache.doris.analysis.ShowBrokerStmt;
import org.apache.doris.analysis.ShowBuildIndexStmt;
import org.apache.doris.analysis.ShowCatalogRecycleBinStmt;
import org.apache.doris.analysis.ShowCatalogStmt;
import org.apache.doris.analysis.ShowCollationStmt;
import org.apache.doris.analysis.ShowColumnHistStmt;
import org.apache.doris.analysis.ShowColumnStatsStmt;
import org.apache.doris.analysis.ShowColumnStmt;
import org.apache.doris.analysis.ShowConvertLSCStmt;
import org.apache.doris.analysis.ShowCreateCatalogStmt;
import org.apache.doris.analysis.ShowCreateDbStmt;
import org.apache.doris.analysis.ShowCreateFunctionStmt;
import org.apache.doris.analysis.ShowCreateLoadStmt;
import org.apache.doris.analysis.ShowCreateMaterializedViewStmt;
import org.apache.doris.analysis.ShowCreateRepositoryStmt;
import org.apache.doris.analysis.ShowCreateRoutineLoadStmt;
import org.apache.doris.analysis.ShowCreateTableStmt;
import org.apache.doris.analysis.ShowDataSkewStmt;
import org.apache.doris.analysis.ShowDataStmt;
import org.apache.doris.analysis.ShowDataTypesStmt;
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
import org.apache.doris.analysis.ShowMTMVJobStmt;
import org.apache.doris.analysis.ShowMTMVTaskStmt;
import org.apache.doris.analysis.ShowPartitionIdStmt;
import org.apache.doris.analysis.ShowPartitionsStmt;
import org.apache.doris.analysis.ShowPluginsStmt;
import org.apache.doris.analysis.ShowPolicyStmt;
import org.apache.doris.analysis.ShowProcStmt;
import org.apache.doris.analysis.ShowProcesslistStmt;
import org.apache.doris.analysis.ShowQueryProfileStmt;
import org.apache.doris.analysis.ShowQueryStatsStmt;
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
import org.apache.doris.analysis.ShowTypeCastStmt;
import org.apache.doris.analysis.ShowUserPropertyStmt;
import org.apache.doris.analysis.ShowVariablesStmt;
import org.apache.doris.analysis.ShowViewStmt;
import org.apache.doris.analysis.ShowWorkloadGroupsStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.backup.AbstractJob;
import org.apache.doris.backup.BackupJob;
import org.apache.doris.backup.Repository;
import org.apache.doris.backup.RestoreJob;
import org.apache.doris.blockrule.SqlBlockRule;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ColumnIdFlushDaemon;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.EncryptKey;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionUtil;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.MetadataViewer;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.catalog.View;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.clone.DynamicPartitionScheduler;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.proc.BackendsProcDir;
import org.apache.doris.common.proc.BuildIndexProcDir;
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
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.external.iceberg.IcebergTableCreationRecord;
import org.apache.doris.load.DeleteHandler;
import org.apache.doris.load.ExportJob;
import org.apache.doris.load.ExportMgr;
import org.apache.doris.load.Load;
import org.apache.doris.load.LoadJob;
import org.apache.doris.load.LoadJob.JobState;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.mtmv.MTMVJobManager;
import org.apache.doris.mtmv.metadata.MTMVJob;
import org.apache.doris.mtmv.metadata.MTMVTask;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Histogram;
import org.apache.doris.statistics.StatisticsRepository;
import org.apache.doris.statistics.TableStatsMeta;
import org.apache.doris.statistics.query.QueryStatsUtil;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Diagnoser;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentClient;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.thrift.TCheckStorageFormatResult;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.thrift.TUnit;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
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
        } else if (stmt instanceof ShowDataTypesStmt) {
            handleShowDataTypes();
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
        } else if (stmt instanceof ShowCreateLoadStmt) {
            handleShowCreateLoad();
        } else if (stmt instanceof ShowCreateRepositoryStmt) {
            handleShowCreateRepository();
        } else if (stmt instanceof ShowDeleteStmt) {
            handleShowDelete();
        } else if (stmt instanceof ShowAlterStmt) {
            handleShowAlter();
        } else if (stmt instanceof ShowUserPropertyStmt) {
            handleShowUserProperty();
        } else if (stmt instanceof ShowDataStmt) {
            handleShowData();
        } else if (stmt instanceof ShowQueryStatsStmt) {
            handleShowQueryStats();
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
        } else if (stmt instanceof ShowBrokerStmt) {
            handleShowBroker();
        } else if (stmt instanceof ShowResourcesStmt) {
            handleShowResources();
        } else if (stmt instanceof ShowWorkloadGroupsStmt) {
            handleShowWorkloadGroups();
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
        } else if (stmt instanceof ShowColumnHistStmt) {
            handleShowColumnHist();
        } else if (stmt instanceof ShowTableCreationStmt) {
            handleShowTableCreation();
        } else if (stmt instanceof ShowLastInsertStmt) {
            handleShowLastInsert();
        } else if (stmt instanceof AdminShowTabletStorageFormatStmt) {
            handleAdminShowTabletStorageFormat();
        } else if (stmt instanceof AdminDiagnoseTabletStmt) {
            handleAdminDiagnoseTablet();
        } else if (stmt instanceof ShowCreateMaterializedViewStmt) {
            handleShowCreateMaterializedView();
        } else if (stmt instanceof ShowPolicyStmt) {
            handleShowPolicy();
        } else if (stmt instanceof ShowCatalogStmt) {
            handleShowCatalogs();
        } else if (stmt instanceof ShowCreateCatalogStmt) {
            handleShowCreateCatalog();
        } else if (stmt instanceof ShowAnalyzeStmt) {
            handleShowAnalyze();
        } else if (stmt instanceof AdminCopyTabletStmt) {
            handleCopyTablet();
        } else if (stmt instanceof ShowCatalogRecycleBinStmt) {
            handleShowCatalogRecycleBin();
        } else if (stmt instanceof ShowMTMVJobStmt) {
            handleMTMVJobs();
        } else if (stmt instanceof ShowMTMVTaskStmt) {
            handleMTMVTasks();
        } else if (stmt instanceof ShowTypeCastStmt) {
            handleShowTypeCastStmt();
        } else if (stmt instanceof ShowBuildIndexStmt) {
            handleShowBuildIndexStmt();
        } else if (stmt instanceof ShowAnalyzeTaskStatus) {
            handleShowAnalyzeTaskStatus();
        } else if (stmt instanceof ShowConvertLSCStmt) {
            handleShowConvertLSC();
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

    // Handle show processlist
    private void handleShowProcesslist() {
        ShowProcesslistStmt showStmt = (ShowProcesslistStmt) stmt;
        List<List<String>> rowSet = Lists.newArrayList();

        List<ConnectContext.ThreadInfo> threadInfos = ctx.getConnectScheduler()
                .listConnection(ctx.getQualifiedUser(), showStmt.isFull());
        long nowMs = System.currentTimeMillis();
        for (ConnectContext.ThreadInfo info : threadInfos) {
            rowSet.add(info.toRow(ctx.getConnectionId(), nowMs));
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
        rowSet.add(Lists.newArrayList("ELASTICSEARCH", "YES", "ELASTICSEARCH cluster which data is in it",
                "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("HIVE", "YES", "HIVE database which data is in it", "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("ICEBERG", "YES", "ICEBERG data lake which data is in it", "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("ODBC", "YES", "ODBC driver which data we can connect", "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("HUDI", "YES", "HUDI data lake which data is in it", "NO", "NO", "NO"));

        // Only success
        resultSet = new ShowResultSet(showStmt.getMetaData(), rowSet);
    }

    // Handle show functions
    private void handleShowFunctions() throws AnalysisException {
        ShowFunctionsStmt showStmt = (ShowFunctionsStmt) stmt;

        List<List<String>> resultRowSet = getResultRowSet(showStmt);

        // Only success
        ShowResultSetMetaData showMetaData = showStmt.getIsVerbose() ? showStmt.getMetaData() :
                ShowResultSetMetaData.builder()
                        .addColumn(new Column("Function Name", ScalarType.createVarchar(256))).build();
        resultSet = new ShowResultSet(showMetaData, resultRowSet);
    }

    private void handleShowDataTypes() throws AnalysisException {
        ShowDataTypesStmt showStmt = (ShowDataTypesStmt) stmt;
        List<List<String>> rows = showStmt.getTypesAvailableInDdl();
        showStmt.sortMetaData(rows);
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    /***
     * get resultRowSet by showFunctionsStmt
     * @param showStmt
     * @return
     * @throws AnalysisException
     */
    private List<List<String>> getResultRowSet(ShowFunctionsStmt showStmt) throws AnalysisException {
        List<Function> functions = getFunctions(showStmt);
        return getResultRowSetByFunctions(showStmt, functions);
    }

    /***
     * get functions by showFunctionsStmt
     * @param showStmt
     * @return
     * @throws AnalysisException
     */
    private List<Function> getFunctions(ShowFunctionsStmt showStmt) throws AnalysisException {
        List<Function> functions = Lists.newArrayList();
        if (!FunctionUtil.isGlobalFunction(showStmt.getType())) {
            Util.prohibitExternalCatalog(ctx.getDefaultCatalog(), stmt.getClass().getSimpleName());
            DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(showStmt.getDbName());
            if (db instanceof Database) {
                functions = showStmt.getIsBuiltin() ? ctx.getEnv().getBuiltinFunctions()
                        : ((Database) db).getFunctions();
            }
        } else {
            functions = Env.getCurrentEnv().getGlobalFunctionMgr().getFunctions();
        }
        return functions;
    }

    /***
     * get resultRowSet By showFunctionsStmt and functions
     * @param showStmt
     * @param functions
     * @return
     */
    private List<List<String>> getResultRowSetByFunctions(ShowFunctionsStmt showStmt, List<Function> functions) {
        List<List<String>> resultRowSet = Lists.newArrayList();
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
        return resultRowSet;
    }

    // Handle show create function
    private void handleShowCreateFunction() throws AnalysisException {
        ShowCreateFunctionStmt showCreateFunctionStmt = (ShowCreateFunctionStmt) stmt;
        List<List<String>> resultRowSet = getResultRowSet(showCreateFunctionStmt);
        resultSet = new ShowResultSet(showCreateFunctionStmt.getMetaData(), resultRowSet);
    }

    /***
     * get resultRowSet by showCreateFunctionStmt
     * @param showCreateFunctionStmt
     * @return
     * @throws AnalysisException
     */
    private List<List<String>> getResultRowSet(ShowCreateFunctionStmt showCreateFunctionStmt) throws AnalysisException {
        Function function = getFunction(showCreateFunctionStmt);
        return getResultRowSetByFunction(function);
    }

    private Function getFunction(ShowCreateFunctionStmt showCreateFunctionStmt) throws AnalysisException {
        if (!FunctionUtil.isGlobalFunction(showCreateFunctionStmt.getType())) {
            Util.prohibitExternalCatalog(ctx.getDefaultCatalog(), stmt.getClass().getSimpleName());
            DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(showCreateFunctionStmt.getDbName());
            if (db instanceof Database) {
                return ((Database) db).getFunction(showCreateFunctionStmt.getFunction());
            }
        } else {
            return Env.getCurrentEnv().getGlobalFunctionMgr().getFunction(showCreateFunctionStmt.getFunction());
        }
        return null;
    }

    /***
     * get resultRowSet by function
     * @param function
     * @return
     */
    private List<List<String>> getResultRowSetByFunction(Function function) {
        if (Objects.isNull(function)) {
            return Lists.newArrayList();
        }
        List<List<String>> resultRowSet = Lists.newArrayList();
        List<String> resultRow = Lists.newArrayList();
        resultRow.add(function.signatureString());
        resultRow.add(function.toSql(false));
        resultRowSet.add(resultRow);
        return resultRowSet;
    }

    // Handle show encryptkeys
    private void handleShowEncryptKeys() throws AnalysisException {
        ShowEncryptKeysStmt showStmt = (ShowEncryptKeysStmt) stmt;
        Util.prohibitExternalCatalog(ctx.getDefaultCatalog(), stmt.getClass().getSimpleName());
        DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(showStmt.getDbName());
        List<List<String>> resultRowSet = Lists.newArrayList();
        if (db instanceof Database) {
            List<EncryptKey> encryptKeys = ((Database) db).getEncryptKeys();
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

            Set<String> encryptKeyNameSet = new HashSet<>();
            for (List<Comparable> row : rowSet) {
                List<String> resultRow = Lists.newArrayList();
                for (Comparable column : row) {
                    resultRow.add(column.toString());
                }
                resultRowSet.add(resultRow);
                encryptKeyNameSet.add(resultRow.get(0));
            }
        }

        ShowResultSetMetaData showMetaData = showStmt.getMetaData();
        resultSet = new ShowResultSet(showMetaData, resultRowSet);
    }

    private void handleShowProc() throws AnalysisException {
        ShowProcStmt showProcStmt = (ShowProcStmt) stmt;
        ShowResultSetMetaData metaData = showProcStmt.getMetaData();
        ProcNodeInterface procNode = showProcStmt.getNode();

        List<List<String>> finalRows = procNode.fetchResult().getRows();
        resultSet = new ShowResultSet(metaData, finalRows);
    }

    private void handleShowDbId() {
        ShowDbIdStmt showStmt = (ShowDbIdStmt) stmt;
        long dbId = showStmt.getDbId();
        List<List<String>> rows = Lists.newArrayList();
        DatabaseIf database = ctx.getCurrentCatalog().getDbNullable(dbId);
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
        Env env = ctx.getEnv();
        List<Long> dbIds = env.getInternalCatalog().getDbIds();
        // TODO should use inverted index
        for (long dbId : dbIds) {
            Database database = env.getInternalCatalog().getDbNullable(dbId);
            if (database == null) {
                continue;
            }
            TableIf table = database.getTableNullable(tableId);
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
        Env env = ctx.getEnv();
        List<Long> dbIds = env.getInternalCatalog().getDbIds();
        // TODO should use inverted index
        for (long dbId : dbIds) {
            Database database = env.getInternalCatalog().getDbNullable(dbId);
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
        // cluster feature is deprecated.
        CatalogIf catalogIf = ctx.getCatalog(showDbStmt.getCatalogName());
        if (catalogIf == null) {
            throw new AnalysisException("No catalog found with name " + showDbStmt.getCatalogName());
        }
        List<String> dbNames = catalogIf.getDbNames();
        PatternMatcher matcher = null;
        if (showDbStmt.getPattern() != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(showDbStmt.getPattern(),
                    CaseSensibility.DATABASE.getCaseSensibility());
        }
        Set<String> dbNameSet = Sets.newTreeSet();
        for (String fullName : dbNames) {
            final String db = ClusterNamespace.getNameFromFullName(fullName);
            // Filter dbname
            if (matcher != null && !matcher.match(db)) {
                continue;
            }

            if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(), showDbStmt.getCatalogName(),
                    fullName, PrivPredicate.SHOW)) {
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
        DatabaseIf<TableIf> db = ctx.getEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(showTableStmt.getCatalog())
                .getDbOrAnalysisException(showTableStmt.getDb());
        PatternMatcher matcher = null;
        if (showTableStmt.getPattern() != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(showTableStmt.getPattern(),
                    CaseSensibility.TABLE.getCaseSensibility());
        }
        for (TableIf tbl : db.getTables()) {
            if (tbl.getName().startsWith(FeConstants.TEMP_MATERIZLIZE_DVIEW_PREFIX)) {
                continue;
            }
            if (matcher != null && !matcher.match(tbl.getName())) {
                continue;
            }
            // check tbl privs
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(ConnectContext.get(), showTableStmt.getCatalog(), db.getFullName(), tbl.getName(),
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
        // sort by table name
        rows.sort((x, y) -> {
            return x.get(0).compareTo(y.get(0));
        });

        resultSet = new ShowResultSet(showTableStmt.getMetaData(), rows);
    }

    // Show table status statement.
    private void handleShowTableStatus() throws AnalysisException {
        ShowTableStatusStmt showStmt = (ShowTableStatusStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        DatabaseIf<TableIf> db = ctx.getEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(showStmt.getCatalog())
                .getDbOrAnalysisException(showStmt.getDb());
        if (db != null) {
            PatternMatcher matcher = null;
            if (showStmt.getPattern() != null) {
                matcher = PatternMatcherWrapper.createMysqlPattern(showStmt.getPattern(),
                        CaseSensibility.TABLE.getCaseSensibility());
            }
            for (TableIf table : db.getTables()) {
                if (matcher != null && !matcher.match(table.getName())) {
                    continue;
                }

                // check tbl privs
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), showStmt.getCatalog(),
                                db.getFullName(), table.getName(), PrivPredicate.SHOW)) {
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
                // Use estimatedRowCount(), not getRowCount().
                // because estimatedRowCount() is an async call, it will not block, and it will call getRowCount()
                // finally. So that for some table(especially external table),
                // we can get the row count without blocking.
                row.add(String.valueOf(table.estimatedRowCount()));
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
        // sort by table name
        rows.sort((x, y) -> {
            return x.get(0).compareTo(y.get(0));
        });
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show variables like
    private void handleShowVariables() throws AnalysisException {
        ShowVariablesStmt showStmt = (ShowVariablesStmt) stmt;
        PatternMatcher matcher = null;
        if (showStmt.getPattern() != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(showStmt.getPattern(),
                    CaseSensibility.VARIABLES.getCaseSensibility());
        }
        List<List<String>> rows = VariableMgr.dump(showStmt.getType(), ctx.getSessionVariable(), matcher);
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show create database
    private void handleShowCreateDb() throws AnalysisException {
        ShowCreateDbStmt showStmt = (ShowCreateDbStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(showStmt.getDb());
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
        DatabaseIf db = ctx.getEnv().getCatalogMgr().getCatalogOrAnalysisException(showStmt.getCtl())
                .getDbOrAnalysisException(showStmt.getDb());
        TableIf table = db.getTableOrAnalysisException(showStmt.getTable());
        List<List<String>> rows = Lists.newArrayList();

        table.readLock();
        try {
            if (table.getType() == TableType.HMS_EXTERNAL_TABLE) {
                rows.add(Arrays.asList(table.getName(),
                        HiveMetaStoreClientHelper.showCreateTable(((HMSExternalTable) table).getRemoteTable())));
                resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
                return;
            }
            List<String> createTableStmt = Lists.newArrayList();
            Env.getDdlStmt(null, null, table, createTableStmt, null, null, false,
                    true /* hide password */, false, -1L, showStmt.isNeedBriefDdl(), false);
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
                resultSet = table.getType() != TableType.MATERIALIZED_VIEW
                        ? new ShowResultSet(showStmt.getMetaData(), rows)
                        : new ShowResultSet(ShowCreateTableStmt.getMaterializedViewMetaData(), rows);
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
        DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(showStmt.getDb());
        TableIf table = db.getTableOrAnalysisException(showStmt.getTable());
        PatternMatcher matcher = null;
        if (showStmt.getPattern() != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(showStmt.getPattern(),
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
        DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(showStmt.getDbName());
        OlapTable table = db.getOlapTableOrAnalysisException(showStmt.getTableName().getTbl());
        table.readLock();
        try {
            List<Index> indexes = table.getIndexes();
            for (Index index : indexes) {
                rows.add(Lists.newArrayList(showStmt.getTableName().toString(), "", index.getIndexName(),
                        "", String.join(",", index.getColumns()), "", "", "", "",
                        "", index.getIndexType().name(), index.getComment(), index.getPropertiesString()));
            }
        } finally {
            table.readUnlock();
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show view statement.
    private void handleShowView() {
        ShowViewStmt showStmt = (ShowViewStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        List<View> matchViews = showStmt.getMatchViews();
        for (View view : matchViews) {
            view.readLock();
            try {
                List<String> createViewStmt = Lists.newArrayList();
                Env.getDdlStmt(view, createViewStmt, null, null, false, true /* hide password */, -1L);
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

        Util.prohibitExternalCatalog(ctx.getDefaultCatalog(), stmt.getClass().getSimpleName());
        Env env = ctx.getEnv();
        DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(showStmt.getDbName());
        long dbId = db.getId();

        // combine the List<LoadInfo> of load(v1) and loadManager(v2)
        Load load = env.getLoadInstance();
        List<List<Comparable>> loadInfos = load.getLoadJobInfosByDb(dbId, db.getFullName(), showStmt.getLabelValue(),
                showStmt.isAccurateMatch(), showStmt.getStates());
        Set<String> statesValue = showStmt.getStates() == null ? null : showStmt.getStates().stream()
                .map(entity -> entity.name())
                .collect(Collectors.toSet());
        loadInfos.addAll(env.getLoadManager()
                .getLoadJobInfosByDb(dbId, showStmt.getLabelValue(), showStmt.isAccurateMatch(), statesValue));

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

        Env env = Env.getCurrentEnv();
        Database db = env.getInternalCatalog().getDbOrAnalysisException(showStmt.getDbName());
        long dbId = db.getId();

        List<List<Comparable>> streamLoadRecords = env.getStreamLoadRecordMgr()
                .getStreamLoadRecordByDb(dbId, showStmt.getLabelValue(), showStmt.isAccurateMatch(),
                        showStmt.getState());

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

        Env env = Env.getCurrentEnv();
        // try to fetch load id from mysql load first and mysql load only support find by label.
        if (showWarningsStmt.isFindByLabel()) {
            String label = showWarningsStmt.getLabel();
            String urlString = env.getLoadManager().getMysqlLoadManager().getErrorUrlByLoadId(label);
            if (urlString != null && !urlString.isEmpty()) {
                URL url;
                try {
                    url = new URL(urlString);
                } catch (MalformedURLException e) {
                    throw new AnalysisException("Invalid url: " + e.getMessage());
                }
                handleShowLoadWarningsFromURL(showWarningsStmt, url);
                return;
            }
        }

        Database db = env.getInternalCatalog().getDbOrAnalysisException(showWarningsStmt.getDbName());
        ShowResultSet showResultSet = handleShowLoadWarningV2(showWarningsStmt, db);
        if (showResultSet != null) {
            resultSet = showResultSet;
            return;
        }

        long dbId = db.getId();
        Load load = env.getLoadInstance();
        long jobId = 0;
        LoadJob job = null;
        String label = null;
        if (showWarningsStmt.isFindByLabel()) {
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
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkDbPriv(ConnectContext.get(), db.getFullName(), PrivPredicate.SHOW)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                        ConnectContext.get().getQualifiedUser(), db.getFullName());
            }
        } else {
            for (String tblName : tableNames) {
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), db.getFullName(), tblName, PrivPredicate.SHOW)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW LOAD WARNING",
                            ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                            db.getFullName() + ": " + tblName);
                }
            }
        }
        List<List<String>> rows = Lists.newArrayList();
        long limit = showWarningsStmt.getLimitNum();
        if (limit != -1L && limit < rows.size()) {
            rows = rows.subList(0, (int) limit);
        }

        resultSet = new ShowResultSet(showWarningsStmt.getMetaData(), rows);
    }

    private ShowResultSet handleShowLoadWarningV2(ShowLoadWarningsStmt showWarningsStmt, Database db)
            throws AnalysisException {
        LoadManager loadManager = Env.getCurrentEnv().getLoadManager();
        if (showWarningsStmt.isFindByLabel()) {
            List<List<Comparable>> loadJobInfosByDb = loadManager.getLoadJobInfosByDb(db.getId(),
                    showWarningsStmt.getLabel(),
                    true, null);
            if (CollectionUtils.isEmpty(loadJobInfosByDb)) {
                return null;
            }
            List<List<String>> infoList = Lists.newArrayListWithCapacity(loadJobInfosByDb.size());
            for (List<Comparable> comparables : loadJobInfosByDb) {
                List<String> singleInfo = comparables.stream().map(Object::toString).collect(Collectors.toList());
                infoList.add(singleInfo);
            }
            return new ShowResultSet(showWarningsStmt.getMetaData(), infoList);
        }
        org.apache.doris.load.loadv2.LoadJob loadJob = loadManager.getLoadJob(showWarningsStmt.getJobId());
        if (loadJob == null) {
            return null;
        }
        List<String> singleInfo;
        try {
            singleInfo = loadJob
                    .getShowInfo()
                    .stream()
                    .map(Objects::toString)
                    .collect(Collectors.toList());
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
        return new ShowResultSet(showWarningsStmt.getMetaData(), Lists.newArrayList(Collections.singleton(singleInfo)));
    }

    private void handleShowLoadWarningsFromURL(ShowLoadWarningsStmt showWarningsStmt, URL url)
            throws AnalysisException {
        String host = url.getHost();
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        int port = url.getPort();
        SystemInfoService infoService = Env.getCurrentSystemInfo();
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
                matcher = PatternMatcherWrapper.createMysqlPattern(showRoutineLoadStmt.getPattern(),
                        CaseSensibility.ROUTINE_LOAD.getCaseSensibility());
            }
            routineLoadJobList = Env.getCurrentEnv().getRoutineLoadManager()
                    .getJob(showRoutineLoadStmt.getDbFullName(), showRoutineLoadStmt.getName(),
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
                if (routineLoadJob.isMultiTable()) {
                    if (!Env.getCurrentEnv().getAccessManager()
                            .checkDbPriv(ConnectContext.get(), dbFullName, PrivPredicate.LOAD)) {
                        LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId()).add("operator",
                                        "show routine load job").add("user", ConnectContext.get().getQualifiedUser())
                                .add("remote_ip", ConnectContext.get().getRemoteIP()).add("db_full_name", dbFullName)
                                .add("table_name", tableName).add("error_msg", "The database access denied"));
                        continue;
                    }
                    rows.add(routineLoadJob.getShowInfo());
                    continue;
                }
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), dbFullName, tableName, PrivPredicate.LOAD)) {
                    LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId()).add("operator",
                                    "show routine load job").add("user", ConnectContext.get().getQualifiedUser())
                            .add("remote_ip", ConnectContext.get().getRemoteIP()).add("db_full_name", dbFullName)
                            .add("table_name", tableName).add("error_msg", "The table access denied"));
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
            routineLoadJob = Env.getCurrentEnv().getRoutineLoadManager()
                    .getJob(showRoutineLoadTaskStmt.getDbFullName(), showRoutineLoadTaskStmt.getJobName());
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
            throw new AnalysisException("The table metadata of job has been changed."
                    + " The job will be cancelled automatically", e);
        }
        if (routineLoadJob.isMultiTable()) {
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkDbPriv(ConnectContext.get(), dbFullName, PrivPredicate.LOAD)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR, "LOAD",
                        ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                        dbFullName);
            }
            rows.addAll(routineLoadJob.getTasksShowInfo());
            resultSet = new ShowResultSet(showRoutineLoadTaskStmt.getMetaData(), rows);
            return;
        }
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), dbFullName, tableName, PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
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

        Env env = Env.getCurrentEnv();
        Database db = env.getInternalCatalog().getDbOrAnalysisException(showStmt.getDbName());
        long dbId = db.getId();

        DeleteHandler deleteHandler = env.getDeleteHandler();
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
        // Only SchemaChangeProc support where/order by/limit syntax
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

    private void handleShowQueryStats() throws AnalysisException {
        ShowQueryStatsStmt showStmt = (ShowQueryStatsStmt) stmt;
        resultSet = new ShowResultSet(showStmt.getMetaData(), showStmt.getResultRows());
    }

    private void handleShowPartitions() throws AnalysisException {
        ShowPartitionsStmt showStmt = (ShowPartitionsStmt) stmt;
        if (showStmt.getCatalog().isInternalCatalog()) {
            ProcNodeInterface procNodeI = showStmt.getNode();
            Preconditions.checkNotNull(procNodeI);
            List<List<String>> rows = ((PartitionsProcDir) procNodeI).fetchResultByFilter(showStmt.getFilterMap(),
                    showStmt.getOrderByPairs(), showStmt.getLimitElement()).getRows();
            resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
        } else {
            handleShowHMSTablePartitions(showStmt);
        }
    }

    private void handleShowHMSTablePartitions(ShowPartitionsStmt showStmt) {
        HMSExternalCatalog catalog = (HMSExternalCatalog) (showStmt.getCatalog());
        List<List<String>> rows = new ArrayList<>();
        String dbName = ClusterNamespace.getNameFromFullName(showStmt.getTableName().getDb());

        List<String> partitionNames = catalog.getClient().listPartitionNames(dbName,
                showStmt.getTableName().getTbl());
        for (String partition : partitionNames) {
            List<String> list = new ArrayList<>();
            list.add(partition);
            rows.add(list);
        }

        // sort by partition name
        rows.sort(Comparator.comparing(x -> x.get(0)));

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowTablet() throws AnalysisException {
        ShowTabletStmt showStmt = (ShowTabletStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();

        Env env = Env.getCurrentEnv();
        if (showStmt.isShowSingleTablet()) {
            long tabletId = showStmt.getTabletId();
            TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
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
            long queryHits = 0L;

            int tabletIdx = -1;
            // check real meta
            do {
                Database db = env.getInternalCatalog().getDbNullable(dbId);
                if (db == null) {
                    isSync = false;
                    break;
                }
                dbName = db.getFullName();
                Table table = db.getTableNullable(tableId);
                if (!(table instanceof OlapTable)) {
                    isSync = false;
                    break;
                }
                if (Config.enable_query_hit_stats) {
                    MaterializedIndex mi = ((OlapTable) table).getPartition(partitionId).getIndex(indexId);
                    if (mi != null) {
                        Tablet t = mi.getTablet(tabletId);
                        for (Replica r : t.getReplicas()) {
                            queryHits += QueryStatsUtil.getMergedReplicaStats(r.getId());
                        }
                    }
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
                    isSync.toString(), String.valueOf(tabletIdx), String.valueOf(queryHits), detailCmd));
        } else {
            Database db = env.getInternalCatalog().getDbOrAnalysisException(showStmt.getDbName());
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
                List<List<Comparable>> tabletInfos = new ArrayList<>();
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
                    tabletInfos = tabletInfos.subList((int) showStmt.getOffset(), (int) sizeLimit);
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
        List<List<String>> brokersInfo = Env.getCurrentEnv().getBrokerMgr().getBrokersInfo();

        // Only success
        resultSet = new ShowResultSet(showStmt.getMetaData(), brokersInfo);
    }

    // Handle show resources
    private void handleShowResources() {
        ShowResourcesStmt showStmt = (ShowResourcesStmt) stmt;
        List<List<Comparable>> resourcesInfos = Env.getCurrentEnv().getResourceMgr()
                .getResourcesInfo(showStmt.getNameValue(), showStmt.isAccurateMatch(), showStmt.getTypeSet());

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

    private void handleShowWorkloadGroups() {
        ShowWorkloadGroupsStmt showStmt = (ShowWorkloadGroupsStmt) stmt;
        List<List<String>> workloadGroupsInfos = Env.getCurrentEnv().getWorkloadGroupMgr().getResourcesInfo();

        resultSet = new ShowResultSet(showStmt.getMetaData(), workloadGroupsInfos);
    }

    private void handleShowExport() throws AnalysisException {
        ShowExportStmt showExportStmt = (ShowExportStmt) stmt;
        Env env = Env.getCurrentEnv();
        Database db = env.getInternalCatalog().getDbOrAnalysisException(showExportStmt.getDbName());
        long dbId = db.getId();

        ExportMgr exportMgr = env.getExportMgr();

        Set<ExportJob.JobState> states = null;
        ExportJob.JobState state = showExportStmt.getJobState();
        if (state != null) {
            states = Sets.newHashSet(state);
        }
        List<List<String>> infos = exportMgr.getExportJobInfosByIdOrState(
                dbId, showExportStmt.getJobId(), showExportStmt.getLabel(), showExportStmt.isLabelUseLike(), states,
                showExportStmt.getOrderByPairs(), showExportStmt.getLimit());

        resultSet = new ShowResultSet(showExportStmt.getMetaData(), infos);
    }

    private void handleShowBackends() {
        final ShowBackendsStmt showStmt = (ShowBackendsStmt) stmt;
        List<List<String>> backendInfos = BackendsProcDir.getBackendInfos();

        backendInfos.sort(new Comparator<List<String>>() {
            @Override
            public int compare(List<String> o1, List<String> o2) {
                return Integer.parseInt(o1.get(0)) - Integer.parseInt(o2.get(0));
            }
        });

        resultSet = new ShowResultSet(showStmt.getMetaData(), backendInfos);
    }

    private void handleShowFrontends() {
        final ShowFrontendsStmt showStmt = (ShowFrontendsStmt) stmt;
        List<List<String>> infos = Lists.newArrayList();
        FrontendsProcNode.getFrontendsInfo(Env.getCurrentEnv(), infos);

        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowRepositories() {
        final ShowRepositoriesStmt showStmt = (ShowRepositoriesStmt) stmt;
        List<List<String>> repoInfos = Env.getCurrentEnv().getBackupHandler().getRepoMgr().getReposInfo();
        resultSet = new ShowResultSet(showStmt.getMetaData(), repoInfos);
    }

    private void handleShowSnapshot() throws AnalysisException {
        final ShowSnapshotStmt showStmt = (ShowSnapshotStmt) stmt;
        Repository repo = Env.getCurrentEnv().getBackupHandler().getRepoMgr().getRepo(showStmt.getRepoName());
        if (repo == null) {
            throw new AnalysisException("Repository " + showStmt.getRepoName() + " does not exist");
        }

        List<List<String>> snapshotInfos = repo.getSnapshotInfos(showStmt.getSnapshotName(), showStmt.getTimestamp());
        resultSet = new ShowResultSet(showStmt.getMetaData(), snapshotInfos);
    }

    private void handleShowBackup() throws AnalysisException {
        ShowBackupStmt showStmt = (ShowBackupStmt) stmt;
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(showStmt.getDbName());

        List<AbstractJob> jobs = Env.getCurrentEnv().getBackupHandler()
                .getJobs(db.getId(), showStmt.getSnapshotPredicate());

        List<BackupJob> backupJobs = jobs.stream().filter(job -> job instanceof BackupJob)
                .map(job -> (BackupJob) job).collect(Collectors.toList());

        List<List<String>> infos = backupJobs.stream().map(BackupJob::getInfo).collect(Collectors.toList());

        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowRestore() throws AnalysisException {
        ShowRestoreStmt showStmt = (ShowRestoreStmt) stmt;
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(showStmt.getDbName());

        List<AbstractJob> jobs = Env.getCurrentEnv().getBackupHandler()
                .getJobs(db.getId(), showStmt.getLabelPredicate());

        List<RestoreJob> restoreJobs = jobs.stream().filter(job -> job instanceof RestoreJob)
                .map(job -> (RestoreJob) job).collect(Collectors.toList());

        List<List<String>> infos;
        if (showStmt.isNeedBriefResult()) {
            infos = restoreJobs.stream().map(RestoreJob::getBriefInfo).collect(Collectors.toList());
        } else {
            infos = restoreJobs.stream().map(RestoreJob::getFullInfo).collect(Collectors.toList());
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowSyncJobs() throws AnalysisException {
        ShowSyncJobStmt showStmt = (ShowSyncJobStmt) stmt;
        Env env = Env.getCurrentEnv();
        DatabaseIf db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(showStmt.getDbName());

        List<List<Comparable>> syncInfos = env.getSyncJobManager().getSyncJobsInfoByDbId(db.getId());
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
        List<List<String>> infos = Env.getCurrentEnv().getAuth().getAuthInfo(showStmt.getUserIdent());
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowRoles() {
        ShowRolesStmt showStmt = (ShowRolesStmt) stmt;
        List<List<String>> infos = Env.getCurrentEnv().getAuth().getRoleInfo();
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
            matcher = PatternMatcherWrapper.createMysqlPattern(showStmt.getPattern(),
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
            results = Env.getCurrentEnv().getSmallFileMgr().getInfo(showStmt.getDbName());
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), results);
    }

    private void handleShowDynamicPartition() throws AnalysisException {
        ShowDynamicPartitionStmt showDynamicPartitionStmt = (ShowDynamicPartitionStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        DatabaseIf db = ctx.getEnv().getInternalCatalog().getDbOrAnalysisException(showDynamicPartitionStmt.getDb());
        if (db != null && (db instanceof Database)) {
            List<Table> tableList = db.getTables();
            for (Table tbl : tableList) {
                if (!(tbl instanceof OlapTable)) {
                    continue;
                }

                DynamicPartitionScheduler dynamicPartitionScheduler = Env.getCurrentEnv()
                        .getDynamicPartitionScheduler();
                OlapTable olapTable = (OlapTable) tbl;
                olapTable.readLock();
                try {
                    if (!olapTable.dynamicPartitionExists()) {
                        dynamicPartitionScheduler.removeRuntimeInfo(olapTable.getId());
                        continue;
                    }

                    // check tbl privs
                    if (!Env.getCurrentEnv().getAccessManager()
                            .checkTblPriv(ConnectContext.get(), db.getFullName(), olapTable.getName(),
                                    PrivPredicate.SHOW)) {
                        continue;
                    }
                    DynamicPartitionProperty dynamicPartitionProperty
                            = olapTable.getTableProperty().getDynamicPartitionProperty();
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
                            dynamicPartitionScheduler.getRuntimeInfo(olapTable.getId(),
                                    DynamicPartitionScheduler.LAST_UPDATE_TIME),
                            dynamicPartitionScheduler.getRuntimeInfo(olapTable.getId(),
                                    DynamicPartitionScheduler.LAST_SCHEDULER_TIME),
                            dynamicPartitionScheduler.getRuntimeInfo(olapTable.getId(),
                                    DynamicPartitionScheduler.DYNAMIC_PARTITION_STATE),
                            dynamicPartitionScheduler.getRuntimeInfo(olapTable.getId(),
                                    DynamicPartitionScheduler.CREATE_PARTITION_MSG),
                            dynamicPartitionScheduler.getRuntimeInfo(olapTable.getId(),
                                    DynamicPartitionScheduler.DROP_PARTITION_MSG),
                            dynamicPartitionProperty.getSortedReservedHistoryPeriods(unsortedReservedHistoryPeriods,
                                    dynamicPartitionProperty.getTimeUnit().toUpperCase())));
                } catch (DdlException e) {
                    LOG.warn("", e);
                } finally {
                    olapTable.readUnlock();
                }
            }
        }
        resultSet = new ShowResultSet(showDynamicPartitionStmt.getMetaData(), rows);
    }

    // Show transaction statement.
    private void handleShowTransaction() throws AnalysisException {
        ShowTransactionStmt showStmt = (ShowTransactionStmt) stmt;
        DatabaseIf db = ctx.getEnv().getInternalCatalog().getDbOrAnalysisException(showStmt.getDbName());

        TransactionStatus status = showStmt.getStatus();
        GlobalTransactionMgr transactionMgr = Env.getCurrentGlobalTransactionMgr();
        if (status != TransactionStatus.UNKNOWN) {
            resultSet = new ShowResultSet(showStmt.getMetaData(),
                    transactionMgr.getDbTransInfoByStatus(db.getId(), status));
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
        List<List<String>> rows = Env.getCurrentPluginMgr().getPluginShowInfos();
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
            case FRAGMENTS: {
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
                    throw new AnalysisException("Failed to get instance list for fragment: "
                            + showStmt.getFragmentId());
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
                    throw new AnalysisException("Failed to get instance tree for instance: "
                            + showStmt.getInstanceId());
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
            case QUERY_IDS:
                rows = ProfileManager.getInstance().getQueryWithType(ProfileManager.ProfileType.LOAD);
                break;
            case TASK_IDS: {
                rows = ProfileManager.getInstance().getLoadJobTaskList(showStmt.getJobId());
                break;
            }
            case FRAGMENTS: {
                ProfileTreeNode treeRoot = ProfileManager.getInstance().getFragmentProfileTree(showStmt.getJobId(),
                        showStmt.getTaskId());
                if (treeRoot == null) {
                    throw new AnalysisException("Failed to get fragment tree for load: " + showStmt.getJobId());
                }
                List<String> row = Lists.newArrayList(ProfileTreePrinter.printFragmentTree(treeRoot));
                rows.add(row);
                break;
            }
            case INSTANCES: {
                // For load profile, there should be only one fragment in each execution profile
                // And the fragment id is 0.
                List<Triple<String, String, Long>> instanceList
                        = ProfileManager.getInstance().getFragmentInstanceList(showStmt.getJobId(),
                        showStmt.getTaskId(), ((ShowLoadProfileStmt) stmt).getFragmentId());
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
                        showStmt.getTaskId(), showStmt.getFragmentId(), showStmt.getInstanceId());
                if (treeRoot == null) {
                    throw new AnalysisException("Failed to get instance tree for instance: "
                            + showStmt.getInstanceId());
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

    private void handleShowCreateRepository() throws AnalysisException {
        ShowCreateRepositoryStmt showCreateRepositoryStmt = (ShowCreateRepositoryStmt) stmt;

        String repoName = showCreateRepositoryStmt.getRepoName();
        List<List<String>> rows = Lists.newArrayList();

        Repository repo = Env.getCurrentEnv().getBackupHandler().getRepoMgr().getRepo(repoName);
        if (repo == null) {
            throw new AnalysisException("repository not exist.");
        }
        rows.add(Lists.newArrayList(repoName, repo.getCreateStatement()));
        resultSet = new ShowResultSet(showCreateRepositoryStmt.getMetaData(), rows);
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
                routineLoadJobList = Env.getCurrentEnv().getRoutineLoadManager().getJob(dbName, labelName, true, null);
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
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), dbName, tableName, PrivPredicate.LOAD)) {
                    resultSet = new ShowResultSet(showCreateRoutineLoadStmt.getMetaData(), rows);
                    continue;
                }
                rows.add(Lists.newArrayList(String.valueOf(job.getId()),
                        showCreateRoutineLoadStmt.getLabel(), job.getShowCreateInfo()));
            }
        } else {
            // if job exists
            RoutineLoadJob routineLoadJob;
            try {
                routineLoadJob = Env.getCurrentEnv().getRoutineLoadManager().checkPrivAndGetJob(dbName, labelName);
                // get routine load info
                rows.add(Lists.newArrayList(String.valueOf(routineLoadJob.getId()),
                        showCreateRoutineLoadStmt.getLabel(), routineLoadJob.getShowCreateInfo()));
            } catch (MetaNotFoundException | DdlException e) {
                LOG.warn(e.getMessage(), e);
                throw new AnalysisException(e.getMessage());
            }
        }
        resultSet = new ShowResultSet(showCreateRoutineLoadStmt.getMetaData(), rows);
    }

    private void handleShowCreateLoad() throws AnalysisException {
        ShowCreateLoadStmt showCreateLoadStmt = (ShowCreateLoadStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        String labelName = showCreateLoadStmt.getLabel();

        Util.prohibitExternalCatalog(ctx.getDefaultCatalog(), stmt.getClass().getSimpleName());
        Env env = ctx.getEnv();
        DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(showCreateLoadStmt.getDb());
        long dbId = db.getId();
        try {
            List<Pair<Long, String>> result = env.getLoadManager().getCreateLoadStmt(dbId, labelName);
            rows.addAll(result.stream().map(pair -> Lists.newArrayList(String.valueOf(pair.first), pair.second))
                    .collect(Collectors.toList()));
        } catch (DdlException e) {
            LOG.warn(e.getMessage(), e);
            throw new AnalysisException(e.getMessage());
        }
        resultSet = new ShowResultSet(showCreateLoadStmt.getMetaData(), rows);
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

    private void handleShowTableStats() {
        ShowTableStatsStmt showTableStatsStmt = (ShowTableStatsStmt) stmt;
        TableIf tableIf = showTableStatsStmt.getTable();
        TableStatsMeta tableStats = Env.getCurrentEnv().getAnalysisManager().findTableStatsStatus(tableIf.getId());
        /*
           HMSExternalTable table will fetch row count from HMS
           or estimate with file size and schema if it's not analyzed.
           tableStats == null means it's not analyzed, in this case show the estimated row count.
         */
        if (tableStats == null && tableIf instanceof HMSExternalTable) {
            resultSet = showTableStatsStmt.constructResultSet(tableIf.estimatedRowCount());
        } else {
            resultSet = showTableStatsStmt.constructResultSet(tableStats);
        }
    }

    private void handleShowColumnStats() throws AnalysisException {
        ShowColumnStatsStmt showColumnStatsStmt = (ShowColumnStatsStmt) stmt;
        TableName tableName = showColumnStatsStmt.getTableName();
        TableIf tableIf = showColumnStatsStmt.getTable();
        List<Pair<String, ColumnStatistic>> columnStatistics = new ArrayList<>();
        Set<String> columnNames = showColumnStatsStmt.getColumnNames();
        PartitionNames partitionNames = showColumnStatsStmt.getPartitionNames();
        boolean showCache = showColumnStatsStmt.isCached();

        for (String colName : columnNames) {
            // Show column statistics in columnStatisticsCache. For validation.
            if (showCache) {
                ColumnStatistic columnStatistic = Env.getCurrentEnv().getStatisticsCache().getColumnStatistics(
                        tableIf.getDatabase().getCatalog().getId(),
                        tableIf.getDatabase().getId(), tableIf.getId(), colName);
                columnStatistics.add(Pair.of(colName, columnStatistic));
            } else if (partitionNames == null) {
                ColumnStatistic columnStatistic =
                        StatisticsRepository.queryColumnStatisticsByName(tableIf.getId(), colName);
                columnStatistics.add(Pair.of(colName, columnStatistic));
            } else {
                columnStatistics.addAll(StatisticsRepository.queryColumnStatisticsByPartitions(tableName,
                                colName, showColumnStatsStmt.getPartitionNames().getPartitionNames())
                        .stream().map(s -> Pair.of(colName, s))
                        .collect(Collectors.toList()));
            }

        }
        resultSet = showColumnStatsStmt.constructResultSet(columnStatistics);
    }

    public void handleShowColumnHist() {
        ShowColumnHistStmt showColumnHistStmt = (ShowColumnHistStmt) stmt;
        TableIf tableIf = showColumnHistStmt.getTable();
        Set<String> columnNames = showColumnHistStmt.getColumnNames();

        List<Pair<String, Histogram>> columnStatistics = columnNames.stream()
                .map(colName -> Pair.of(colName,
                        StatisticsRepository.queryColumnHistogramByName(tableIf.getId(), colName)))
                .collect(Collectors.toList());

        resultSet = showColumnHistStmt.constructResultSet(columnStatistics);
    }

    public void handleShowSqlBlockRule() throws AnalysisException {
        ShowSqlBlockRuleStmt showStmt = (ShowSqlBlockRuleStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        List<SqlBlockRule> sqlBlockRules = Env.getCurrentEnv().getSqlBlockRuleMgr().getSqlBlockRule(showStmt);
        sqlBlockRules.forEach(rule -> rows.add(rule.getShowInfo()));
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowTableCreation() throws AnalysisException {
        ShowTableCreationStmt showStmt = (ShowTableCreationStmt) stmt;
        String dbName = showStmt.getDbName();
        DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(dbName);

        List<IcebergTableCreationRecord> records = ctx.getEnv().getIcebergTableCreationRecordMgr()
                .getTableCreationRecordByDbId(db.getId());

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
        for (Backend be : Env.getCurrentSystemInfo().getIdToBackend().values()) {
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

    private void handleShowCreateMaterializedView() throws AnalysisException {
        List<List<String>> resultRowSet = new ArrayList<>();
        ShowCreateMaterializedViewStmt showStmt = (ShowCreateMaterializedViewStmt) stmt;
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(showStmt.getTableName().getDb());
        Table table = db.getTableOrAnalysisException(showStmt.getTableName().getTbl());
        if (table instanceof OlapTable) {
            OlapTable baseTable = ((OlapTable) table);
            Long indexIdByName = baseTable.getIndexIdByName(showStmt.getMvName());
            if (indexIdByName != null) {
                MaterializedIndexMeta meta = baseTable.getIndexMetaByIndexId(indexIdByName);
                if (meta != null && meta.getDefineStmt() != null) {
                    String originStmt = meta.getDefineStmt().originStmt;
                    List<String> data = new ArrayList<>();
                    data.add(showStmt.getTableName().getTbl());
                    data.add(showStmt.getMvName());
                    data.add(originStmt);
                    resultRowSet.add(data);
                }
            }
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), resultRowSet);
    }

    public void handleShowPolicy() throws AnalysisException {
        ShowPolicyStmt showStmt = (ShowPolicyStmt) stmt;
        resultSet = Env.getCurrentEnv().getPolicyMgr().showPolicy(showStmt);
    }

    public void handleShowCatalogs() throws AnalysisException {
        ShowCatalogStmt showStmt = (ShowCatalogStmt) stmt;
        resultSet = Env.getCurrentEnv().getCatalogMgr().showCatalogs(showStmt, ctx.getCurrentCatalog() != null
            ? ctx.getCurrentCatalog().getName() : null);
    }

    // Show create catalog
    private void handleShowCreateCatalog() throws AnalysisException {
        ShowCreateCatalogStmt showStmt = (ShowCreateCatalogStmt) stmt;

        resultSet = Env.getCurrentEnv().getCatalogMgr().showCreateCatalog(showStmt);
    }

    private void handleShowAnalyze() {
        ShowAnalyzeStmt showStmt = (ShowAnalyzeStmt) stmt;
        List<AnalysisInfo> results = Env.getCurrentEnv().getAnalysisManager()
                .showAnalysisJob(showStmt);
        List<List<String>> resultRows = Lists.newArrayList();
        for (AnalysisInfo analysisInfo : results) {
            List<String> row = new ArrayList<>();
            row.add(String.valueOf(analysisInfo.jobId));
            row.add(analysisInfo.catalogName);
            row.add(analysisInfo.dbName);
            row.add(analysisInfo.tblName);
            row.add(analysisInfo.colName);
            row.add(analysisInfo.jobType.toString());
            row.add(analysisInfo.analysisType.toString());
            row.add(analysisInfo.message);
            row.add(TimeUtils.DATETIME_FORMAT.format(
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(analysisInfo.lastExecTimeInMs),
                            ZoneId.systemDefault())));
            row.add(analysisInfo.state.toString());
            row.add(Env.getCurrentEnv().getAnalysisManager().getJobProgress(analysisInfo.jobId));
            row.add(analysisInfo.scheduleType.toString());
            resultRows.add(row);
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), resultRows);
    }

    private void handleCopyTablet() throws AnalysisException {
        AdminCopyTabletStmt copyStmt = (AdminCopyTabletStmt) stmt;
        long tabletId = copyStmt.getTabletId();
        long version = copyStmt.getVersion();
        long backendId = copyStmt.getBackendId();

        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
        if (tabletMeta == null) {
            throw new AnalysisException("Unknown tablet: " + tabletId);
        }

        // 1. find replica
        Replica replica = null;
        if (backendId != -1) {
            replica = invertedIndex.getReplica(tabletId, backendId);
        } else {
            List<Replica> replicas = invertedIndex.getReplicasByTabletId(tabletId);
            if (!replicas.isEmpty()) {
                replica = replicas.get(0);
            }
        }
        if (replica == null) {
            throw new AnalysisException("Replica not found on backend: " + backendId);
        }
        backendId = replica.getBackendId();
        Backend be = Env.getCurrentSystemInfo().getBackend(backendId);
        if (be == null || !be.isAlive()) {
            throw new AnalysisException("Unavailable backend: " + backendId);
        }

        // 2. find version
        if (version != -1 && replica.getVersion() < version) {
            throw new AnalysisException("Version is larger than replica max version: " + replica.getVersion());
        }
        version = version == -1 ? replica.getVersion() : version;

        // 3. get create table stmt
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(tabletMeta.getDbId());
        OlapTable tbl = (OlapTable) db.getTableNullable(tabletMeta.getTableId());
        if (tbl == null) {
            throw new AnalysisException("Failed to find table: " + tabletMeta.getTableId());
        }

        List<String> createTableStmt = Lists.newArrayList();
        tbl.readLock();
        try {
            Env.getDdlStmt(tbl, createTableStmt, null, null, false, true /* hide password */, version);
        } finally {
            tbl.readUnlock();
        }

        // 4. create snapshot task
        SnapshotTask task = new SnapshotTask(null, backendId, tabletId, -1, tabletMeta.getDbId(),
                tabletMeta.getTableId(), tabletMeta.getPartitionId(), tabletMeta.getIndexId(), tabletId, version, 0,
                copyStmt.getExpirationMinutes() * 60 * 1000, false);
        task.setIsCopyTabletTask(true);
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<Long, Long>(1);
        countDownLatch.addMark(backendId, tabletId);
        task.setCountDownLatch(countDownLatch);

        // 5. send task and wait
        AgentBatchTask batchTask = new AgentBatchTask();
        batchTask.addTask(task);
        try {
            AgentTaskQueue.addBatchTask(batchTask);
            AgentTaskExecutor.submit(batchTask);

            boolean ok = false;
            try {
                ok = countDownLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
                ok = false;
            }

            if (!ok) {
                throw new AnalysisException(
                        "Failed to make snapshot for tablet " + tabletId + " on backend: " + backendId);
            }

            // send result
            List<List<String>> resultRowSet = Lists.newArrayList();
            List<String> row = Lists.newArrayList();
            row.add(String.valueOf(tabletId));
            row.add(String.valueOf(backendId));
            row.add(be.getHost());
            row.add(task.getResultSnapshotPath());
            row.add(String.valueOf(copyStmt.getExpirationMinutes()));
            row.add(createTableStmt.get(0));
            resultRowSet.add(row);

            ShowResultSetMetaData showMetaData = copyStmt.getMetaData();
            resultSet = new ShowResultSet(showMetaData, resultRowSet);
        } finally {
            AgentTaskQueue.removeBatchTask(batchTask, TTaskType.MAKE_SNAPSHOT);
        }
    }

    private void handleShowCatalogRecycleBin() throws AnalysisException {
        ShowCatalogRecycleBinStmt showStmt = (ShowCatalogRecycleBinStmt) stmt;

        Predicate<String> predicate = showStmt.getNamePredicate();
        List<List<String>> infos = Env.getCurrentRecycleBin().getInfo().stream()
                .filter(x -> predicate.test(x.get(1)))
                .collect(Collectors.toList());

        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleMTMVJobs() throws AnalysisException {
        ShowMTMVJobStmt showStmt = (ShowMTMVJobStmt) stmt;
        MTMVJobManager jobManager = Env.getCurrentEnv().getMTMVJobManager();
        List<MTMVJob> jobs = Lists.newArrayList();
        if (showStmt.isShowAllJobs()) {
            jobs.addAll(jobManager.showAllJobs());
        } else if (showStmt.isShowAllJobsFromDb()) {
            jobs.addAll(jobManager.showJobs(showStmt.getDbName()));
        } else if (showStmt.isShowAllJobsOnMv()) {
            jobs.addAll(jobManager.showJobs(showStmt.getDbName(), showStmt.getMVName()));
        } else if (showStmt.isSpecificJob()) {
            jobs.add(jobManager.getJob(showStmt.getJobName()));
        }
        List<List<String>> results = Lists.newArrayList();
        for (MTMVJob job : jobs) {
            results.add(job.toStringRow());
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), results);
    }

    private void handleMTMVTasks() throws AnalysisException {
        ShowMTMVTaskStmt showStmt = (ShowMTMVTaskStmt) stmt;
        MTMVJobManager jobManager = Env.getCurrentEnv().getMTMVJobManager();
        List<MTMVTask> tasks = Lists.newArrayList();
        if (showStmt.isShowAllTasks()) {
            tasks.addAll(jobManager.getTaskManager().showAllTasks());
        } else if (showStmt.isShowAllTasksFromDb()) {
            tasks.addAll(jobManager.getTaskManager().showTasksWithLock(showStmt.getDbName()));
        } else if (showStmt.isShowAllTasksOnMv()) {
            tasks.addAll(jobManager.getTaskManager().showTasks(showStmt.getDbName(), showStmt.getMVName()));
        } else if (showStmt.isSpecificTask()) {
            tasks.add(jobManager.getTaskManager().getTask(showStmt.getTaskId()));
        }
        List<List<String>> results = Lists.newArrayList();
        for (MTMVTask task : tasks) {
            results.add(task.toStringRow());
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), results);
    }

    private void handleShowTypeCastStmt() throws AnalysisException {
        ShowTypeCastStmt showStmt = (ShowTypeCastStmt) stmt;

        Util.prohibitExternalCatalog(ctx.getDefaultCatalog(), stmt.getClass().getSimpleName());
        DatabaseIf db = ctx.getCurrentCatalog().getDbOrAnalysisException(showStmt.getDbName());

        List<List<String>> resultRowSet = Lists.newArrayList();
        ImmutableSetMultimap<PrimitiveType, PrimitiveType> castMap = PrimitiveType.getImplicitCastMap();
        if (db instanceof Database) {
            resultRowSet = castMap.entries().stream().map(primitiveTypePrimitiveTypeEntry -> {
                List<String> list = Lists.newArrayList();
                list.add(primitiveTypePrimitiveTypeEntry.getKey().toString());
                list.add(primitiveTypePrimitiveTypeEntry.getValue().toString());
                return list;
            }).collect(Collectors.toList());
        }

        // Only success
        ShowResultSetMetaData showMetaData = showStmt.getMetaData();
        resultSet = new ShowResultSet(showMetaData, resultRowSet);
    }

    private void  handleShowBuildIndexStmt() throws AnalysisException {
        ShowBuildIndexStmt showStmt = (ShowBuildIndexStmt) stmt;
        ProcNodeInterface procNodeI = showStmt.getNode();
        Preconditions.checkNotNull(procNodeI);
        // List<List<String>> rows = ((BuildIndexProcDir) procNodeI).fetchResult().getRows();
        List<List<String>> rows = ((BuildIndexProcDir) procNodeI).fetchResultByFilter(showStmt.getFilterMap(),
                    showStmt.getOrderPairs(), showStmt.getLimitElement()).getRows();
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowAnalyzeTaskStatus() {
        ShowAnalyzeTaskStatus showStmt = (ShowAnalyzeTaskStatus) stmt;
        List<AnalysisInfo> analysisInfos = Env.getCurrentEnv().getAnalysisManager().findTasks(showStmt.getJobId());
        List<List<String>> rows = new ArrayList<>();
        for (AnalysisInfo analysisInfo : analysisInfos) {
            List<String> row = new ArrayList<>();
            row.add(String.valueOf(analysisInfo.taskId));
            row.add(analysisInfo.colName);
            row.add(analysisInfo.message);
            row.add(TimeUtils.DATETIME_FORMAT.format(
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(analysisInfo.lastExecTimeInMs),
                            ZoneId.systemDefault())));
            row.add(String.valueOf(analysisInfo.timeCostInMs));
            row.add(analysisInfo.state.toString());
            rows.add(row);
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }


    private void handleShowConvertLSC() {
        ShowConvertLSCStmt showStmt = (ShowConvertLSCStmt) stmt;
        ColumnIdFlushDaemon columnIdFlusher = Env.getCurrentEnv().getColumnIdFlusher();
        columnIdFlusher.readLock();
        List<List<String>> rows;
        try {
            Map<String, Map<String, ColumnIdFlushDaemon.FlushStatus>> resultCollector =
                    columnIdFlusher.getResultCollector();
            rows = new ArrayList<>();
            String db = ((ShowConvertLSCStmt) stmt).getDbName();
            if (db != null) {
                Map<String, ColumnIdFlushDaemon.FlushStatus> tblNameToStatus = resultCollector.get(db);
                if (tblNameToStatus != null) {
                    tblNameToStatus.forEach((tblName, status) -> {
                        List<String> row = new ArrayList<>();
                        row.add(db);
                        row.add(tblName);
                        row.add(status.getMsg());
                        rows.add(row);
                    });
                }
            } else {
                resultCollector.forEach((dbName, tblNameToStatus) ->
                        tblNameToStatus.forEach((tblName, status) -> {
                            List<String> row = new ArrayList<>();
                            row.add(dbName);
                            row.add(tblName);
                            row.add(status.getMsg());
                            rows.add(row);
                        }));
            }
        } finally {
            columnIdFlusher.readUnlock();
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

}

