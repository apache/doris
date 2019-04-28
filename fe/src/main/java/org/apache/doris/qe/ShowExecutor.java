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

import com.google.common.base.Strings;
import org.apache.doris.analysis.AdminShowConfigStmt;
import org.apache.doris.analysis.AdminShowReplicaDistributionStmt;
import org.apache.doris.analysis.AdminShowReplicaStatusStmt;
import org.apache.doris.analysis.DescribeStmt;
import org.apache.doris.analysis.HelpStmt;
import org.apache.doris.analysis.ShowAlterStmt;
import org.apache.doris.analysis.ShowAuthorStmt;
import org.apache.doris.analysis.ShowBackendsStmt;
import org.apache.doris.analysis.ShowBackupStmt;
import org.apache.doris.analysis.ShowBrokerStmt;
import org.apache.doris.analysis.ShowClusterStmt;
import org.apache.doris.analysis.ShowCollationStmt;
import org.apache.doris.analysis.ShowColumnStmt;
import org.apache.doris.analysis.ShowCreateDbStmt;
import org.apache.doris.analysis.ShowCreateTableStmt;
import org.apache.doris.analysis.ShowDataStmt;
import org.apache.doris.analysis.ShowDbStmt;
import org.apache.doris.analysis.ShowDeleteStmt;
import org.apache.doris.analysis.ShowEnginesStmt;
import org.apache.doris.analysis.ShowExportStmt;
import org.apache.doris.analysis.ShowFrontendsStmt;
import org.apache.doris.analysis.ShowGrantsStmt;
import org.apache.doris.analysis.ShowLoadStmt;
import org.apache.doris.analysis.ShowLoadWarningsStmt;
import org.apache.doris.analysis.ShowMigrationsStmt;
import org.apache.doris.analysis.ShowPartitionsStmt;
import org.apache.doris.analysis.ShowProcStmt;
import org.apache.doris.analysis.ShowProcesslistStmt;
import org.apache.doris.analysis.ShowRepositoriesStmt;
import org.apache.doris.analysis.ShowRestoreStmt;
import org.apache.doris.analysis.ShowRolesStmt;
import org.apache.doris.analysis.ShowRollupStmt;
import org.apache.doris.analysis.ShowRoutineLoadStmt;
import org.apache.doris.analysis.ShowRoutineLoadTaskStmt;
import org.apache.doris.analysis.ShowSnapshotStmt;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.ShowTableStatusStmt;
import org.apache.doris.analysis.ShowTableStmt;
import org.apache.doris.analysis.ShowTabletStmt;
import org.apache.doris.analysis.ShowUserPropertyStmt;
import org.apache.doris.analysis.ShowVariablesStmt;
import org.apache.doris.backup.AbstractJob;
import org.apache.doris.backup.BackupJob;
import org.apache.doris.backup.Repository;
import org.apache.doris.backup.RestoreJob;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MetadataViewer;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.View;
import org.apache.doris.cluster.BaseParam;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.proc.BackendsProcDir;
import org.apache.doris.common.proc.FrontendsProcNode;
import org.apache.doris.common.proc.LoadProcDir;
import org.apache.doris.common.proc.PartitionsProcDir;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.TabletsProcDir;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
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
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        } else if (stmt instanceof ShowTableStmt) {
            handleShowTable();
        } else if (stmt instanceof ShowTableStatusStmt) {
            handleShowTableStatus();
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
        } else if (stmt instanceof ShowVariablesStmt) {
            handleShowVariables();
        } else if (stmt instanceof ShowColumnStmt) {
            handleShowColumn();
        } else if (stmt instanceof ShowLoadStmt) {
            handleShowLoad();
        } else if (stmt instanceof ShowLoadWarningsStmt) {
            handleShowLoadWarnings();
        } else if (stmt instanceof ShowRoutineLoadStmt) {
            handleShowRoutineLoad();
        } else if (stmt instanceof ShowRoutineLoadTaskStmt) {
            handleShowRoutineLoadTask();
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
        } else if (stmt instanceof AdminShowReplicaStatusStmt) {
            handleAdminShowTabletStatus();
        } else if (stmt instanceof AdminShowReplicaDistributionStmt) {
            handleAdminShowTabletDistribution();
        } else if (stmt instanceof AdminShowConfigStmt) {
            handleAdminShowConfig();
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

        // Only success
        resultSet = new ShowResultSet(showStmt.getMetaData(), rowSet);
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
        Database db = ctx.getCatalog().getDb(showTableStmt.getDb());
        if (db != null) {
            Map<String, String> tableMap = Maps.newTreeMap();
            db.readLock();
            try {
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
                    tableMap.put(tbl.getName(), tbl.getMysqlType());
                }
            } finally {
                db.readUnlock();
            }

            for (Map.Entry<String, String> entry : tableMap.entrySet()) {
                if (showTableStmt.isVerbose()) {
                    rows.add(Lists.newArrayList(entry.getKey(), entry.getValue()));
                } else {
                    rows.add(Lists.newArrayList(entry.getKey()));
                }
            }
        }
        resultSet = new ShowResultSet(showTableStmt.getMetaData(), rows);
    }

    // Show table status statement.
    private void handleShowTableStatus() throws AnalysisException {
        ShowTableStatusStmt showStmt = (ShowTableStatusStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        Database db = ctx.getCatalog().getDb(showStmt.getDb());
        if (db != null) {
            db.readLock();
            try {
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
                    // version, ra
                    for (int i = 0; i < 15; ++i) {
                        row.add(null);
                    }
                    row.add(table.getComment());
                    rows.add(row);
                }
            } finally {
                db.readUnlock();
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
        Database db = ctx.getCatalog().getDb(showStmt.getDb());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDb());
        }
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE DATABASE `").append(ClusterNamespace.getNameFromFullName(showStmt.getDb())).append("`");
        rows.add(Lists.newArrayList(ClusterNamespace.getNameFromFullName(showStmt.getDb()), sb.toString()));
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show create table
    private void handleShowCreateTable() throws AnalysisException {
        ShowCreateTableStmt showStmt = (ShowCreateTableStmt) stmt;
        Database db = ctx.getCatalog().getDb(showStmt.getDb());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDb());
        }
        List<List<String>> rows = Lists.newArrayList();
        db.readLock();
        try {
            Table table = db.getTable(showStmt.getTable());
            if (table == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, showStmt.getTable());
            }

            List<String> createTableStmt = Lists.newArrayList();
            Catalog.getDdlStmt(table, createTableStmt, null, null, false, (short) -1, true /* hide password */);
            if (createTableStmt.isEmpty()) {
                resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
                return;
            }

            if (table instanceof View) {
                View view = (View) table;
                StringBuilder sb = new StringBuilder();
                sb.append("CREATE VIEW `").append(table.getName()).append("` AS ").append(view.getInlineViewDef());
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
            db.readUnlock();
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
        Database db = ctx.getCatalog().getDb(showStmt.getDb());
        if (db != null) {
            db.readLock();
            try {
                Table table = db.getTable(showStmt.getTable());
                if (table != null) {
                    PatternMatcher matcher = null;
                    if (showStmt.getPattern() != null) {
                        matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern(),
                                                                    CaseSensibility.COLUMN.getCaseSensibility());
                    }
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
                }
            } finally {
                db.readUnlock();
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

        Catalog catalog = Catalog.getInstance();
        Database db = catalog.getDb(showStmt.getDbName());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDbName());
        }
        long dbId = db.getId();

        Load load = catalog.getLoadInstance();
        List<List<Comparable>> loadInfos = load.getLoadJobInfosByDb(dbId, db.getFullName(),
                                                                    showStmt.getLabelValue(),
                                                                    showStmt.isAccurateMatch(),
                                                                    showStmt.getStates(),
                                                                    showStmt.getOrderByPairs());
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
        if (limit != -1L && limit < rows.size()) {
            rows = rows.subList(0, (int) limit);
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowLoadWarnings() throws AnalysisException {
        ShowLoadWarningsStmt showWarningsStmt = (ShowLoadWarningsStmt) stmt;

        if (showWarningsStmt.getURL() != null) {
            handleShowLoadWarningsFromURL(showWarningsStmt, showWarningsStmt.getURL());
            return;
        }

        Catalog catalog = Catalog.getInstance();
        Database db = catalog.getDb(showWarningsStmt.getDbName());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showWarningsStmt.getDbName());
        }

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
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED,
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
                                                        tblName);
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
        if (!be.isAvailable()) {
            throw new AnalysisException("Backend " + host + ":" + port + " is not available");
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
                    rows.add(Lists.newArrayList("-1", "N/A", line));
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
            routineLoadJobList =
                    Catalog.getCurrentCatalog().getRoutineLoadManager().getJob(showRoutineLoadStmt.getDbFullName(),
                                                                               showRoutineLoadStmt.getName(),
                                                                               showRoutineLoadStmt.isIncludeHistory());
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
                                                + " include history " + showRoutineLoadStmt.isIncludeHistory());
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
                                                tableName);
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

        Catalog catalog = Catalog.getInstance();
        Database db = catalog.getDb(showStmt.getDbName());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDbName());
        }
        long dbId = db.getId();

        Load load = catalog.getLoadInstance();
        List<List<Comparable>> deleteInfos = load.getDeleteInfosByDb(dbId, true);
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
        List<List<String>> rows = procNodeI.fetchResult().getRows();

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
        List<List<String>> rows = procNodeI.fetchResult().getRows();

        if (showStmt.getPartitionName() != null) {
            // filter by partition name
            List<List<String>> oneRow = Lists.newArrayList();
            String partitionName = showStmt.getPartitionName();
            Iterator<List<String>> iter = rows.iterator();
            while (iter.hasNext()) {
                List<String> row = iter.next();
                if (row.get(PartitionsProcDir.PARTITION_NAME_INDEX).equalsIgnoreCase(partitionName)) {
                    oneRow.add(row);
                    break;
                }
            }
            rows = oneRow;
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowTablet() throws AnalysisException {
        ShowTabletStmt showStmt = (ShowTabletStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();

        Catalog catalog = Catalog.getInstance();
        if (showStmt.isShowSingleTablet()) {
            long tabletId = showStmt.getTabletId();
            TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();

            Long dbId = invertedIndex.getDbId(tabletId);
            String dbName = null;
            Long tableId = invertedIndex.getTableId(tabletId);
            String tableName = null;
            Long partitionId = invertedIndex.getPartitionId(tabletId);
            String partitionName = null;
            Long indexId = invertedIndex.getIndexId(tabletId);
            String indexName = null;
            Boolean isSync = true;

            // check real meta
            do {
                Database db = catalog.getDb(dbId);
                if (db == null) {
                    isSync = false;
                    break;
                }
                dbName = db.getFullName();

                db.readLock();
                try {
                    Table table = db.getTable(tableId);
                    if (table == null || !(table instanceof OlapTable)) {
                        isSync = false;
                        break;
                    }
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
                } finally {
                    db.readUnlock();
                }
            } while (false);

            String detailCmd = String.format("SHOW PROC '/dbs/%d/%d/partitions/%d/%d/%d';",
                                             dbId, tableId, partitionId, indexId, tabletId);
            rows.add(Lists.newArrayList(dbName, tableName, partitionName, indexName,
                                        dbId.toString(), tableId.toString(),
                                        partitionId.toString(), indexId.toString(),
                                        isSync.toString(), detailCmd));
        } else {
            Database db = catalog.getDb(showStmt.getDbName());
            if (db == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDbName());
            }

            db.readLock();
            try {
                Table table = db.getTable(showStmt.getTableName());
                if (table == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, showStmt.getTableName());
                }
                if (!(table instanceof OlapTable)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NOT_OLAP_TABLE, showStmt.getTableName());
                }

                OlapTable olapTable = (OlapTable) table;

                for (Partition partition : olapTable.getPartitions()) {
                    for (MaterializedIndex index : partition.getMaterializedIndices()) {
                        TabletsProcDir procDir = new TabletsProcDir(db, index);
                        rows.addAll(procDir.fetchResult().getRows());
                    }
                }

            } finally {
                db.readUnlock();
            }
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Handle show brokers
    private void handleShowBroker() {
        ShowBrokerStmt showStmt = (ShowBrokerStmt) stmt;
        List<List<String>> rowSet = Catalog.getInstance().getBrokerMgr().getBrokersInfo();

        // Only success
        resultSet = new ShowResultSet(showStmt.getMetaData(), rowSet);
    }

    private void handleShowExport() throws AnalysisException {
        ShowExportStmt showExportStmt = (ShowExportStmt) stmt;

        Catalog catalog = Catalog.getInstance();
        Database db = catalog.getDb(showExportStmt.getDbName());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showExportStmt.getDbName());
        }
        long dbId = db.getId();

        ExportMgr exportMgr = catalog.getExportMgr();

        Set<ExportJob.JobState> states = null;
        ExportJob.JobState state = showExportStmt.getJobState();
        if (state != null) {
            states = Sets.newHashSet(state);
        }
        List<List<Comparable>> infos = exportMgr.getExportJobInfosByIdOrState(
                dbId, showExportStmt.getJobId(),
                states, showExportStmt.getOrderByPairs());
        List<List<String>> rows = Lists.newArrayList();
        for (List<Comparable> loadInfo : infos) {
            List<String> oneInfo = new ArrayList<String>(loadInfo.size());
            for (Comparable element : loadInfo) {
                oneInfo.add(element.toString());
            }
            rows.add(oneInfo);
        }

        // filter by limit
        long limit = showExportStmt.getLimit();
        if (limit != -1L && limit < rows.size()) {
            rows = rows.subList(0, (int) limit);
        }

        resultSet = new ShowResultSet(showExportStmt.getMetaData(), rows);
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
        List<List<String>> repoInfos = Catalog.getInstance().getBackupHandler().getRepoMgr().getReposInfo();
        resultSet = new ShowResultSet(showStmt.getMetaData(), repoInfos);
    }

    private void handleShowSnapshot() throws AnalysisException {
        final ShowSnapshotStmt showStmt = (ShowSnapshotStmt) stmt;
        Repository repo = Catalog.getInstance().getBackupHandler().getRepoMgr().getRepo(showStmt.getRepoName());
        if (repo == null) {
            throw new AnalysisException("Repository " + showStmt.getRepoName() + " does not exist");
        }

        List<List<String>> snapshotInfos = repo.getSnapshotInfos(showStmt.getSnapshotName(), showStmt.getTimestamp());
        resultSet = new ShowResultSet(showStmt.getMetaData(), snapshotInfos);
    }

    private void handleShowBackup() throws AnalysisException {
        ShowBackupStmt showStmt = (ShowBackupStmt) stmt;
        Database db = Catalog.getInstance().getDb(showStmt.getDbName());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDbName());
        }

        AbstractJob jobI = Catalog.getInstance().getBackupHandler().getJob(db.getId());
        if (!(jobI instanceof BackupJob)) {
            resultSet = new ShowResultSet(showStmt.getMetaData(), EMPTY_SET);
            return;
        }

        BackupJob backupJob = (BackupJob) jobI;
        List<String> info = backupJob.getInfo();
        List<List<String>> infos = Lists.newArrayList();
        infos.add(info);
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowRestore() throws AnalysisException {
        ShowRestoreStmt showStmt = (ShowRestoreStmt) stmt;
        Database db = Catalog.getInstance().getDb(showStmt.getDbName());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDbName());
        }

        AbstractJob jobI = Catalog.getInstance().getBackupHandler().getJob(db.getId());
        if (!(jobI instanceof RestoreJob)) {
            resultSet = new ShowResultSet(showStmt.getMetaData(), EMPTY_SET);
            return;
        }

        RestoreJob restoreJob = (RestoreJob) jobI;
        List<String> info = restoreJob.getInfo();
        List<List<String>> infos = Lists.newArrayList();
        infos.add(info);
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowGrants() {
        ShowGrantsStmt showStmt = (ShowGrantsStmt) stmt;
        List<List<String>> infos = Catalog.getCurrentCatalog().getAuth().getAuthInfo(showStmt.getUserIdent(),
                                                                                     showStmt.isAll());
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowRoles() {
        ShowRolesStmt showStmt = (ShowRolesStmt) stmt;
        List<List<String>> infos = Catalog.getCurrentCatalog().getAuth().getRoleInfo();
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
        try {
            results = ConfigBase.getConfigInfo();
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), results);
    }

}



