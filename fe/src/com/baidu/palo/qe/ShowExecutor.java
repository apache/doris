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

package com.baidu.palo.qe;

import com.baidu.palo.analysis.DescribeStmt;
import com.baidu.palo.analysis.HelpStmt;
import com.baidu.palo.analysis.ShowAlterStmt;
import com.baidu.palo.analysis.ShowAuthorStmt;
import com.baidu.palo.analysis.ShowBackendsStmt;
import com.baidu.palo.analysis.ShowBackupStmt;
import com.baidu.palo.analysis.ShowBrokerStmt;
import com.baidu.palo.analysis.ShowClusterStmt;
import com.baidu.palo.analysis.ShowCollationStmt;
import com.baidu.palo.analysis.ShowColumnStmt;
import com.baidu.palo.analysis.ShowCreateDbStmt;
import com.baidu.palo.analysis.ShowCreateTableStmt;
import com.baidu.palo.analysis.ShowDataStmt;
import com.baidu.palo.analysis.ShowDbStmt;
import com.baidu.palo.analysis.ShowDeleteStmt;
import com.baidu.palo.analysis.ShowEnginesStmt;
import com.baidu.palo.analysis.ShowExportStmt;
import com.baidu.palo.analysis.ShowLoadStmt;
import com.baidu.palo.analysis.ShowLoadWarningsStmt;
import com.baidu.palo.analysis.ShowMigrationsStmt;
import com.baidu.palo.analysis.ShowPartitionsStmt;
import com.baidu.palo.analysis.ShowProcStmt;
import com.baidu.palo.analysis.ShowProcesslistStmt;
import com.baidu.palo.analysis.ShowRestoreStmt;
import com.baidu.palo.analysis.ShowRollupStmt;
import com.baidu.palo.analysis.ShowStmt;
import com.baidu.palo.analysis.ShowTableStatusStmt;
import com.baidu.palo.analysis.ShowTableStmt;
import com.baidu.palo.analysis.ShowTabletStmt;
import com.baidu.palo.analysis.ShowUserPropertyStmt;
import com.baidu.palo.analysis.ShowUserStmt;
import com.baidu.palo.analysis.ShowVariablesStmt;
import com.baidu.palo.analysis.ShowWhiteListStmt;
import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.catalog.TabletInvertedIndex;
import com.baidu.palo.catalog.UserPropertyMgr;
import com.baidu.palo.catalog.View;
import com.baidu.palo.cluster.BaseParam;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.PatternMatcher;
import com.baidu.palo.common.proc.BackendsProcDir;
import com.baidu.palo.common.proc.LoadProcDir;
import com.baidu.palo.common.proc.PartitionsProcDir;
import com.baidu.palo.common.proc.ProcNodeInterface;
import com.baidu.palo.common.proc.TabletsProcDir;
import com.baidu.palo.load.ExportJob;
import com.baidu.palo.load.ExportMgr;
import com.baidu.palo.load.Load;
import com.baidu.palo.load.LoadErrorHub;
import com.baidu.palo.load.LoadErrorHub.HubType;
import com.baidu.palo.load.LoadJob;
import com.baidu.palo.load.LoadJob.JobState;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        } else if (stmt instanceof ShowWhiteListStmt) {
            handleShowWhiteList();
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
        } else if (stmt instanceof ShowUserStmt) {
            handleShowUser();
        } else {
            handleEmtpy();
        }

        return resultSet;
    }

    private void handleShowWhiteList() {
        ShowWhiteListStmt showWhiteStmt = (ShowWhiteListStmt) stmt;
        List<List<String>> rowSet = ctx.getCatalog().showWhiteList(ctx.getUser());
        resultSet = new ShowResultSet(showWhiteStmt.getMetaData(), rowSet);
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

        List<ConnectContext.ThreadInfo> threadInfos = ctx.getConnectScheduler().listConnection(ctx.getUser());
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
            if (ctx.getCatalog().getUserMgr().isSuperuser(ctx.getUser())
                    && !ctx.getCatalog().getUserMgr().isAdmin(ctx.getUser())) {
                // hide ip and host info
                for (List<String> row : finalRows) {
                    row.remove(BackendsProcDir.IP_INDEX);
                    // remove twice cause posistion shift to left after removing
                    row.remove(BackendsProcDir.IP_INDEX);
                }

                // mod meta data
                metaData.removeColumn(BackendsProcDir.IP_INDEX);
                // remove twice cause posistion shift to left after removing
                metaData.removeColumn(BackendsProcDir.IP_INDEX);
            }
        }

        resultSet = new ShowResultSet(metaData, finalRows);
    }

    // Show clusters
    private void handleShowCluster() throws AnalysisException {
        final ShowClusterStmt showStmt = (ShowClusterStmt) stmt;
        final List<List<String>> rows = Lists.newArrayList();
        final List<String> clusterNames = ctx.getCatalog().getClusterNames();

        if (!ctx.getCatalog().getUserMgr().isAdmin(ctx.getUser())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_SHOW_ACCESS_DENIED);
        }

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

        if (!ctx.getCatalog().getUserMgr().isAdmin(ctx.getUser())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_SHOW_ACCESS_DENIED);
        }

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
            matcher = PatternMatcher.createMysqlPattern(showDbStmt.getPattern());
        }
        UserPropertyMgr userPropertyMgr = ctx.getCatalog().getUserMgr();
        Set<String> dbNameSet = Sets.newTreeSet();
        for (String fullName : dbNames) {
            final String db = ClusterNamespace.getNameFromFullName(fullName);
            // Filter dbname
            if (matcher != null && !matcher.match(db)) {
                continue;
            }
            if (userPropertyMgr.checkAccess(ctx.getUser(), fullName, AccessPrivilege.READ_ONLY)) {
                dbNameSet.add(db);
            }
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
                    matcher = PatternMatcher.createMysqlPattern(showTableStmt.getPattern());
                }
                for (Table tbl : db.getTables()) {
                    if (matcher != null && !matcher.match(tbl.getName())) {
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
                    matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern());
                }
                for (Table table : db.getTables()) {
                    if (matcher != null && !matcher.match(table.getName())) {
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
            matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern());
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
            Catalog.getDdlStmt(table, createTableStmt, null, null, false, (short) -1);

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
                        matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern());
                    }
                    List<Column> columns = table.getBaseSchema();
                    for (Column col : columns) {
                        if (matcher != null && !matcher.match(col.getName())) {
                            continue;
                        }
                        String aggType = col.getAggregationType() == null ? "" : col.getAggregationType().toSql();
                        if (showStmt.isVerbose()) {
                            // Field Type Collation Null Key Default Extra
                            // Privileges Comment
                            rows.add(Lists.newArrayList(col.getName(), col.getDataType().toString(), "", "NO", "", "",
                                    aggType, "", col.getComment()));
                        } else {
                            // Field Type Null Key Default Extra
                            rows.add(Lists.newArrayList(col.getName(), col.getDataType().toString(), "NO", "", "",
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
            resultSet = new ShowResultSet(helpStmt.getMetaData(), Lists.<List<String>> newArrayList(
                    Lists.newArrayList(topic.getName(), topic.getDescription(), topic.getExample())));
        } else {
            List<String> categories = module.listCategoryByName(mark);
            if (categories.isEmpty()) {
                // If no category match for this name, return
                resultSet = new ShowResultSet(helpStmt.getKeywordMetaData(), EMPTY_SET);
            } else if (categories.size() > 1) {
                // Send category list
                resultSet = new ShowResultSet(helpStmt.getCategoryMetaData(),
                        Lists.<List<String>> newArrayList(categories));
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
        List<List<Comparable>> loadInfos = load.getLoadJobInfosByDb(dbId, showStmt.getLabelValue(),
                showStmt.isAccurateMatch(), showStmt.getStates(), showStmt.getOrderByPairs());
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

        Catalog catalog = Catalog.getInstance();
        Database db = catalog.getDb(showWarningsStmt.getDbName());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showWarningsStmt.getDbName());
        }

        long dbId = db.getId();
        Load load = catalog.getLoadInstance();
        long jobId = 0;
        String label = null;
        if (showWarningsStmt.isFindByLabel()) {
            label = showWarningsStmt.getLabel();
            jobId = load.getLatestJobIdByLabel(dbId, showWarningsStmt.getLabel());
        } else {
            LOG.info("load_job_id={}", jobId);
            jobId = showWarningsStmt.getJobId();
            LoadJob job = load.getLoadJob(jobId);
            if (job == null) {
                throw new AnalysisException("job is not exist.");
            }
            label = job.getLabel();
            LOG.info("label={}", label);
        }

        LoadErrorHub.Param param = load.getLoadErrorHubInfo();
        if (param == null || param.getType() == HubType.NULL_TYPE) {
            throw new AnalysisException("no load error hub be supplied.");
        }
        LoadErrorHub importer = LoadErrorHub.createHub(param);
        ArrayList<LoadErrorHub.ErrorMsg> errors = importer.fetchLoadError(jobId);
        importer.close();

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
    // Show user property statement
    private void handleShowUserProperty() throws AnalysisException {
        ShowUserPropertyStmt showStmt = (ShowUserPropertyStmt) stmt;
        showStmt.handleShow();
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

    private void handleShowBackup() throws AnalysisException {
        ShowBackupStmt showStmt = (ShowBackupStmt) stmt;
        resultSet = new ShowResultSet(showStmt.getMetaData(), showStmt.getResultRows());
    }

    private void handleShowRestore() throws AnalysisException {
        ShowRestoreStmt showStmt = (ShowRestoreStmt) stmt;
        resultSet = new ShowResultSet(showStmt.getMetaData(), showStmt.getResultRows());
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
        final List<List<String>> backendInfos = BackendsProcDir.getClusterBackendInfos(showStmt.getClusterName());
        resultSet = new ShowResultSet(showStmt.getMetaData(), backendInfos);
    }

    private void handleShowUser() {
        final ShowUserStmt showStmt = (ShowUserStmt) stmt;
        final List<List<String>> userInfos = Catalog.getInstance().getUserMgr()
                .fetchAccessResourceResult(showStmt.getUser());
        resultSet = new ShowResultSet(showStmt.getMetaData(), userInfos);
    }

}

