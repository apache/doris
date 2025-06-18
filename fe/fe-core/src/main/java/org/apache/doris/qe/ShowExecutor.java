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
import org.apache.doris.analysis.DiagnoseTabletStmt;
import org.apache.doris.analysis.HelpStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.ShowAlterStmt;
import org.apache.doris.analysis.ShowAnalyzeStmt;
import org.apache.doris.analysis.ShowAnalyzeTaskStatus;
import org.apache.doris.analysis.ShowAuthorStmt;
import org.apache.doris.analysis.ShowCloudWarmUpStmt;
import org.apache.doris.analysis.ShowColumnStatsStmt;
import org.apache.doris.analysis.ShowConfigStmt;
import org.apache.doris.analysis.ShowCreateLoadStmt;
import org.apache.doris.analysis.ShowCreateMTMVStmt;
import org.apache.doris.analysis.ShowDbIdStmt;
import org.apache.doris.analysis.ShowEnginesStmt;
import org.apache.doris.analysis.ShowIndexStmt;
import org.apache.doris.analysis.ShowLoadWarningsStmt;
import org.apache.doris.analysis.ShowPolicyStmt;
import org.apache.doris.analysis.ShowQueuedAnalyzeJobsStmt;
import org.apache.doris.analysis.ShowReplicaStatusStmt;
import org.apache.doris.analysis.ShowRollupStmt;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.ShowStreamLoadStmt;
import org.apache.doris.analysis.ShowTrashDiskStmt;
import org.apache.doris.analysis.ShowUserPropertyStmt;
import org.apache.doris.analysis.ShowVariablesStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MetadataViewer;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.load.CloudLoadManager;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Pair;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.RollupProcDir;
import org.apache.doris.common.proc.SchemaChangeProcDir;
import org.apache.doris.common.proc.TrashProcNode;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.qe.help.HelpModule;
import org.apache.doris.qe.help.HelpTopic;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.AutoAnalysisPendingJob;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.PartitionColumnStatistic;
import org.apache.doris.statistics.PartitionColumnStatisticCacheKey;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.StatisticsRepository;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Diagnoser;
import org.apache.doris.system.NodeType;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
        checkStmtSupported();
        if (stmt instanceof ShowRollupStmt) {
            handleShowRollup();
        } else if (stmt instanceof ShowAuthorStmt) {
            handleShowAuthor();
        } else if (stmt instanceof HelpStmt) {
            handleHelp();
        } else if (stmt instanceof ShowDbIdStmt) {
            handleShowDbId();
        } else if (stmt instanceof ShowCreateMTMVStmt) {
            handleShowCreateMTMV();
        } else if (stmt instanceof ShowEnginesStmt) {
            handleShowEngines();
        } else if (stmt instanceof ShowVariablesStmt) {
            handleShowVariables();
        } else if (stmt instanceof ShowStreamLoadStmt) {
            handleShowStreamLoad();
        } else if (stmt instanceof ShowLoadWarningsStmt) {
            handleShowLoadWarnings();
        } else if (stmt instanceof ShowAlterStmt) {
            handleShowAlter();
        } else if (stmt instanceof ShowUserPropertyStmt) {
            handleShowUserProperty();
        } else if (stmt instanceof ShowTrashDiskStmt) {
            handleShowTrashDisk();
        } else if (stmt instanceof ShowReplicaStatusStmt) {
            handleAdminShowTabletStatus();
        } else if (stmt instanceof ShowConfigStmt) {
            if (Config.isCloudMode() && !ctx.getCurrentUserIdentity()
                    .getUser().equals(Auth.ROOT_USER)) {
                LOG.info("stmt={}, not supported in cloud mode", stmt.toString());
                throw new AnalysisException("Unsupported operation");
            }
            handleAdminShowConfig();
        } else if (stmt instanceof ShowIndexStmt) {
            handleShowIndex();
        } else if (stmt instanceof ShowColumnStatsStmt) {
            handleShowColumnStats();
        } else if (stmt instanceof DiagnoseTabletStmt) {
            handleAdminDiagnoseTablet();
        } else if (stmt instanceof ShowPolicyStmt) {
            handleShowPolicy();
        } else if (stmt instanceof ShowAnalyzeStmt) {
            handleShowAnalyze();
        } else if (stmt instanceof ShowQueuedAnalyzeJobsStmt) {
            handleShowQueuedAnalyzeJobs();
        } else if (stmt instanceof AdminCopyTabletStmt) {
            handleCopyTablet();
        } else if (stmt instanceof ShowAnalyzeTaskStatus) {
            handleShowAnalyzeTaskStatus();
        } else if (stmt instanceof ShowCloudWarmUpStmt) {
            handleShowCloudWarmUpJob();
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
        rowSet.add(Lists.newArrayList("Olap engine", "YES", "Default storage engine of Doris", "NO", "NO", "NO"));
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

    public boolean isShowTablesCaseSensitive() {
        if (GlobalVariable.lowerCaseTableNames == 0) {
            return CaseSensibility.TABLE.getCaseSensibility();
        }
        return false;
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

    private void handleShowCreateMTMV() throws AnalysisException {
        ShowCreateMTMVStmt showStmt = (ShowCreateMTMVStmt) stmt;
        DatabaseIf db = ctx.getEnv().getCatalogMgr().getCatalogOrAnalysisException(showStmt.getCtl())
                .getDbOrAnalysisException(showStmt.getDb());
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException(showStmt.getTable());
        List<List<String>> rows = Lists.newArrayList();
        String mtmvDdl = Env.getMTMVDdl(mtmv);
        rows.add(Lists.newArrayList(mtmv.getName(), mtmvDdl));
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show index statement.
    private void handleShowIndex() throws AnalysisException {
        ShowIndexStmt showStmt = (ShowIndexStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        DatabaseIf db = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(showStmt.getTableName().getCtl())
                .getDbOrAnalysisException(showStmt.getDbName());
        if (db instanceof Database) {
            TableIf table = db.getTableOrAnalysisException(showStmt.getTableName().getTbl());
            if (table instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) table;
                olapTable.readLock();
                try {
                    List<Index> indexes = olapTable.getIndexes();
                    for (Index index : indexes) {
                        rows.add(Lists.newArrayList(showStmt.getTableName().toString(), "", index.getIndexName(),
                                "", String.join(",", index.getColumns()), "", "", "", "",
                                "", index.getIndexType().name(), index.getComment(), index.getPropertiesString()));
                    }
                } finally {
                    olapTable.readUnlock();
                }
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
        resultSet = handleShowLoadWarningV2(showWarningsStmt, db);
    }

    private ShowResultSet handleShowLoadWarningV2(ShowLoadWarningsStmt showWarningsStmt, Database db)
            throws AnalysisException {
        LoadManager loadManager = Env.getCurrentEnv().getLoadManager();
        if (showWarningsStmt.isFindByLabel()) {
            List<List<Comparable>> loadJobInfosByDb;
            if (!Config.isCloudMode()) {
                loadJobInfosByDb = loadManager.getLoadJobInfosByDb(db.getId(),
                        showWarningsStmt.getLabel(),
                        true, null);
            } else {
                loadJobInfosByDb = ((CloudLoadManager) loadManager)
                        .getLoadJobInfosByDb(db.getId(),
                        showWarningsStmt.getLabel(),
                        true, null, null, null, false, null, false, null, false);
            }
            if (CollectionUtils.isEmpty(loadJobInfosByDb)) {
                throw new AnalysisException("Job does not exist");
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
            throw new AnalysisException("Job does not exist");
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
            throw new AnalysisException(NetUtils.getHostPortInAccessibleFormat(host, port) + " is not a valid backend");
        }
        if (!be.isAlive()) {
            throw new AnalysisException(
                    "Backend " + NetUtils.getHostPortInAccessibleFormat(host, port) + " is not alive");
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

    // Show user property statement
    private void handleShowUserProperty() throws AnalysisException {
        ShowUserPropertyStmt showStmt = (ShowUserPropertyStmt) stmt;
        resultSet = new ShowResultSet(showStmt.getMetaData(), showStmt.getRows());
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

    private void handleShowTrashDisk() {
        ShowTrashDiskStmt showStmt = (ShowTrashDiskStmt) stmt;
        List<List<String>> infos = Lists.newArrayList();
        TrashProcNode.getTrashDiskInfo(showStmt.getBackend(), infos);
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleAdminShowTabletStatus() throws AnalysisException {
        ShowReplicaStatusStmt showStmt = (ShowReplicaStatusStmt) stmt;
        List<List<String>> results;
        try {
            results = MetadataViewer.getTabletStatus(showStmt);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), results);
    }

    private void handleAdminShowConfig() throws AnalysisException {
        ShowConfigStmt showStmt = (ShowConfigStmt) stmt;
        if (showStmt.getType() == NodeType.FRONTEND) {
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
        } else {
            handShowBackendConfig(showStmt);
        }
    }

    private void handShowBackendConfig(ShowConfigStmt stmt) throws AnalysisException {
        List<List<String>> results = new ArrayList<>();
        List<Long> backendIds;
        final SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        if (stmt.isShowSingleBackend()) {
            long backendId = stmt.getBackendId();
            if (systemInfoService.getBackend(backendId) == null) {
                throw new AnalysisException("Backend " + backendId + " not exists");
            }
            Backend backend = systemInfoService.getBackend(backendId);
            if (!backend.isAlive()) {
                throw new AnalysisException("Backend " + backendId + " is not alive");
            }
            backendIds = Lists.newArrayList(backendId);
        } else {
            backendIds = systemInfoService.getAllBackendIds(true);
        }

        PatternMatcher matcher = null;
        if (stmt.getPattern() != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(stmt.getPattern(),
                    CaseSensibility.CONFIG.getCaseSensibility());
        }
        for (long beId : backendIds) {
            Backend backend = systemInfoService.getBackend(beId);
            String host = backend.getHost();
            int httpPort = backend.getHttpPort();
            String urlString = String.format("http://%s:%d/api/show_config", host, httpPort);
            try {
                URL url = new URL(urlString);
                URLConnection urlConnection = url.openConnection();
                InputStream inputStream = urlConnection.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                while (reader.ready()) {
                    // line's format like [["k1","v1"], ["k2","v2"]]
                    String line = reader.readLine();
                    JSONArray outer = new JSONArray(line);
                    for (int i = 0; i < outer.length(); ++i) {
                        // [key, type, value, isMutable]
                        JSONArray inner = outer.getJSONArray(i);
                        if (matcher == null || matcher.match(inner.getString(0))) {
                            List<String> rows = Lists.newArrayList();
                            rows.add(String.valueOf(beId));
                            rows.add(host);
                            rows.add(inner.getString(0));  // key
                            rows.add(inner.getString(2));  // value
                            rows.add(inner.getString(1));  // Type
                            rows.add(inner.getString(3));  // isMutable
                            results.add(rows);
                        }
                    }
                }
            } catch (Exception e) {
                throw new AnalysisException(
                        String.format("Can’t get backend config, backendId: %d, host: %s", beId, host));
            }
        }
        resultSet = new ShowResultSet(stmt.getMetaData(), results);
    }

    private void handleShowCloudWarmUpJob() throws AnalysisException {
        ShowCloudWarmUpStmt showStmt = (ShowCloudWarmUpStmt) stmt;
        if (showStmt.showAllJobs()) {
            int limit = ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr().MAX_SHOW_ENTRIES;
            resultSet = new ShowResultSet(showStmt.getMetaData(),
                            ((CloudEnv) Env.getCurrentEnv()).getCacheHotspotMgr().getAllJobInfos(limit));
        } else {
            resultSet = new ShowResultSet(showStmt.getMetaData(),
                            ((CloudEnv) Env.getCurrentEnv())
                                    .getCacheHotspotMgr()
                                    .getSingleJobInfo(showStmt.getJobId()));
        }
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

    private void handleShowColumnStats() throws AnalysisException {
        ShowColumnStatsStmt showColumnStatsStmt = (ShowColumnStatsStmt) stmt;
        TableIf tableIf = showColumnStatsStmt.getTable();
        List<Pair<Pair<String, String>, ColumnStatistic>> columnStatistics = new ArrayList<>();
        Set<String> columnNames = showColumnStatsStmt.getColumnNames();
        PartitionNames partitionNames = showColumnStatsStmt.getPartitionNames();
        boolean showCache = showColumnStatsStmt.isCached();
        boolean isAllColumns = showColumnStatsStmt.isAllColumns();
        if (partitionNames != null) {
            List<String> partNames = partitionNames.getPartitionNames() == null
                    ? new ArrayList<>(tableIf.getPartitionNames())
                    : partitionNames.getPartitionNames();
            if (showCache) {
                resultSet = showColumnStatsStmt.constructPartitionCachedColumnStats(
                    getCachedPartitionColumnStats(columnNames, partNames, tableIf), tableIf);
            } else {
                List<ResultRow> partitionColumnStats =
                        StatisticsRepository.queryColumnStatisticsByPartitions(tableIf, columnNames, partNames);
                resultSet = showColumnStatsStmt.constructPartitionResultSet(partitionColumnStats, tableIf);
            }
        } else {
            if (isAllColumns && !showCache) {
                getStatsForAllColumns(columnStatistics, tableIf);
            } else {
                getStatsForSpecifiedColumns(columnStatistics, columnNames, tableIf, showCache);
            }
            resultSet = showColumnStatsStmt.constructResultSet(columnStatistics);
        }
    }

    private void getStatsForAllColumns(List<Pair<Pair<String, String>, ColumnStatistic>> columnStatistics,
            TableIf tableIf) {
        List<ResultRow> resultRows = StatisticsRepository.queryColumnStatisticsForTable(
                tableIf.getDatabase().getCatalog().getId(), tableIf.getDatabase().getId(), tableIf.getId());
        // row[4] is index id, row[5] is column name.
        for (ResultRow row : resultRows) {
            String indexName = tableIf.getName();
            long indexId = Long.parseLong(row.get(4));
            if (tableIf instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) tableIf;
                indexName = olapTable.getIndexNameById(indexId == -1 ? olapTable.getBaseIndexId() : indexId);
            }
            if (indexName == null) {
                continue;
            }
            try {
                columnStatistics.add(Pair.of(Pair.of(indexName, row.get(5)), ColumnStatistic.fromResultRow(row)));
            } catch (Exception e) {
                LOG.warn("Failed to deserialize column statistics. reason: [{}]. Row [{}]", e.getMessage(), row);
                if (LOG.isDebugEnabled()) {
                    LOG.debug(e);
                }
            }
        }
    }

    private void getStatsForSpecifiedColumns(List<Pair<Pair<String, String>, ColumnStatistic>> columnStatistics,
            Set<String> columnNames, TableIf tableIf, boolean showCache)
            throws AnalysisException {
        for (String colName : columnNames) {
            // Olap base index use -1 as index id.
            List<Long> indexIds = Lists.newArrayList();
            if (tableIf instanceof OlapTable) {
                indexIds = ((OlapTable) tableIf).getMvColumnIndexIds(colName);
            } else {
                indexIds.add(-1L);
            }
            for (long indexId : indexIds) {
                String indexName = tableIf.getName();
                if (tableIf instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) tableIf;
                    indexName = olapTable.getIndexNameById(indexId == -1 ? olapTable.getBaseIndexId() : indexId);
                }
                if (indexName == null) {
                    continue;
                }
                // Show column statistics in columnStatisticsCache.
                ColumnStatistic columnStatistic;
                if (showCache) {
                    columnStatistic = Env.getCurrentEnv().getStatisticsCache().getColumnStatistics(
                        tableIf.getDatabase().getCatalog().getId(),
                        tableIf.getDatabase().getId(), tableIf.getId(), indexId, colName);
                } else {
                    columnStatistic = StatisticsRepository.queryColumnStatisticsByName(
                        tableIf.getDatabase().getCatalog().getId(),
                        tableIf.getDatabase().getId(), tableIf.getId(), indexId, colName);
                }
                columnStatistics.add(Pair.of(Pair.of(indexName, colName), columnStatistic));
            }
        }
    }

    private Map<PartitionColumnStatisticCacheKey, PartitionColumnStatistic> getCachedPartitionColumnStats(
            Set<String> columnNames, List<String> partitionNames, TableIf tableIf) {
        Map<PartitionColumnStatisticCacheKey, PartitionColumnStatistic> ret = new HashMap<>();
        long catalogId = tableIf.getDatabase().getCatalog().getId();
        long dbId = tableIf.getDatabase().getId();
        long tableId = tableIf.getId();
        for (String colName : columnNames) {
            // Olap base index use -1 as index id.
            List<Long> indexIds = Lists.newArrayList();
            if (tableIf instanceof OlapTable) {
                indexIds = ((OlapTable) tableIf).getMvColumnIndexIds(colName);
            } else {
                indexIds.add(-1L);
            }
            for (long indexId : indexIds) {
                String indexName = tableIf.getName();
                if (tableIf instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) tableIf;
                    indexName = olapTable.getIndexNameById(indexId == -1 ? olapTable.getBaseIndexId() : indexId);
                }
                if (indexName == null) {
                    continue;
                }
                for (String partName : partitionNames) {
                    PartitionColumnStatistic partitionStatistics = Env.getCurrentEnv().getStatisticsCache()
                            .getPartitionColumnStatistics(catalogId, dbId, tableId, indexId, partName, colName);
                    ret.put(new PartitionColumnStatisticCacheKey(catalogId, dbId, tableId, indexId, partName, colName),
                            partitionStatistics);
                }
            }
        }
        return ret;
    }

    private void handleAdminDiagnoseTablet() {
        DiagnoseTabletStmt showStmt = (DiagnoseTabletStmt) stmt;
        List<List<String>> resultRowSet = Diagnoser.diagnoseTablet(showStmt.getTabletId());
        ShowResultSetMetaData showMetaData = showStmt.getMetaData();
        resultSet = new ShowResultSet(showMetaData, resultRowSet);
    }

    public void handleShowPolicy() throws AnalysisException {
        ShowPolicyStmt showStmt = (ShowPolicyStmt) stmt;
        resultSet = Env.getCurrentEnv().getPolicyMgr().showPolicy(showStmt);
    }

    private void handleShowAnalyze() {
        ShowAnalyzeStmt showStmt = (ShowAnalyzeStmt) stmt;
        List<AnalysisInfo> results = Env.getCurrentEnv().getAnalysisManager().findAnalysisJobs(showStmt);
        List<List<String>> resultRows = Lists.newArrayList();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        for (AnalysisInfo analysisInfo : results) {
            try {
                List<String> row = new ArrayList<>();
                row.add(String.valueOf(analysisInfo.jobId));
                CatalogIf<? extends DatabaseIf<? extends TableIf>> c
                        = StatisticsUtil.findCatalog(analysisInfo.catalogId);
                row.add(c.getName());
                Optional<? extends DatabaseIf<? extends TableIf>> databaseIf = c.getDb(analysisInfo.dbId);
                row.add(databaseIf.isPresent() ? databaseIf.get().getFullName() : "DB may get deleted");
                if (databaseIf.isPresent()) {
                    Optional<? extends TableIf> table = databaseIf.get().getTable(analysisInfo.tblId);
                    row.add(table.isPresent() ? Util.getTempTableDisplayName(table.get().getName())
                            : "Table may get deleted");
                } else {
                    row.add("DB may get deleted");
                }
                StringBuffer sb = new StringBuffer();
                String colNames = analysisInfo.colName;
                if (colNames != null) {
                    for (String columnName : colNames.split(",")) {
                        String[] kv = columnName.split(":");
                        sb.append(Util.getTempTableDisplayName(kv[0]))
                            .append(":").append(kv[1]).append(",");
                    }
                }
                String newColNames = sb.toString();
                newColNames = StringUtils.isEmpty(newColNames) ? ""
                        : newColNames.substring(0, newColNames.length() - 1);
                row.add(newColNames);
                row.add(analysisInfo.jobType.toString());
                row.add(analysisInfo.analysisType.toString());
                row.add(analysisInfo.message);
                row.add(TimeUtils.getDatetimeFormatWithTimeZone().format(
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(analysisInfo.lastExecTimeInMs),
                                ZoneId.systemDefault())));
                row.add(analysisInfo.state.toString());
                row.add(Env.getCurrentEnv().getAnalysisManager().getJobProgress(analysisInfo.jobId));
                row.add(analysisInfo.scheduleType.toString());
                LocalDateTime startTime =
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(analysisInfo.startTime),
                                java.time.ZoneId.systemDefault());
                LocalDateTime endTime =
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(analysisInfo.endTime),
                                java.time.ZoneId.systemDefault());
                row.add(startTime.format(formatter));
                row.add(endTime.format(formatter));
                row.add(analysisInfo.priority.name());
                row.add(String.valueOf(analysisInfo.enablePartition));
                resultRows.add(row);
            } catch (Exception e) {
                LOG.warn("Failed to get analyze info for table {}.{}.{}, reason: {}",
                        analysisInfo.catalogId, analysisInfo.dbId, analysisInfo.tblId, e.getMessage());
                continue;
            }
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), resultRows);
    }

    private void handleShowQueuedAnalyzeJobs() {
        ShowQueuedAnalyzeJobsStmt showStmt = (ShowQueuedAnalyzeJobsStmt) stmt;
        List<AutoAnalysisPendingJob> jobs = Env.getCurrentEnv().getAnalysisManager().showAutoPendingJobs(
                showStmt.getTableName(), showStmt.getPriority());
        List<List<String>> resultRows = Lists.newArrayList();
        for (AutoAnalysisPendingJob job : jobs) {
            try {
                List<String> row = new ArrayList<>();
                CatalogIf<? extends DatabaseIf<? extends TableIf>> c = StatisticsUtil.findCatalog(job.catalogName);
                row.add(c.getName());
                Optional<? extends DatabaseIf<? extends TableIf>> databaseIf = c.getDb(job.dbName);
                row.add(databaseIf.isPresent() ? databaseIf.get().getFullName() : "DB may get deleted");
                if (databaseIf.isPresent()) {
                    Optional<? extends TableIf> table = databaseIf.get().getTable(job.tableName);
                    row.add(table.isPresent() ? table.get().getName() : "Table may get deleted");
                } else {
                    row.add("DB may get deleted");
                }
                row.add(job.getColumnNames());
                row.add(String.valueOf(job.priority));
                resultRows.add(row);
            } catch (Exception e) {
                LOG.warn("Failed to get pending jobs for table {}.{}.{}, reason: {}",
                        job.catalogName, job.dbName, job.tableName, e.getMessage());
                continue;
            }
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
        backendId = replica.getBackendIdWithoutException();
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

    private void handleShowAnalyzeTaskStatus() {
        ShowAnalyzeTaskStatus showStmt = (ShowAnalyzeTaskStatus) stmt;
        AnalysisInfo jobInfo = Env.getCurrentEnv().getAnalysisManager().findJobInfo(showStmt.getJobId());
        TableIf table = StatisticsUtil.findTable(jobInfo.catalogId, jobInfo.dbId, jobInfo.tblId);
        List<AnalysisInfo> analysisInfos = Env.getCurrentEnv().getAnalysisManager().findTasks(showStmt.getJobId());
        List<List<String>> rows = new ArrayList<>();
        for (AnalysisInfo analysisInfo : analysisInfos) {
            List<String> row = new ArrayList<>();
            row.add(String.valueOf(analysisInfo.taskId));
            row.add(analysisInfo.colName);
            if (table instanceof OlapTable && analysisInfo.indexId != -1) {
                row.add(((OlapTable) table).getIndexNameById(analysisInfo.indexId));
            } else {
                row.add(table.getName());
            }
            row.add(analysisInfo.message);
            row.add(TimeUtils.getDatetimeFormatWithTimeZone().format(
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(analysisInfo.lastExecTimeInMs),
                            ZoneId.systemDefault())));
            row.add(String.valueOf(analysisInfo.timeCostInMs));
            row.add(analysisInfo.state.toString());
            rows.add(row);
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void checkStmtSupported() throws AnalysisException {
        // check stmt has been supported in cloud mode
        if (Config.isNotCloudMode()) {
            return;
        }

        if (stmt instanceof ShowReplicaStatusStmt
                || stmt instanceof ShowConfigStmt) {
            if (!ctx.getCurrentUserIdentity().getUser().equals(Auth.ROOT_USER)) {
                LOG.info("stmt={}, not supported in cloud mode", stmt.toString());
                throw new AnalysisException("Unsupported operation");
            }
        }

        if (stmt instanceof DiagnoseTabletStmt
                || stmt instanceof AdminCopyTabletStmt) {
            LOG.info("stmt={}, not supported in cloud mode", stmt.toString());
            throw new AnalysisException("Unsupported operation");
        }
    }
}
