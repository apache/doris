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

import org.apache.doris.analysis.DiagnoseTabletStmt;
import org.apache.doris.analysis.HelpStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.ShowAlterStmt;
import org.apache.doris.analysis.ShowAnalyzeStmt;
import org.apache.doris.analysis.ShowAnalyzeTaskStatus;
import org.apache.doris.analysis.ShowColumnStatsStmt;
import org.apache.doris.analysis.ShowCreateMTMVStmt;
import org.apache.doris.analysis.ShowEnginesStmt;
import org.apache.doris.analysis.ShowIndexPolicyStmt;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.RollupProcDir;
import org.apache.doris.common.proc.SchemaChangeProcDir;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.qe.help.HelpModule;
import org.apache.doris.qe.help.HelpTopic;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.PartitionColumnStatistic;
import org.apache.doris.statistics.PartitionColumnStatisticCacheKey;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.StatisticsRepository;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.system.Diagnoser;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
        checkStmtSupported();
        if (stmt instanceof HelpStmt) {
            handleHelp();
        } else if (stmt instanceof ShowCreateMTMVStmt) {
            handleShowCreateMTMV();
        } else if (stmt instanceof ShowEnginesStmt) {
            handleShowEngines();
        } else if (stmt instanceof ShowAlterStmt) {
            handleShowAlter();
        } else if (stmt instanceof ShowColumnStatsStmt) {
            handleShowColumnStats();
        } else if (stmt instanceof DiagnoseTabletStmt) {
            handleAdminDiagnoseTablet();
        } else if (stmt instanceof ShowIndexPolicyStmt) {
            handleShowIndexPolicy();
        } else if (stmt instanceof ShowAnalyzeStmt) {
            handleShowAnalyze();
        } else if (stmt instanceof ShowAnalyzeTaskStatus) {
            handleShowAnalyzeTaskStatus();
        } else {
            handleEmtpy();
        }

        return resultSet;
    }

    // Handle show authors
    private void handleEmtpy() {
        // Only success
        resultSet = new ShowResultSet(stmt.getMetaData(), EMPTY_SET);
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

    public boolean isShowTablesCaseSensitive() {
        if (GlobalVariable.lowerCaseTableNames == 0) {
            return CaseSensibility.TABLE.getCaseSensibility();
        }
        return false;
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
        ConnectContext connectContext = ConnectContext.get();
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
                        tableIf.getDatabase().getId(), tableIf.getId(), indexId, colName, connectContext);
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
        ConnectContext ctx = ConnectContext.get();
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
                            .getPartitionColumnStatistics(catalogId, dbId, tableId, indexId, partName, colName, ctx);
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


    public void handleShowIndexPolicy() throws AnalysisException {
        ShowIndexPolicyStmt showStmt = (ShowIndexPolicyStmt) stmt;
        resultSet = Env.getCurrentEnv().getIndexPolicyMgr().showIndexPolicy(showStmt);
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

        if (stmt instanceof DiagnoseTabletStmt) {
            LOG.info("stmt={}, not supported in cloud mode", stmt.toString());
            throw new AnalysisException("Unsupported operation");
        }
    }
}
