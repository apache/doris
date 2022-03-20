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

package org.apache.doris.statistics;

import org.apache.doris.analysis.AnalyzeStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.UserException;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.glassfish.jersey.internal.guava.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/*
Used to store statistics job info,
including job status, progress, etc.
 */
public class StatisticsJob {
    private static final Logger LOG = LogManager.getLogger(StatisticsJob.class);

    public enum JobState {
        PENDING,
        SCHEDULING,
        RUNNING,
        FINISHED,
        CANCELLED,
        FAILED
    }

    private final long id = Catalog.getCurrentCatalog().getNextId();

    /**
     * to be collected database stats.
     */
    private final long dbId;

    /**
     * to be collected table stats.
     */
    private final List<Long> tableIds;

    /**
     * to be collected column stats.
     */
    private final Map<Long, List<String>> tableIdToColumnName;

    private final Map<String, String> properties;

    /**
     * to be executed tasks.
     */
    private List<StatisticsTask> tasks = Lists.newArrayList();

    private JobState jobState = JobState.PENDING;

    private final long createTime = System.currentTimeMillis();
    private long scheduleTime = -1L;
    private long finishTime = -1L;
    private int progress = 0;

    public StatisticsJob(Long dbId,
                         List<Long> tableIdList,
                         Map<Long, List<String>> tableIdToColumnName,
                         Map<String, String> properties) {
        this.dbId = dbId;
        this.tableIds = tableIdList;
        this.tableIdToColumnName = tableIdToColumnName;
        this.properties = properties;
    }

    public long getId() {
        return this.id;
    }

    public long getDbId() {
        return this.dbId;
    }

    public List<Long> getTableIds() {
        return this.tableIds;
    }

    public Map<Long, List<String>> getTableIdToColumnName() {
        return this.tableIdToColumnName;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public List<StatisticsTask> getTasks() {
        return this.tasks;
    }

    public void setTasks(List<StatisticsTask> tasks) {
        this.tasks = tasks;
    }

    public JobState getJobState() {
        return this.jobState;
    }

    public void setJobState(JobState jobState) {
        this.jobState = jobState;
    }

    public long getCreateTime() {
        return this.createTime;
    }

    public long getScheduleTime() {
        return this.scheduleTime;
    }

    public void setScheduleTime(long scheduleTime) {
        this.scheduleTime = scheduleTime;
    }

    public long getFinishTime() {
        return this.finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public int getProgress() {
        return this.progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    /**
     * get statisticsJob from analyzeStmt.
     * AnalyzeStmt: analyze t1(c1,c2,c3)
     * tableId: [t1]
     * tableIdToColumnName <t1, [c1,c2,c3]>
     */
    public static StatisticsJob fromAnalyzeStmt(AnalyzeStmt analyzeStmt) throws UserException {
        List<Long> tableIdList = Lists.newArrayList();
        Map<Long, List<String>> tableIdToColumnName = Maps.newHashMap();
        List<String> columnNames = analyzeStmt.getColumnNames();

        String dbName = analyzeStmt.getDbName();
        String tblName = analyzeStmt.getTblName();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbName);

        if (Strings.isNullOrEmpty(tblName)) {
            List<Table> tables = db.getTables();
            for (Table table : tables) {
                long tableId = table.getId();
                tableIdList.add(tableId);
                List<String> colNames = Lists.newArrayList();
                List<Column> baseSchema = table.getBaseSchema();
                baseSchema.stream().map(Column::getName).forEach(colNames::add);
                tableIdToColumnName.put(tableId, colNames);
            }
        } else {
            Table table = db.getOlapTableOrDdlException(tblName);
            tableIdList.add(table.getId());
            tableIdToColumnName.put(table.getId(), columnNames);
        }

        return new StatisticsJob(
                db.getId(),
                tableIdList,
                tableIdToColumnName,
                analyzeStmt.getProperties());
    }

    public Set<Long> relatedTableId() {
        Set<Long> relatedTableId = Sets.newHashSet();
        relatedTableId.addAll(this.tableIds);
        relatedTableId.addAll(this.tableIdToColumnName.keySet());
        return relatedTableId;
    }
}
