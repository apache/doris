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
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.internal.guava.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to store statistics job info,
 * including job status, progress, etc.
 */
public class StatisticsJob {

    public enum JobState {
        PENDING,
        SCHEDULING,
        RUNNING,
        FINISHED,
        CANCELLED,
        FAILED
    }

    private long id;

    /**
     * to be collected database stats.
     */
    private long dbId;

    /**
     * to be collected table stats.
     */
    private List<Long> tableIdList;

    /**
     * to be collected column stats.
     */
    private Map<Long, List<String>> tableIdToColumnName;

    /**
     * to be executed tasks.
     */
    private List<StatisticsTask> taskList;

    private int progress = 0;
    private JobState jobState = JobState.PENDING;
    ;

    public StatisticsJob() {
        this.id = Catalog.getCurrentCatalog().getNextId();
    }

    public StatisticsJob(Long dbId, List<Long> tableIdList, Map<Long, List<String>> tableIdToColumnName) {
        this.dbId = dbId;
        this.tableIdList = tableIdList;
        this.tableIdToColumnName = tableIdToColumnName;
        this.id = Catalog.getCurrentCatalog().getNextId();
        this.taskList = Lists.newArrayList();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public List<Long> getTableIdList() {
        return tableIdList;
    }

    public void setTableIdList(List<Long> tableIdList) {
        this.tableIdList = tableIdList;
    }

    public Map<Long, List<String>> getTableIdToColumnName() {
        return tableIdToColumnName;
    }

    public void setTableIdToColumnName(Map<Long, List<String>> tableIdToColumnName) {
        this.tableIdToColumnName = tableIdToColumnName;
    }

    public List<StatisticsTask> getTaskList() {
        return taskList;
    }

    public void setTaskList(List<StatisticsTask> taskList) {
        this.taskList = taskList;
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public JobState getJobState() {
        return jobState;
    }

    public void setJobState(JobState jobState) {
        this.jobState = jobState;
    }

    /**
     * get statisticsJob from analyzeStmt.
     * AnalyzeStmt: analyze t1(c1,c2,c3)
     * tableId: [t1]
     * tableIdToColumnName <t1, [c1,c2,c3]>
     */
    public static StatisticsJob fromAnalyzeStmt(AnalyzeStmt analyzeStmt) throws DdlException {

        long dbId;
        final List<Long> tableIdList = Lists.newArrayList();
        final Map<Long, List<String>> tableIdToColumnName = Maps.newHashMap();
        final TableName dbTableName = analyzeStmt.getTableName();

        // analyze table
        if (dbTableName != null) {
            // get dbName
            String dbName = dbTableName.getDb();
            if (StringUtils.isNotBlank(dbName)) {
                dbName = analyzeStmt.getClusterName() + ":" + dbName;
            } else {
                dbName = analyzeStmt.getAnalyzer().getDefaultDb();
            }

            // check db
            Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbName);
            if (db == null) {
                throw new DdlException("The database(" + dbName + ") does not exist.");
            }

            // check table
            Table table = db.getOlapTableOrDdlException(dbTableName.getTbl());
            if (table == null) {
                throw new DdlException("The table(" + dbTableName.getTbl() + ") does not exist.");
            }

            // check column
            List<String> columnNames = analyzeStmt.getColumnNames();
            if (columnNames != null) {
                for (String columnName : columnNames) {
                    Column column = table.getColumn(columnName);
                    if (column == null) {
                        throw new DdlException("The column(" + columnName + ") does not exist.");
                    }
                }
            }

            // if columnNames isEmpty then analyze all columns
            if (columnNames == null || columnNames.isEmpty()) {
                List<Column> baseSchema = table.getBaseSchema();
                columnNames = baseSchema.stream().map(Column::getName).collect(Collectors.toList());
            }

            dbId = db.getId();
            tableIdList.add(table.getId());
            tableIdToColumnName.put(table.getId(), columnNames);
        } else {
            // analyze all tables under the current db
            String dbName = analyzeStmt.getAnalyzer().getDefaultDb();
            Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbName);
            dbId = db.getId();

            List<Table> tables = db.getTables();
            for (Table table : tables) {
                long tableId = table.getId();
                tableIdList.add(tableId);
                List<Column> baseSchema = table.getBaseSchema();
                List<String> columnNames = baseSchema.stream().map(Column::getName).collect(Collectors.toList());
                tableIdToColumnName.put(tableId, columnNames);
            }
        }

        return new StatisticsJob(dbId, tableIdList, tableIdToColumnName);
    }

    public Set<Long> relatedTableId() {
        Set<Long> relatedTableId = Sets.newHashSet();
        relatedTableId.addAll(tableIdList);
        relatedTableId.addAll(tableIdToColumnName.keySet());
        return relatedTableId;
    }
}
