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
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

/***
 * Used to store statistics job info,
 * including job status, progress, etc.
 */
public class StatisticsJob {
    private static final Logger LOG = LogManager.getLogger(StatisticsJob.class);

    public enum JobState {
        PENDING,
        SCHEDULING,
        RUNNING,
        FINISHED,
        FAILED,
        CANCELLED
    }

    private final long id = Catalog.getCurrentCatalog().getNextId();

    /**
     * to be collected database stats.
     */
    private final long dbId;

    /**
     * to be collected table stats.
     */
    private final Set<Long> tblIds;

    /**
     * to be collected column stats.
     */
    private final Map<Long, List<String>> tableIdToColumnName;

    private final Map<String, String> properties;

    /**
     * to be executed tasks.
     */
    private final List<StatisticsTask> tasks = Lists.newArrayList();

    private JobState jobState = JobState.PENDING;
    private final List<String> errorMsgs = Lists.newArrayList();

    private final long createTime = System.currentTimeMillis();
    private long startTime = -1L;
    private long finishTime = -1L;
    private int progress = 0;

    public StatisticsJob(Long dbId,
                         Set<Long> tblIds,
                         Map<Long, List<String>> tableIdToColumnName,
                         Map<String, String> properties) {
        this.dbId = dbId;
        this.tblIds = tblIds;
        this.tableIdToColumnName = tableIdToColumnName;
        this.properties = properties == null ? Maps.newHashMap() : properties;
    }

    public long getId() {
        return this.id;
    }

    public long getDbId() {
        return this.dbId;
    }

    public Set<Long> getTblIds() {
        return this.tblIds;
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

    public List<String> getErrorMsgs() {
        return this.errorMsgs;
    }

    public JobState getJobState() {
        return this.jobState;
    }

    public long getCreateTime() {
        return this.createTime;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public long getFinishTime() {
        return this.finishTime;
    }

    public int getProgress() {
        return this.progress;
    }

    /**
     * get statisticsJob from analyzeStmt.
     * AnalyzeStmt: analyze t1(c1,c2,c3)
     * tableId: [t1]
     * tableIdToColumnName <t1, [c1,c2,c3]>
     */
    public static StatisticsJob fromAnalyzeStmt(AnalyzeStmt analyzeStmt) throws AnalysisException {
        long dbId = analyzeStmt.getDbId();
        Map<Long, List<String>> tableIdToColumnName = analyzeStmt.getTableIdToColumnName();
        Set<Long> tblIds = analyzeStmt.getTblIds();
        Map<String, String> properties = analyzeStmt.getProperties();
        return new StatisticsJob(dbId, tblIds, tableIdToColumnName, properties);
    }
}
