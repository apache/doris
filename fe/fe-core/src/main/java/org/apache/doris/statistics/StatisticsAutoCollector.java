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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StatisticsAutoCollector extends StatisticsCollector {

    private static final Logger LOG = LogManager.getLogger(StatisticsAutoCollector.class);

    public StatisticsAutoCollector() {
        super("Automatic Analyzer",
                TimeUnit.MINUTES.toMillis(Config.auto_check_statistics_in_minutes),
                new AnalysisTaskExecutor(Config.auto_analyze_simultaneously_running_task_num,
                        StatisticConstants.TASK_QUEUE_CAP));
    }

    @Override
    protected void collect() {
        while (canCollect()) {
            Map.Entry<TableIf, Set<String>> job = getJob();
            if (job == null) {
                // No more job to process, break and sleep.
                break;
            }
            try {
                TableIf table = job.getKey();
                Set<String> columns = job.getValue()
                        .stream()
                        .filter(c -> needAnalyzeColumn(table, c))
                        .collect(Collectors.toSet());
                processOneJob(table, columns);
            } catch (Exception e) {
                LOG.warn("Failed to analyze table {} with columns [{}]",
                        job.getKey().getName(), job.getValue().stream().collect(Collectors.joining(",")), e);
            }
        }
    }

    protected boolean canCollect() {
        return StatisticsUtil.enableAutoAnalyze()
            && StatisticsUtil.inAnalyzeTime(LocalTime.now(TimeUtils.getTimeZone().toZoneId()));
    }

    protected Map.Entry<TableIf, Set<String>> getJob() {
        AnalysisManager manager = Env.getServingEnv().getAnalysisManager();
        Optional<Map.Entry<TableIf, Set<String>>> job = fetchJobFromMap(manager.highPriorityJobs);
        if (job.isPresent()) {
            return job.get();
        }
        job = fetchJobFromMap(manager.midPriorityJobs);
        if (job.isPresent()) {
            return job.get();
        }
        job = fetchJobFromMap(manager.lowPriorityJobs);
        return job.isPresent() ? job.get() : null;
    }

    protected Optional<Map.Entry<TableIf, Set<String>>> fetchJobFromMap(Map<TableIf, Set<String>> jobMap) {
        synchronized (jobMap) {
            Optional<Map.Entry<TableIf, Set<String>>> first = jobMap.entrySet().stream().findFirst();
            first.ifPresent(entry -> jobMap.remove(entry.getKey()));
            return first;
        }
    }

    protected void processOneJob(TableIf table, Set<String> columns) throws DdlException {
        Set<String> collect = columns.stream().filter(c -> needAnalyzeColumn(table, c)).collect(Collectors.toSet());
        if (collect.isEmpty()) {
            return;
        }
        AnalysisInfo analyzeJob = createAnalyzeJobForTbl(table, columns);
        createSystemAnalysisJob(analyzeJob);
    }

    protected boolean needAnalyzeColumn(TableIf table, String column) {
        //TODO: Calculate column health value.
        return true;
    }

    protected AnalysisInfo createAnalyzeJobForTbl(TableIf table, Set<String> columns) {
        AnalysisMethod analysisMethod = table.getDataSize(true) >= StatisticsUtil.getHugeTableLowerBoundSizeInBytes()
                ? AnalysisMethod.SAMPLE : AnalysisMethod.FULL;
        return new AnalysisInfoBuilder()
                .setJobId(Env.getCurrentEnv().getNextId())
                .setCatalogId(table.getDatabase().getCatalog().getId())
                .setDBId(table.getDatabase().getId())
                .setTblId(table.getId())
                .setColName(columns.stream().collect(Collectors.joining(",")))
                .setAnalysisType(AnalysisInfo.AnalysisType.FUNDAMENTALS)
                .setAnalysisMode(AnalysisInfo.AnalysisMode.INCREMENTAL)
                .setAnalysisMethod(analysisMethod)
                .setSampleRows(analysisMethod.equals(AnalysisMethod.SAMPLE)
                    ? StatisticsUtil.getHugeTableSampleRows() : -1)
                .setScheduleType(ScheduleType.AUTOMATIC)
                .setState(AnalysisState.PENDING)
                .setTaskIds(new ArrayList<>())
                .setLastExecTimeInMs(System.currentTimeMillis())
                .setJobType(JobType.SYSTEM)
                .setTblUpdateTime(table.getUpdateTime())
                .setEmptyJob(table instanceof OlapTable && table.getRowCount() == 0)
                .build();
    }
}
