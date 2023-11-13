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

package org.apache.doris.analysis;


import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMode;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;

import org.apache.logging.log4j.core.util.CronExpression;

import java.util.Map;

public class AnalyzeStmt extends StatementBase {

    protected AnalyzeProperties analyzeProperties;

    public AnalyzeStmt(AnalyzeProperties analyzeProperties) {
        this.analyzeProperties = analyzeProperties;
    }

    public Map<String, String> getProperties() {
        return analyzeProperties.getProperties();
    }

    public AnalysisMode getAnalysisMode() {
        return analyzeProperties.isIncremental() ? AnalysisMode.INCREMENTAL : AnalysisMode.FULL;
    }

    public AnalysisType getAnalysisType() {
        return analyzeProperties.getAnalysisType();
    }

    public AnalysisMethod getAnalysisMethod() {
        double samplePercent = analyzeProperties.getSamplePercent();
        int sampleRows = analyzeProperties.getSampleRows();
        return (samplePercent > 0 || sampleRows > 0) ? AnalysisMethod.SAMPLE : AnalysisMethod.FULL;
    }

    public ScheduleType getScheduleType() {
        if (analyzeProperties.isAutomatic()) {
            return ScheduleType.AUTOMATIC;
        }
        return analyzeProperties.getPeriodTimeInMs() > 0 || analyzeProperties.getCron() != null
                ? ScheduleType.PERIOD : ScheduleType.ONCE;
    }

    public boolean isSync() {
        return analyzeProperties.isSync();
    }

    public int getSamplePercent() {
        return analyzeProperties.getSamplePercent();
    }

    public int getSampleRows() {
        return analyzeProperties.getSampleRows();
    }

    public int getNumBuckets() {
        return analyzeProperties.getNumBuckets();
    }

    public long getPeriodTimeInMs() {
        return analyzeProperties.getPeriodTimeInMs();
    }

    public AnalyzeProperties getAnalyzeProperties() {
        return analyzeProperties;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    public CronExpression getCron() {
        return analyzeProperties.getCron();
    }

    public boolean forceFull() {
        return analyzeProperties.forceFull();
    }

    public boolean usingSqlForPartitionColumn() {
        return analyzeProperties.usingSqlForPartitionColumn();
    }
}
