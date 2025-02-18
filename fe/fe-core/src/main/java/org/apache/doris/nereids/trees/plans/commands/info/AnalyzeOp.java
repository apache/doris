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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.AnalyzeProperties;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.AnalysisInfo;

import org.apache.logging.log4j.core.util.CronExpression;

import java.util.Map;

/**
 * AnalyzeOp
 */
public abstract class AnalyzeOp {
    protected AnalyzeProperties analyzeProperties;

    protected PlanType planType;

    public AnalyzeOp(AnalyzeProperties analyzeProperties, PlanType planType) {
        this.analyzeProperties = analyzeProperties;
        this.planType = planType;
    }

    public PlanType getPlanType() {
        return planType;
    }

    /**
     * getProperties
     */
    public Map<String, String> getProperties() {
        return analyzeProperties.getProperties();
    }

    public AnalysisInfo.AnalysisType getAnalysisType() {
        return analyzeProperties.getAnalysisType();
    }

    public AnalysisInfo.AnalysisMethod getAnalysisMethod() {
        double samplePercent = analyzeProperties.getSamplePercent();
        int sampleRows = analyzeProperties.getSampleRows();
        return (samplePercent > 0 || sampleRows > 0)
                ? AnalysisInfo.AnalysisMethod.SAMPLE
                : AnalysisInfo.AnalysisMethod.FULL;
    }

    public AnalysisInfo.ScheduleType getScheduleType() {
        if (analyzeProperties.isAutomatic()) {
            return AnalysisInfo.ScheduleType.AUTOMATIC;
        }
        return analyzeProperties.getPeriodTimeInMs() > 0 || analyzeProperties.getCron() != null
            ? AnalysisInfo.ScheduleType.PERIOD : AnalysisInfo.ScheduleType.ONCE;
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

    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    public CronExpression getCron() {
        return analyzeProperties.getCron();
    }

    public boolean forceFull() {
        return analyzeProperties.forceFull();
    }

    public boolean usingSqlForExternalTable() {
        return analyzeProperties.usingSqlForExternalTable();
    }

    public void validate(ConnectContext ctx) throws UserException {
        if (analyzeProperties != null) {
            analyzeProperties.check();
        }
    }

    public String toSql() {
        return "";
    }
}
