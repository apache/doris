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

package org.apache.doris.common.profile;

import org.apache.doris.common.util.RuntimeProfile;

import com.google.common.collect.ImmutableList;

import java.util.Map;

public class SummaryProfile {
    // Summary
    public static final String START_TIME = "Start Time";
    public static final String END_TIME = "End Time";
    public static final String TOTAL_TIME = "Total";
    public static final String QUERY_STATE = "Query State";
    public static final String JOB_ID = "Job ID";
    public static final String QUERY_ID = "Query ID";
    public static final String QUERY_TYPE = "Query Type";
    public static final String DORIS_VERSION = "Doris Version";
    public static final String USER = "User";
    public static final String DEFAULT_DB = "Default Db";
    public static final String SQL_STATEMENT = "Sql Statement";
    public static final String IS_CACHED = "Is Cached";
    public static final String TOTAL_INSTANCES_NUM = "Total Instances Num";
    public static final String INSTANCES_NUM_PER_BE = "Instances Num Per BE";
    public static final String PARALLEL_FRAGMENT_EXEC_INSTANCE = "Parallel Fragment Exec Instance Num";
    public static final String TRACE_ID = "Trace ID";

    // Execution  Summary
    public static final String ANALYSIS_TIME = "Analysis Time";
    public static final String PLAN_TIME = "Plan Time";
    public static final String SCHEDULE_TIME = "Schedule Time";
    public static final String FETCH_RESULT_TIME = "Fetch Result Time";
    public static final String WRITE_RESULT_TIME = "Write Result Time";
    public static final String WAIT_FETCH_RESULT_TIME = "Wait and Fetch Result Time";

    private static final ImmutableList<String> SUMMARY_KEYS = ImmutableList.of(
            START_TIME, END_TIME, TOTAL_TIME, QUERY_STATE, JOB_ID, QUERY_ID, QUERY_TYPE,
            DORIS_VERSION, USER, DEFAULT_DB, SQL_STATEMENT, IS_CACHED, TOTAL_INSTANCES_NUM,
            INSTANCES_NUM_PER_BE, PARALLEL_FRAGMENT_EXEC_INSTANCE, TRACE_ID);

    private static final ImmutableList<String> EXECUTION_SUMMARY_KEYS = ImmutableList.of(
            ANALYSIS_TIME, PLAN_TIME, SCHEDULE_TIME, FETCH_RESULT_TIME, WRITE_RESULT_TIME,
            WAIT_FETCH_RESULT_TIME);


    private RuntimeProfile summaryProfile;
    private RuntimeProfile executionSummaryProfile;

    public SummaryProfile(RuntimeProfile rootProfile) {
        summaryProfile = new RuntimeProfile("Summary");
        executionSummaryProfile = new RuntimeProfile("Execution Summary");
        init();
        rootProfile.addChild(summaryProfile);
        rootProfile.addChild(executionSummaryProfile);
    }

    private void init() {
        for (String key : SUMMARY_KEYS) {
            summaryProfile.addInfoString(key, "N/A");
        }
        for (String key : EXECUTION_SUMMARY_KEYS) {
            executionSummaryProfile.addInfoString(key, "N/A");
        }
    }

    public long getStartTime() {
        return Long.parseLong(summaryProfile.getInfoString(START_TIME));
    }

    public void update(Map<String, String> summaryInfo, Map<String, String> executionSummaryInfo) {
        updateSummaryProfile(summaryInfo);
        updateExecutionSummaryProfile(executionSummaryInfo);
    }

    private void updateSummaryProfile(Map<String, String> infos) {
        for (String key : infos.keySet()) {
            if (SUMMARY_KEYS.contains(key)) {
                summaryProfile.addInfoString(key, infos.get(key));
            }
        }
    }

    private void updateExecutionSummaryProfile(Map<String, String> infos) {
        for (String key : infos.keySet()) {
            if (EXECUTION_SUMMARY_KEYS.contains(key)) {
                executionSummaryProfile.addInfoString(key, infos.get(key));
            }
        }
    }
}
