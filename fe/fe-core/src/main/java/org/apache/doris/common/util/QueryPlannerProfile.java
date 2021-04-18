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

package org.apache.doris.common.util;

import org.apache.doris.thrift.TUnit;

/**
 * This profile is mainly used to record the time-consuming situation related to
 * executing SQL parsing, planning, scheduling, and fetching results on the FE side.
 * Can be expanded later.
 *
 * All timestamp is in nona second
 */
public class QueryPlannerProfile {
    public static final String KEY_ANALYSIS = "Analysis Time";
    public static final String KEY_PLAN = "Plan Time";
    public static final String KEY_SCHEDULE = "Schedule Time";
    public static final String KEY_FETCH = "Wait and Fetch Result Time";

    // timestamp of query begin
    private long queryBeginTime = -1;
    // Analysis end time
    private long queryAnalysisFinishTime = -1;
    // Plan end time
    private long queryPlanFinishTime = -1;
    // Fragment schedule and send end time
    private long queryScheduleFinishTime = -1;
    // Query result fetch end time
    private long queryFetchResultFinishTime = -1;

    public void setQueryBeginTime() {
        this.queryBeginTime = TimeUtils.getStartTime();
    }

    public void setQueryAnalysisFinishTime() {
        this.queryAnalysisFinishTime = TimeUtils.getStartTime();
    }

    public void setQueryPlanFinishTime() {
        this.queryPlanFinishTime = TimeUtils.getStartTime();
    }

    public void setQueryScheduleFinishTime() {
        this.queryScheduleFinishTime = TimeUtils.getStartTime();
    }

    public void setQueryFetchResultFinishTime() {
        this.queryFetchResultFinishTime = TimeUtils.getStartTime();
    }

    public long getQueryBeginTime() {
        return queryBeginTime;
    }

    private String getPrettyQueryAnalysisFinishTime() {
        if (queryBeginTime == -1 || queryAnalysisFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(queryAnalysisFinishTime - queryBeginTime, TUnit.TIME_NS);
    }

    private String getPrettyQueryPlanFinishTime() {
        if (queryAnalysisFinishTime == -1 || queryPlanFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(queryPlanFinishTime - queryAnalysisFinishTime, TUnit.TIME_NS);
    }

    private String getPrettyQueryScheduleFinishTime() {
        if (queryPlanFinishTime == -1 || queryScheduleFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(queryScheduleFinishTime - queryPlanFinishTime, TUnit.TIME_NS);
    }

    private String getPrettyQueryFetchResultFinishTime() {
        if (queryScheduleFinishTime == -1 || queryFetchResultFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(queryFetchResultFinishTime - queryScheduleFinishTime, TUnit.TIME_NS);
    }

    public void initRuntimeProfile(RuntimeProfile plannerProfile) {
        plannerProfile.addInfoString(KEY_ANALYSIS, getPrettyQueryAnalysisFinishTime());
        plannerProfile.addInfoString(KEY_PLAN, getPrettyQueryPlanFinishTime());
        plannerProfile.addInfoString(KEY_SCHEDULE, getPrettyQueryScheduleFinishTime());
        plannerProfile.addInfoString(KEY_FETCH, getPrettyQueryFetchResultFinishTime());
    }
}
