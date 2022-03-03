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

import org.apache.doris.common.Config;

import java.util.List;

/*
The @SampleSQLStatisticsTask is also a statistical task that executes a query
and uses the query result as a statistical value (same as @SQLStatisticsTask).
The only difference from the SQLStatisticsTask is that the query is a sampling table query.
 */
public class SampleSQLStatisticsTask extends SQLStatisticsTask {
    private float samplePercentage = Config.cbo_default_sample_percentage;

    public SampleSQLStatisticsTask(long jobId, StatsGranularityDesc granularityDesc,
                                   StatsCategoryDesc categoryDesc, List<StatsType> statsTypeList) {
        super(jobId, granularityDesc, categoryDesc, statsTypeList);
    }

    @Override
    protected void constructQuery() {
        // TODO
        super.constructQuery();
        // step1: construct table sample
    }
}
