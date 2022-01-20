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

import org.apache.doris.catalog.Catalog;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * The StatisticsTask belongs to one StatisticsJob.
 * A job may be split into multiple tasks but a task can only belong to one job.
 * @granularityDesc, @categoryDesc, @statsTypeList
 * These three attributes indicate which statistics this task is responsible for collecting.
 * In general, a task will collect more than one @StatsType at the same time
 * while all of types belong to the same @granularityDesc and @categoryDesc.
 * For example: the task is responsible for collecting min, max, ndv of t1.c1 in partition p1.
 * @granularityDesc: StatsGranularity=partition
 */
public class StatisticsTask implements Callable<StatisticsTaskResult> {
    protected long id = Catalog.getCurrentCatalog().getNextId();;
    protected long jobId;
    protected StatsGranularityDesc granularityDesc;
    protected StatsCategoryDesc categoryDesc;
    protected List<StatsType> statsTypeList;

    public StatisticsTask(long jobId, StatsGranularityDesc granularityDesc,
                          StatsCategoryDesc categoryDesc, List<StatsType> statsTypeList) {
        this.jobId = jobId;
        this.granularityDesc = granularityDesc;
        this.categoryDesc = categoryDesc;
        this.statsTypeList = statsTypeList;
    }

    @Override
    public StatisticsTaskResult call() throws Exception {
        // TODO
        return null;
    }
}
