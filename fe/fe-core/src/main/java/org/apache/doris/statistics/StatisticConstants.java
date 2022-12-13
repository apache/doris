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

import java.util.concurrent.TimeUnit;

public class StatisticConstants {
    public static final String STATISTIC_TBL_NAME = "column_statistics";

    public static final String ANALYSIS_JOB_TABLE = "analysis_jobs";

    public static final int MAX_NAME_LEN = 64;

    public static final int ID_LEN = 4096;

    public static final int STATISTIC_PARALLEL_EXEC_INSTANCE_NUM = 1;

    public static final int STATISTICS_CACHE_VALID_DURATION_IN_HOURS = 24 * 2;

    public static final int STATISTICS_CACHE_REFRESH_INTERVAL = 24 * 2;

    /**
     * Bucket count fot column_statistics and analysis_job table.
     */
    public static final int STATISTIC_TABLE_BUCKET_COUNT = 7;

    public static final long STATISTICS_MAX_MEM_PER_QUERY_IN_BYTES = 2L * 1024 * 1024 * 1024;

    /**
     * Determine the execution interval for 'Statistics Table Cleaner' thread.
     */
    public static final int STATISTIC_CLEAN_INTERVAL_IN_HOURS = 24 * 2;


    /**
     * The max cached item in `StatisticsCache`.
     */
    public static final long STATISTICS_RECORDS_CACHE_SIZE = 100000;

    /**
     * If analysis job execution time exceeds this time, it would be cancelled.
     */
    public static final long STATISTICS_TASKS_TIMEOUT_IN_MS = TimeUnit.MINUTES.toMillis(10);


    public static final int LOAD_TASK_LIMITS = 10;

}
