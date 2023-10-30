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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.FeConstants;
import org.apache.doris.system.SystemInfoService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class StatisticConstants {

    public static final String STATISTIC_TBL_NAME = "column_statistics";
    public static final String HISTOGRAM_TBL_NAME = "histogram_statistics";

    public static final int MAX_NAME_LEN = 64;

    public static final int ID_LEN = 4096;

    public static final int STATISTICS_CACHE_REFRESH_INTERVAL = 24 * 2;

    /**
     * Bucket count fot column_statistics and analysis_job table.
     */
    public static final int STATISTIC_TABLE_BUCKET_COUNT = 7;

    /**
     * Determine the execution interval for 'Statistics Table Cleaner' thread.
     */
    public static final int STATISTIC_CLEAN_INTERVAL_IN_HOURS = 24 * 2;

    public static final long PRELOAD_RETRY_TIMES = 5;

    public static final long PRELOAD_RETRY_INTERVAL_IN_SECONDS = TimeUnit.SECONDS.toMillis(10);

    public static final int ANALYSIS_JOB_INFO_EXPIRATION_TIME_IN_DAYS = 7;

    public static final int FETCH_LIMIT = 10000;
    public static final int FETCH_INTERVAL_IN_MS = 500;

    public static final int HISTOGRAM_MAX_BUCKET_NUM = 128;

    public static final int ANALYZE_MANAGER_INTERVAL_IN_SECS = 60;

    public static List<String> SYSTEM_DBS = new ArrayList<>();

    public static int ANALYZE_TASK_RETRY_TIMES = 5;

    public static final String DB_NAME = SystemInfoService.DEFAULT_CLUSTER + ":" + FeConstants.INTERNAL_DB_NAME;

    public static final String FULL_QUALIFIED_STATS_TBL_NAME = FeConstants.INTERNAL_DB_NAME + "." + STATISTIC_TBL_NAME;

    public static final int STATISTIC_INTERNAL_TABLE_REPLICA_NUM = 3;

    public static final int RETRY_LOAD_QUEUE_SIZE = 1000;

    public static final int RETRY_LOAD_THREAD_POOL_SIZE = 1;

    public static final int LOAD_RETRY_TIMES = 3;

    // union more relation than 512 may cause StackOverFlowException in the future.
    public static final int UNION_ALL_LIMIT = 512;

    public static final String FULL_AUTO_ANALYZE_START_TIME = "00:00:00";
    public static final String FULL_AUTO_ANALYZE_END_TIME = "23:59:59";

    public static final int ANALYZE_JOB_BUF_SIZE = 20000;

    static {
        SYSTEM_DBS.add(SystemInfoService.DEFAULT_CLUSTER
                + ClusterNamespace.CLUSTER_DELIMITER + FeConstants.INTERNAL_DB_NAME);
        SYSTEM_DBS.add(SystemInfoService.DEFAULT_CLUSTER
                + ClusterNamespace.CLUSTER_DELIMITER + "information_schema");
    }

    public static boolean isSystemTable(TableIf tableIf) {
        if (tableIf instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) tableIf;
            if (StatisticConstants.SYSTEM_DBS.contains(olapTable.getQualifiedDbName())) {
                return true;
            }
        }
        return false;
    }

    public static boolean shouldIgnoreCol(TableIf tableIf, Column c) {
        if (isSystemTable(tableIf)) {
            return true;
        }
        return !c.isVisible();
    }
}
