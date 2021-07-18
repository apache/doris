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

package org.apache.doris.metric.collector;


/**
 * This class contains the keywords and methods used to read and write metric from bdbje.
 */
class BDBJEMetricUtils {
    private static final String KEY_SEPARATOR = ":";
    private static final String UNDER_LINE = "_";
    static final int WRITE_INTERVAL_MS = 15 * 1000;

    static final String TAGS = "tags";
    static final String METRIC = "metric";
    static final String VALUE = "value";

    static final String METRIC_CPU = "cpu";
    static final String METRIC_MEMORY_ALLOCATED_BYTES = "memory_allocated_bytes";
    static final String METRIC_MAX_DISK_IO_UTIL_PERCENT = "max_disk_io_util_percent";
    static final String DISKS_DATA_USED_CAPACITY = "disks_data_used_capacity";
    static final String DISKS_TOTAL_CAPACITY = "disks_total_capacity";
    static final String CONNECTION_TOTAL = "connection_total";
    static final String QPS = "qps";
    static final String QUERY_LATENCY_MS = "query_latency_ms";
    static final String QUANLITE = "quantile";
    static final String PERCENTILE_75TH = "0.75";
    static final String PERCENTILE_95TH = "0.95";
    static final String PERCENTILE_98TH = "0.98";
    static final String PERCENTILE_99TH = "0.99";
    static final String PERCENTILE_999TH = "0.999";
    static final String QUERY_ERR_RATE = "query_err_rate";
    static final String TXN_BEGIN = "txn_begin";
    static final String BEGIN = "begin";
    static final String TXN_SUCCESS = "txn_success";
    static final String SUCCESS = "success";
    static final String TXN_FAILED = "txn_failed";
    static final String FAILED = "failed";
    static final String BASE_COMPACTION_SCORE = "tablet_base_max_compaction_score";
    static final String CUMU_COMPACTION_SCORE = "tablet_cumulative_max_compaction_score";

    static final String BE_DISKS_DATA_USED_CAPACITY = "be_disks_used";
    static final String BE_DISKS_TOTAL_CAPACITY = "be_disks_total";
    static final String FE_NODE_NUM_TOTAL = "fe_node_num_total";
    static final String FE_NODE_NUM_ALIVE = "fe_node_num_alive";
    static final String BE_NODE_NUM_TOTAL = "be_node_num_total";
    static final String BE_NODE_NUM_ALIVE = "be_node_num_alive";

    static String CPU_TOTAL = "cpu_total";
    static String CPU_IDLE = "cpu_idle";

    // The metric key consists of nodeHost, nodePort, metricName and timestampSeconds.
    static String concatBdbKey(String host, int port, String metricName, long timestamp) {
        return host + KEY_SEPARATOR + port + UNDER_LINE + metricName + UNDER_LINE + timestamp;
    }

    static String concatBdbKey(String metricName, long timestamp) {
        return metricName + UNDER_LINE + timestamp;
    }
}
