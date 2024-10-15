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

public enum StatisticalType {
    DEFAULT,
    AGG_NODE,
    ANALYTIC_EVAL_NODE,
    ASSERT_NUM_ROWS_NODE,
    CTE_SCAN_NODE,
    BROKER_SCAN_NODE,
    NESTED_LOOP_JOIN_NODE,
    EMPTY_SET_NODE,
    ES_SCAN_NODE,
    EXCEPT_NODE,
    EXCHANGE_NODE,
    HASH_JOIN_NODE,
    HIVE_SCAN_NODE,
    ICEBERG_SCAN_NODE,
    PAIMON_SCAN_NODE,
    HUDI_SCAN_NODE,
    TVF_SCAN_NODE,
    INTERSECT_NODE,
    LOAD_SCAN_NODE,
    MYSQL_SCAN_NODE,
    ODBC_SCAN_NODE,
    OLAP_SCAN_NODE,
    PARTITION_TOPN_NODE,
    REPEAT_NODE,
    SELECT_NODE,
    SET_OPERATION_NODE,
    SCHEMA_SCAN_NODE,
    SORT_NODE,
    STREAM_LOAD_SCAN_NODE,
    TABLE_FUNCTION_NODE,
    UNION_NODE,
    TABLE_VALUED_FUNCTION_NODE,
    FILE_SCAN_NODE,
    MAX_COMPUTE_SCAN_NODE,
    METADATA_SCAN_NODE,
    JDBC_SCAN_NODE,
    TEST_EXTERNAL_TABLE,
    GROUP_COMMIT_SCAN_NODE,
    TRINO_CONNECTOR_SCAN_NODE,
    LAKESOUL_SCAN_NODE
}
