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

public class DeriveFactory {

    public BaseStatsDerive getStatsDerive(StatisticalType statisticalType) {
        switch (statisticalType) {
            case AGG_NODE:
                return new AggStatsDerive();
            case ANALYTIC_EVAL_NODE:
                return new AnalyticEvalStatsDerive();
            case ASSERT_NUM_ROWS_NODE:
                return new AssertNumRowsStatsDerive();
            case NESTED_LOOP_JOIN_NODE:
                return new NestedLoopJoinStatsDerive();
            case EMPTY_SET_NODE:
            case REPEAT_NODE:
                return new EmptySetStatsDerive();
            case EXCHANGE_NODE:
                return new ExchangeStatsDerive();
            case HASH_JOIN_NODE:
                return new HashJoinStatsDerive();
            case OLAP_SCAN_NODE:
                return new OlapScanStatsDerive();
            case MYSQL_SCAN_NODE:
            case ODBC_SCAN_NODE:
                return new MysqlStatsDerive();
            case SELECT_NODE:
            case SORT_NODE:
                return new SelectStatsDerive();
            case TABLE_FUNCTION_NODE:
                return new TableFunctionStatsDerive();
            case BROKER_SCAN_NODE:
            case EXCEPT_NODE:
            case ES_SCAN_NODE:
            case HIVE_SCAN_NODE:
            case ICEBERG_SCAN_NODE:
            case LAKESOUL_SCAN_NODE:
            case PAIMON_SCAN_NODE:
            case INTERSECT_NODE:
            case SCHEMA_SCAN_NODE:
            case STREAM_LOAD_SCAN_NODE:
            case UNION_NODE:
            case DEFAULT:
            default:
                return new BaseStatsDerive();
        }
    }
}
