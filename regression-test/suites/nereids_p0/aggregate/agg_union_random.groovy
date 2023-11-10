/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("agg_union_random") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "DROP TABLE IF EXISTS test_random;"
    sql """
        create table test_random
        (
            a varchar(100) null,
            b decimalv3(18,10) null
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY HASH(`a`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    explain{
        sql "select a from (select * from test_random union all (select * from test_random))t group by a"
            /**
            STREAM DATA SINK
                EXCHANGE ID: 258
                RANDOM

            252:VOlapScanNode
                TABLE: default_cluster:regression_test_nereids_p0_aggregate.test_random(test_random), PREAGGREGATION: ON
                partitions=0/1, tablets=0/0, tabletList=
                cardinality=1, avgRowSize=0.0, numNodes=1
                pushAggOp=NONE
            **/
        contains "RANDOM"
    }

    sql "DROP TABLE IF EXISTS test_random;"
}
