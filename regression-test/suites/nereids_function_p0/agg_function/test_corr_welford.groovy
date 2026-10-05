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

suite("test_corr_welford") {
    sql """ DROP TABLE IF EXISTS test_corr_welford """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
        CREATE TABLE test_corr_welford (
          `id` int,
          `x` int,
          `y` int,
        ) ENGINE=OLAP
        Duplicate KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    
    // Perfect positive corr_welfordelation    
    sql """
        insert into test_corr_welford values
        (1, 1, 1),
        (2, 2, 2),
        (3, 3, 3),
        (4, 4, 4),
        (5, 5, 5)
        """
    qt_sql "select corr_welford(x,y) from test_corr_welford"
    sql """ truncate table test_corr_welford """
    
    // Perfect negative corr_welfordelation
    sql """
    insert into test_corr_welford values
    (1, 1, 5),
    (2, 2, 4),
    (3, 3, 3),
    (4, 4, 2),
    (5, 5, 1)
    """
    qt_sql "select corr_welford(x,y) from test_corr_welford"
    sql """ truncate table test_corr_welford """
    
    // Zero corr_welfordelation
    sql """
    insert into test_corr_welford values
    (1, 1, 1),
    (2, 1, 2),
    (3, 1, 3),
    (4, 1, 4),
    (5, 1, 5)
    """
    qt_sql "select corr_welford(x,y) from test_corr_welford"
    sql """ truncate table test_corr_welford """
    
    // Partial linear corr_welfordelation
    sql """
    insert into test_corr_welford values
    (1, 1, 1),
    (2, 2, 2),
    (3, 3, 3),
    (4, 4, 4),
    (5, 5, 10)
    """
    qt_sql "select corr_welford(x,y) from test_corr_welford"

    qt_sql "select corr_welford(cast(x as float),cast(y as float)) from test_corr_welford"

    qt_sql_const1 "select corr_welford(x,1) from test_corr_welford"
    qt_sql_const2 "select corr_welford(x,1e100) from test_corr_welford"
    qt_sql_const3 "select corr_welford(x,1e-100) from test_corr_welford"
    qt_sql_const4 "select corr_welford(1,y) from test_corr_welford"
    qt_sql_const5 "select corr_welford(1e100,y) from test_corr_welford"
    qt_sql_const6 "select corr_welford(1e-100,y) from test_corr_welford"
}
