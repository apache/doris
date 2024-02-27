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

suite("test_covar_samp") {
    sql """ DROP TABLE IF EXISTS test_covar_samp """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
        CREATE TABLE test_covar_samp (
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
    
    sql """
        insert into test_covar_samp values
        (1, 1, 1),
        (2, 2, 1),
        (3, 3, 1),
        (4, 4, 3)
        """
    qt_sql "select covar_samp(x,y) from test_covar_samp"
    sql """ truncate table test_covar_samp """
    
    sql """
    insert into test_covar_samp values
    (1, 1, 4),
    (2, 2, 1),
    (3, 3, 1),
    (4, 4, 1)
    """
    qt_sql "select covar_samp(x,y) from test_covar_samp"
    sql """ truncate table test_covar_samp """
    
    // Zero covar
    sql """
    insert into test_covar_samp values
    (1, 1, 1),
    (2, 1, 2),
    (3, 1, 3),
    (4, 1, 4),
    (5, 1, 5)
    """
    qt_sql "select covar_samp(x,y) from test_covar_samp"
    sql """ truncate table test_covar_samp """
    
    // Partial linear 
    sql """
    insert into test_covar_samp values
    (1, 1, 1),
    (2, 2, 1),
    (3, 3, 1),
    (4, 4, 10)
    """
    qt_sql "select covar_samp(x,y) from test_covar_samp"
    sql """ truncate table test_covar_samp """

    sql """
    insert into test_covar_samp values
    (1, 1, 1),
    (2, 2, 2),
    (3, 3, 3),
    (4, 4, 4)
    """
    qt_sql "select covar_samp(x,y) from test_covar_samp"
    
    sql """ DROP TABLE IF EXISTS test_covar_samp """
}
