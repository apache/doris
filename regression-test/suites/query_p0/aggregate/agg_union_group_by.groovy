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

suite("agg_union_group_by") {
    sql "DROP TABLE IF EXISTS t1;"
    sql "DROP TABLE IF EXISTS t2;"
    
    sql """
        create table t1
        (
            a int NULL,
            b int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`, `b`)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    
    sql """
        create table t2
        (
            a int NULL,
            b int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`, `b`)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    
    sql "insert into t1 values(1,2),(3,4),(1,2),(3,4),(5,6),(5,6);"
    sql "insert into t2 values(1,2),(3,4),(1,2),(3,4),(7,8),(7,8);"
    
    // Test case: agg-union with group by on random distributed tables
    // The physical plan should not have redundant aggregation layers
    // when there's no PhysicalDistribute between aggregation and union
    explain {
        sql "select a, b from t1 group by a, b union all select a, b from t2 group by a, b"
        // Should NOT contain consecutive aggregations without DISTRIBUTE
        notContains "AGGREGATE"
        notContains "Aggregate"
    }
    
    // Verify the result is correct
    qt_agg_union_group_by """
        select a, b from t1 group by a, b 
        union all 
        select a, b from t2 group by a, b
        order by a, b;
    """
    
    sql "DROP TABLE IF EXISTS t1;"
    sql "DROP TABLE IF EXISTS t2;"
}
