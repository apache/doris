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

suite("agg_with_sort") {
    sql "SET enable_nereids_planner=true"

    sql """
        DROP TABLE IF EXISTS t1
       """

    sql """
        CREATE TABLE IF NOT EXISTS t1 (a int not null)
        DISTRIBUTED BY HASH(a)
        BUCKETS 1
        PROPERTIES(
            "replication_num"="1"
        );
        """

    sql """
        DROP TABLE IF EXISTS t2
       """

    sql """
        CREATE TABLE IF NOT EXISTS t2 (b int not null, c int not null)
        DISTRIBUTED BY HASH(b)
        BUCKETS 1
        PROPERTIES(
            "replication_num"="1"
        );
        """

    sql """
    insert into t1 values(1);
    """
    
    sql """
    insert into t2 values(2, 3);
    """

    sql "SET enable_fallback_to_original_planner=false"

    sql """sync"""

    qt_select """
        SELECT t1.a
        FROM t1,
        (SELECT b FROM t2 ORDER BY c ) t3;
    """
}
