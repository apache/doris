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

suite("valid_grouping"){

    // this suite test legacy  planner
    sql "set enable_nereids_planner=false"

    sql "drop table if exists valid_grouping"
    sql """
    CREATE TABLE `valid_grouping` (
      `a` INT NULL,
      `b` VARCHAR(10) NULL,
      `c` INT NULL,
      `d` INT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`, `b`)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql "insert into valid_grouping values(1,'d2',3,5);"
    test {
        sql """select
        b, 'day' as DT_TYPE
        from valid_grouping
        group by grouping sets ( (grouping_id(b)),(b));"""
        exception("GROUP BY expression must not contain grouping scalar functions: grouping_id(`b`)")
    }

    test {
        sql """select
        b, 'day' as DT_TYPE
        from valid_grouping
        group by grouping sets ( (grouping(b)),(b));"""
        exception("GROUP BY expression must not contain grouping scalar functions: grouping(`b`)")
    }

}