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

suite("agg_with_const") {
    sql "SET enable_nereids_planner=true"

    sql """
        DROP TABLE IF EXISTS agg_with_const_tbl
       """

    sql """CREATE TABLE IF NOT EXISTS agg_with_const_tbl (col1 int not null, col2 int not null, col3 int not null)
        DISTRIBUTED BY HASH(col3)
        BUCKETS 1
        PROPERTIES(
            "replication_num"="1"
        )
        """

    sql """
    insert into agg_with_const_tbl values(1994, 1994, 1995)
    """

    sql "SET enable_fallback_to_original_planner=false"

    sql """sync"""
    qt_select """
        select count(2) + 1, sum(2) + sum(col1) from agg_with_const_tbl
    """

    qt_select """
        select count(*) from agg_with_const_tbl
    """

    qt_select """
         select count(2) + 1, sum(2) + sum(2) from agg_with_const_tbl
    """

    explain {
        sql """select count(*) from ( select distinct col1 as a0, null as not_appear_col from agg_with_const_tbl)t"""
        notContains "not_appear_col"
    }

}