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

suite("test_bugfix_block_reuse") {
    sql "drop table if exists test_bugfix_block_reuse;"
    sql """
        create table test_bugfix_block_reuse (
            k1 int, v1 decimal(20,3)
        ) distributed by hash(k1) properties("replication_num"="1");
    """
    sql "insert into test_bugfix_block_reuse values(1, 1.1), (2, 2.2), (3, 3.3);"
    sql "sync"
    qt_sql_0 """
        with ta as (
          select
            `v1` as source_,
            'funnel_seq_1' as funnel_seq_
          from
            test_bugfix_block_reuse
        )
        select
          left_.source_ as source_
        from
          (
            select
              source_ as source_,
              row_number() over(PARTITION BY source_) as session_id_
            from
              ta
            where
              funnel_seq_ IN ('funnel_seq_1')
          ) left_
          inner join (
            select
              source_ as source_
            from
              ta
            where
              funnel_seq_ IN ('funnel_seq_2')
          ) right_ on right_.source_ = left_.source_
          order by 1;
    """

    qt_sql_1 """
        with ta as (
          select
            `v1` as source_,
            'funnel_seq_1' as funnel_seq_
          from
            test_bugfix_block_reuse
        )
        select
          left_.source_ as source_
        from
          (
            select
              source_ as source_,
              row_number() over(PARTITION BY source_) as session_id_
            from
              ta
            where
              funnel_seq_ IN ('funnel_seq_1')
          ) left_
          inner join (
            select
              source_ as source_
            from
              ta
            where
              funnel_seq_ IN ('funnel_seq_1')
          ) right_ on right_.source_ = left_.source_
          order by 1;
    """
}