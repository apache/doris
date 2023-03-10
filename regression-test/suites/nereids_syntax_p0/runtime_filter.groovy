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

suite("runtime_filter") {
    sql "drop table if exists date_v2_table"
    sql "drop table if exists datetime_table"

    sql """CREATE TABLE `date_v2_table` (
        `user_id` largeint(40) NOT NULL COMMENT '用户id',
        `date` dateV2 NOT NULL COMMENT '数据灌入日期时间'
    ) distributed by hash(user_id) buckets 1 
    properties("replication_num"="1");
    """

    sql """CREATE TABLE `datetime_table` (
        `user_id` largeint(40) NOT NULL COMMENT '用户id',
        `date` datetime NOT NULL COMMENT '数据灌入日期时间'
    ) distributed by hash(user_id) buckets 1 
    properties("replication_num"="1");
    """

    sql "insert into `date_v2_table` values (1, '2011-01-01'), (2, '2011-02-02');"
    sql "insert into `datetime_table` values (1, '2011-01-01 13:00:00'), (2, '2011-02-02 00:00:00');"

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    for (int i = 0; i < 16; ++i) {
        sql "set runtime_filter_type=${i}"
        test {
            sql "SELECT count(1) FROM datetime_table a, date_v2_table b WHERE a.date = b.date;"
            result([[1L]])
        }
    }
}