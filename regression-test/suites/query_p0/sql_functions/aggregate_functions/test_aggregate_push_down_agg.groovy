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

suite("test_aggregate_push_down_agg") {
    sql """ drop table if exists test_aggregate_push_down_agg; """
    sql """
        create table test_aggregate_push_down_agg (
            k1 int,
            tracking_number varchar(16)
        ) duplicate key(k1)
        distributed by hash(k1)
        properties("replication_num"="1");
    """
    sql """ insert into test_aggregate_push_down_agg values(1, "aaa"), (1, "bbb"), (2, "ccc") """
    qt_sql """
        SELECT
           count(*)
        FROM
           (
               select
                  case
                     when tracking_number is null then ''
                     else tracking_number
                  end as tracking_number
               from
                  test_aggregate_push_down_agg
           ) V_T;
    """
}
