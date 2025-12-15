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


suite("test_bind_slot") {
    sql """
      drop table if exists t; 
       CREATE TABLE t (
      `id` int COMMENT '',
      `status` string COMMENT '',
      `time_created` string COMMENT '',
    )  properties("replication_num" = "1");

  """
  // if scope is not deduplicated, time_created wil be bound twice, causing a slot ambigious error
  sql """select
          from_unixtime(time_created / 1000, 'yyyyMMdd') as date1,
          status,
          count(distinct id) as cnt
          from
            (
              select
                from_unixtime(time_created / 1001) as created_date,
                time_created,
                *
              from
                t
            ) t1
          group by
          from_unixtime(time_created / 1000, 'yyyyMMdd'),
          status;
  """
}
