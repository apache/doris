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

suite("nereids_scalar_fn_Date") {
  sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
  sql """
  CREATE TABLE `datewithstr` (
    `id` bigint,
    `date` varchar(100)
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    )
  """
  sql """
  insert into datewithstr values(1, '1920-15-10'), (2, '1999-12-12'), (3, null), (4, 'doris');
  """

  qt_sql_date_str "select date(date) from datewithstr"
  qt_sql_datev2_str "select datev2(date) from datewithstr"
  qt_sql_to_date_str "select to_date(date) from datewithstr"
  qt_sql_to_datev2_str "select to_datev2(date) from datewithstr"
}