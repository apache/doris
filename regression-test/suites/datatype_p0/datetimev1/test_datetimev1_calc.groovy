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

suite("test_datetimev1_calc", "nonConcurrent") {

    sql """
        admin set frontend config("enable_date_conversion" = "false");
    """

    def table1 = "test_datetimev1_calc_tbl"

    sql "drop table if exists ${table1}"

    sql """
    CREATE TABLE IF NOT EXISTS `${table1}` (
        `id` int,
        `value1` datetimev1 NULL COMMENT "",
        `value2` datetimev1 NULL COMMENT ""
      ) ENGINE=OLAP
      DUPLICATE KEY(`id`)
      COMMENT "OLAP"
      DISTRIBUTED BY HASH(`id`) BUCKETS 4
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    )
    """

    sql """insert into ${table1} values
            (1, '2010-10-01 11:12:13', '2010-10-02 22:22:22'),
            (2, '2010-10-02 12:13:14', '2010-10-02 22:22:22'),
            (5, '2010-10-05 15:35:45', '2010-10-06 16:16:16'),
            (8, null, null), 
            (null, '2010-10-09 19:20:21', null)
    """
    qt_select_all "select * from ${table1} order by 1, 2, 3"

    qt_select_calc1 """
        select
            `id`, `value1`, `value2`
            , value1 + interval 5 day v1
            , value2 - interval 10 day v2
        from ${table1} order by 1, 2, 3, 4, 5;
    """

    qt_select_calc2 """
        select
            `id`, `value1`, `value2`
            , cast(value1 as datev2) v1
            , cast(value1 as date) v2
            , cast(value2 as datetimev2) v3
            , cast(value2 as datetime) v4
        from ${table1} order by 1, 2, 3, 4, 5, 6, 7;
    """

    sql """
        admin set frontend config("enable_date_conversion" = "true");
    """
}
