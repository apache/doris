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
suite("test_two_phase_read_with_having") { // "arrow_flight_sql" not support two phase read

    sql """ set enable_partition_topn = 1; """
    sql """ set topn_opt_limit_threshold = 1024; """
    sql """ set enable_two_phase_read_opt = 1; """

    sql """
        drop table if exists `test_two_phase_read_with_having_tbl`;
    """

    sql """
        CREATE TABLE `test_two_phase_read_with_having_tbl` (
          `id` bigint(20) NOT null,
          `account_id` bigint(20) DEFAULT NULL,
          `queue_id` int(11) DEFAULT NULL,
          `call_date` datetime DEFAULT NULL,
          `loan_id_list` varchar(200) DEFAULT NULL
        ) DUPLICATE KEY (`id`,`account_id`)
        DISTRIBUTED BY HASH (`id`) BUCKETS 3
        PROPERTIES("replication_num" = "1");
    """

    sql """
        INSERT INTO `test_two_phase_read_with_having_tbl` (id,`account_id`, `queue_id`, `call_date`, `loan_id_list`) VALUES
            (10001, 10001, 23, '2023-08-24 10:00:00', '249073260533940345,249073260533940346'),
            (10002, 10002, 23, '2023-08-25 12:00:00', '249073260533940347,249073260533940348'),
            (10003, 10003, 23, '2023-08-26 14:00:00', '249073260533940345,249073260533940350'),
            (10004, 10004, 23, '2023-08-27 16:00:00', '249073260533940351,249073260533940352'),
            (10005, 10005, 23, '2023-08-28 18:00:00', '249073260533940346,249073260533940353');
    """

    qt_test """
        select
            `account_id`
            ,FIND_IN_SET(249073260533940345,loan_id_list) as loan_id
        from `test_two_phase_read_with_having_tbl`
        where
            `queue_id` in (23) and `call_date` >= '2023-08-23 20:33:11' and `call_date` <= '2023-11-20 20:33:11' having loan_id >= 1
        order by `id` desc limit 20 offset 0;
    """
}