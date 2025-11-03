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

suite("test_limit_subquery") {

    sql "drop table if exists customer_repayment;"

    sql """
        CREATE TABLE `customer_repayment` (
        `repayment_id` bigint(20) NOT NULL COMMENT '',
        `register_date` datetime NULL COMMENT ''
        ) ENGINE=OLAP
        UNIQUE KEY(`repayment_id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`repayment_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql "insert into customer_repayment values (1, '2023-01-18 00:00:00'), (2, '2023-01-18 00:00:01'), (3, '2023-01-18 00:00:02');"

    qt_sql1 """
        SELECT
            repayment_id 
        FROM
            customer_repayment 
        WHERE
            repayment_id IN ( SELECT repayment_id FROM customer_repayment ORDER BY register_date DESC LIMIT 0, 1 ) 
        ORDER BY
            register_date DESC;
        """

    qt_sql2 """
        SELECT
            repayment_id 
        FROM
            customer_repayment 
        WHERE
            repayment_id IN ( SELECT repayment_id FROM customer_repayment ORDER BY register_date DESC LIMIT 1, 1 ) 
        ORDER BY
            register_date DESC;
        """

}