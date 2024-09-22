/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("test_reject_not_default_false") {
    sql "drop table if exists test_reject_not_default_false"
    sql """CREATE TABLE `test_reject_not_default_false` (
    `c1` VARCHAR(100) NULL COMMENT '类型',
    `c2` DATETIME NULL COMMENT '收集时间'
    ) ENGINE=OLAP
    DUPLICATE KEY(`c1`)
    COMMENT '环境数据'
    PARTITION BY RANGE(`c2`)
    (
    PARTITION p20240908 VALUES [('2024-09-08 00:00:00'), ('2024-09-09 00:00:00')),
    PARTITION p20240909 VALUES [('2024-09-09 00:00:00'), ('2024-09-10 00:00:00')),
    PARTITION p20240910 VALUES [('2024-09-10 00:00:00'), ('2024-09-11 00:00:00')),
    PARTITION p20240911 VALUES [('2024-09-11 00:00:00'), ('2024-09-12 00:00:00')),
    PARTITION p20240912 VALUES [('2024-09-12 00:00:00'), ('2024-09-13 00:00:00')))
    DISTRIBUTED BY HASH(`c1`) BUCKETS 10
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql "INSERT INTO  test_reject_not_default_false  VALUES( '1703222', '2024-09-09 09:22:44');"

    qt_test_not_like "SELECT * FROM test_reject_not_default_false WHERE '44003' NOT LIKE '%100';"
}