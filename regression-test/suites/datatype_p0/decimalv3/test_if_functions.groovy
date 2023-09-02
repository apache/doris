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

suite("test_if_functions") {
    sql "drop table if exists test_query_ifnull"

    sql """
    CREATE TABLE test_query_ifnull(
        `id` int(11) not null,
        `value` decimalv3(16, 2) NULL,
        `flag` varchar(5) null
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
    );
    """

    sql """insert into test_query_ifnull values(1, null, '3'), (2, 3.4, '3')"""
    qt_select1 """select
                        if (flag_value is null, 0, flag_value)
                    from (
                        select
                            id,
                            if (flag = 3, value, 0) flag_value
                        from
                            test_query_ifnull) t1
                    order by id;"""
    sql "drop table if exists test_query_ifnull"
}
