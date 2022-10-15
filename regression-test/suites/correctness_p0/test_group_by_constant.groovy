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

suite("test_group_by_constant") {

    sql """
        DROP TABLE IF EXISTS `table_group_by_constant`;
    """

    sql """
        CREATE TABLE `table_group_by_constant` (
        `inc_day` date NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`inc_day`)
        DISTRIBUTED BY HASH(`inc_day`) BUCKETS 5
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into table_group_by_constant values('1999-12-01');
    """

    qt_sql """
        SELECT
        case
            when (inc_day = date_sub(curdate(), interval 1 day)) then 'A'
            when (inc_day = date_sub(curdate(), interval 8 day)) then 'B'
            when (inc_day = date_sub(curdate(), interval 365 day)) then 'C'
            else 'D'
        end
        from
        table_group_by_constant
        group by
        case
            when (inc_day = date_sub(curdate(), interval 1 day)) then 'A'
            when (inc_day = date_sub(curdate(), interval 8 day)) then 'B'
            when (inc_day = date_sub(curdate(), interval 365 day)) then 'C'
            else 'D'
        end;
    """

    sql """
        DROP TABLE IF EXISTS `table_group_by_constant`;
    """
}
