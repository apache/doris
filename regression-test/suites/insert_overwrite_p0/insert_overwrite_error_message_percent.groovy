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

suite("insert_overwrite_error_message_percent") {
    sql "drop table if exists insert_overwrite_error_message_percent_src"
    sql "drop table if exists insert_overwrite_error_message_percent_dst"

    sql """
        create table insert_overwrite_error_message_percent_src (
            id int,
            d varchar(10)
        ) engine=olap
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties (
            'replication_allocation' = 'tag.location.default: 1'
        )
    """

    sql """
        create table insert_overwrite_error_message_percent_dst (
            id int,
            ts bigint
        ) engine=olap
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties (
            'replication_allocation' = 'tag.location.default: 1'
        )
    """

    sql """
        insert into insert_overwrite_error_message_percent_src values
        (1, '2024-01-01')
    """

    test {
        sql """
            insert overwrite table insert_overwrite_error_message_percent_dst
            select id, unix_timestamp(d, 'yyyyMMdd')
            from insert_overwrite_error_message_percent_src
        """
        exception "Operation unix_timestamp of 2024-01-01, %Y%m%d is invalid"
    }
}
