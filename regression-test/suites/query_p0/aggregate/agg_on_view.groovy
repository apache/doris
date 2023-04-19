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

suite("agg_on_view") {

    sql """
        drop table if exists agg_on_view_test;
    """
    sql """
    create table agg_on_view_test (
            id int,
                    user_id int,
            name varchar(20)
    ) ENGINE=OLAP
    UNIQUE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
    );
    """

    qt_sql """
    select user_id,null_or_empty(tag) = 1,count(*)
    from (
            select *,
            "abc" as tag
            from agg_on_view_test limit 10)t
    group by user_id,tag
    """

    sql """
        drop table agg_on_view_test;
    """
}