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

suite("test_order_by") {
    sql """
        drop table if exists test_order_by;
    """

    sql """
    create table if not exists test_order_by(
        create_time datetime null default current_timestamp,
        run_time varchar(200) null comment '时间戳',
        create_time2 datetime null
    )
    duplicate key(create_time,run_time)
    distributed by hash(create_time) buckets 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
        insert into test_order_by values ('2023-12-18 23:56:12','2023-12-19 00:01:08.799','2023-12-18 23:56:12');
    """
    sql """
        insert into test_order_by values ('2023-12-18 23:56:12','2023-12-19 00:01:08.797','2023-12-18 23:56:12');
    """
    sql """
        insert into test_order_by values ('2023-12-18 23:56:12','2023-12-19 00:01:08.796','2023-12-18 23:56:12');
    """
    sql """
        insert into test_order_by values ('2023-12-19 00:01:12','2023-12-19 00:06:08.618','2023-12-18 23:56:12');
    """
    sql """
        insert into test_order_by values ('2023-12-19 00:01:12','2023-12-19 00:05:58.513','2023-12-18 23:56:12');
    """

    qt_select """
        select * from test_order_by order by create_time desc;
    """

    qt_select """
        select * from test_order_by order by create_time, run_time desc;
    """

    qt_select """
        select * from test_order_by order by create_time desc, run_time desc;
    """   
}