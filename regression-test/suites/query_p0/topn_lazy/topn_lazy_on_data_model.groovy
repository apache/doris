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

suite("topn_lazy_on_data_model") {
    // mor unique key user_id is not lazy materialized
    sql """
        drop table if exists mor;
        CREATE TABLE mor
        (
        `user_id` LARGEINT NOT NULL,
        `username` VARCHAR(50) NOT NULL,
        age int
        )
        UNIQUE KEY(user_id, username)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "false"
        );

        insert into mor values ( 1, 'a', 10),(1,'b', 20);
    """

    qt_shape "explain shape plan select * from mor order by username limit 1"


    // mow unique key user_id is lazy materialized
    sql """
        drop table if exists mow;
        CREATE TABLE IF NOT EXISTS mow
        (
        `user_id` LARGEINT NOT NULL,
        `username` VARCHAR(50) NOT NULL,
        age int
        )
        UNIQUE KEY(user_id, username)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "true"
        );

        insert into mow values ( 1, 'a', 10),(1,'b', 20);
    """
    
    qt_shape "explain shape plan select *  from mow order by username limit 1"

    // agg key user_id is lazy materialized
    sql """
        drop table if exists agg; 
        CREATE TABLE IF NOT EXISTS agg
        (
        `user_id` LARGEINT NOT NULL,
        `username` VARCHAR(50) NOT NULL,
        age int REPLACE
        )
        aggregate KEY(user_id, username)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
     insert into agg values ( 1, 'a', 10),(1,'b', 20);
    """
    
    qt_shape "explain shape plan select *  from agg order by username limit 1"
}