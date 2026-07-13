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

suite("topNLazyMaterializationUsingIndex") {
    sql """
        set runtime_filter_mode = 'OFF';
        set disable_join_reorder = true;
        drop table if exists t1;
        CREATE TABLE t1
        (
        `user_id` LARGEINT NOT NULL,
        `username` VARCHAR(50) NOT NULL,
        age int,
        addr VARCHAR(50) NOT NULL
        )
        duplicate KEY(user_id, username)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1");

        insert into t1 values ( 1, 'a', 10, 'cd'),(1,'b', 20, 'cq');


        drop table if exists t2;
        CREATE TABLE t2
        (
        `user_id` LARGEINT NOT NULL,
        `username` VARCHAR(50) NOT NULL,
        age int,
        addr VARCHAR(50) NOT NULL
        )
        duplicate KEY(user_id, username)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1");

        insert into t2 values ( 1, 'a', 10, 'cd'),(1,'b', 20, 'cq');

        drop table if exists topn_lazy_filter_order_key;
        CREATE TABLE topn_lazy_filter_order_key
        (
        `k1` INT NOT NULL,
        `k2` INT NOT NULL,
        `v` INT NULL,
        `pad` STRING NULL,
        INDEX idx_v(v) USING INVERTED
        )
        DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1");

        insert into topn_lazy_filter_order_key values
            (1, 10, NULL, 'a'),
            (2, 20, 1, 'b'),
            (3, 30, 2, 'c'),
            (4, 40, NULL, 'd'),
            (5, 50, 3, 'e');

        set topn_lazy_materialization_using_index = true;
        set topn_lazy_materialization_threshold = 1;
        set enable_segment_limit_pushdown = false;
        SET detail_shape_nodes='PhysicalProject';
        """
        qt_plan """
        explain shape plan
        select * from t1 where user_id = 1 order by username limit 1;
        """
        qt_exec """
        select * from t1 where user_id = 1 order by username limit 1;
        """

        qt_plan2 """
        explain shape plan
        select t2.* from t1 join t2 on t1.username=t2.username where t2.user_id > 0 order by username limit 1;
        """

        qt_exe2 """
        select t2.*, t1.* from t1 join t2 on t1.username=t2.username where t2.user_id > 0 order by username limit 1;
        """

        qt_plan_no_effect """
        explain shape plan
        select * from t1 where
            user_id > 0 order by user_id limit 1;
            """

        order_qt_filter_order_key_not_pruned """
        select k1, k2, v, pad
        from topn_lazy_filter_order_key
        where (v is null or k2 < 40)
        order by k1, k2
        limit 1;
        """

}
