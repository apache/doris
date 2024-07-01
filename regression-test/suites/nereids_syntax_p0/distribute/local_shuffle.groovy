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

suite("local_shuffle") {
    multi_sql """
        drop table if exists test_local_shuffle1;
        drop table if exists test_local_shuffle2;
        
        CREATE TABLE `test_local_shuffle1` (
          id int,
          id2 int
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "colocate_with" = "test_local_shuffle_with_colocate"
        );
        
        CREATE TABLE `test_local_shuffle2` (
          id int,
          id2 int
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "colocate_with" = "test_local_shuffle_with_colocate"
        );
        
        insert into test_local_shuffle1 values (1, 1), (2, 2);
        insert into test_local_shuffle2 values (2, 2), (3, 3);
        
        set enable_nereids_distribute_planner=true;
        set enable_pipeline_x_engine=true;
        set disable_join_reorder=true;
        set enable_local_shuffle=true;
        set force_to_local_shuffle=true;
        """

    order_qt_read_single_olap_table "select * from test_local_shuffle1"

    order_qt_broadcast_join """
        select *
        from test_local_shuffle1
        join [broadcast]
        test_local_shuffle2
        on test_local_shuffle1.id=test_local_shuffle2.id
        """

    order_qt_shuffle_join """
        select *
        from test_local_shuffle1
        join [shuffle]
        test_local_shuffle2
        on test_local_shuffle1.id2=test_local_shuffle2.id2
        """

    order_qt_bucket_shuffle_join """
        select *
        from test_local_shuffle1
        join [shuffle]
        test_local_shuffle2
        on test_local_shuffle1.id2=test_local_shuffle2.id2
        """

    order_qt_colocate_join """
        select *
        from test_local_shuffle1
        join [shuffle]
        test_local_shuffle2
        on test_local_shuffle1.id=test_local_shuffle2.id
        """

    order_qt_bucket_shuffle_with_prune_tablets """
        select *
        from
        (
            select *
            from test_local_shuffle1
            where id=1
        ) a
        right outer join [shuffle]
        test_local_shuffle2
        on a.id=test_local_shuffle2.id2
        """

    order_qt_bucket_shuffle_with_prune_tablets2 """
        select *
        from
        test_local_shuffle2
        left outer join [shuffle]
        (
            select *
            from test_local_shuffle1
            where id=1
        ) a
        on a.id=test_local_shuffle2.id2
        """

    order_qt_bucket_shuffle_with_prune_tablets3 """
        select *
        from
        (
            select *
            from test_local_shuffle1
            where id=1
        ) a
        left outer join [shuffle]
        test_local_shuffle2
        on a.id=test_local_shuffle2.id2
        """

    multi_sql """
        drop table if exists test_local_shuffle3;
        CREATE TABLE IF NOT EXISTS test_local_shuffle3 (
         c0 int
        )
        DISTRIBUTED BY HASH (c0) BUCKETS 10 PROPERTIES ("replication_num" = "1");
        
        drop table if exists test_local_shuffle4;
        CREATE TABLE IF NOT EXISTS test_local_shuffle4 (
          c0 int
        )
        DISTRIBUTED BY HASH (c0) BUCKETS 10 PROPERTIES ("replication_num" = "1");
        INSERT INTO test_local_shuffle3 (c0) VALUES (1), (3);
        INSERT INTO test_local_shuffle4 (c0) VALUES (2), (3);
        
        sync;
        
        set enable_nereids_distribute_planner=true;
        set enable_pipeline_x_engine=true;
        set disable_join_reorder=true;
        set enable_local_shuffle=true;
        set force_to_local_shuffle=true;
        """

    order_qt_fillup_bucket """
            SELECT cast(a.c0 as int), cast(b.c0 as int) FROM
            (select * from test_local_shuffle3 where c0 =1)a
            RIGHT OUTER JOIN
            (select * from test_local_shuffle4)b
            ON a.c0 = b.c0
            """

    multi_sql """
        drop table if exists test_shuffle_left_with_local_shuffle;
        CREATE TABLE `test_shuffle_left_with_local_shuffle` (
          id int,
          id2 int
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        
        insert into test_shuffle_left_with_local_shuffle values (1, 1), (2, 2), (3, 4);
        """

    order_qt_shuffle_left """
            select *
            from
            (
              select id2
              from test_shuffle_left_with_local_shuffle
              group by id2
            ) a
            inner join [shuffle]
            test_shuffle_left_with_local_shuffle b
            on a.id2=b.id;
        """
}
