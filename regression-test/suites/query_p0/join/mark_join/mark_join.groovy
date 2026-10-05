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

suite("mark_join") {
    String suiteName = "mark_join"
    String table_t1 = "${suiteName}_table_t1"
    String table_t2 = "${suiteName}_table_t2"
    String table_tbl1 = "${suiteName}_table_tbl1"
    String table_tbl2 = "${suiteName}_table_tbl2"
    String table_tbl3 = "${suiteName}_table_tbl3"
    
    sql "drop table if exists ${table_t1};"
    sql "drop table if exists ${table_t2};"
    sql """
        create table ${table_t1} (
            k1 int null,
            k2 int null,
            k3 bigint null,
        k4 varchar(100) null
        )
        duplicate key (k1,k2,k3)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
    """

    sql """
        create table ${table_t2} (
            k1 int null,
            k2 int null,
            k3 bigint null,
        k4 varchar(100) null
        )
        duplicate key (k1,k2,k3)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
    """

    sql "insert into ${table_t1} select 1,1,1,'a';"
    sql "insert into ${table_t1} select 2,2,2,'b';"
    sql "insert into ${table_t1} select 3,-3,null,'c';"
    sql "insert into ${table_t1} select 3,3,null,'c';"

    sql "insert into ${table_t2} select 1,1,1,'a';"
    sql "insert into ${table_t2} select 2,2,2,'b';"
    sql "insert into ${table_t2} select 3,-3,null,'c';"
    sql "insert into ${table_t2} select 3,3,null,'c';"

    qt_test """
    select * from ${table_t1} where exists (select ${table_t2}.k3 from ${table_t2} where ${table_t1}.k2 = ${table_t2}.k2) or k1 < 10 order by k1, k2;
    """
    qt_test """
    select * from ${table_t1} where not exists (select ${table_t2}.k3 from ${table_t2} where ${table_t1}.k2 = ${table_t2}.k2) or k1 < 10 order by k1, k2;
    """
    qt_test """
    select * from ${table_t1} where ${table_t1}.k1 not in (select ${table_t2}.k3 from ${table_t2} where ${table_t2}.k2 = ${table_t1}.k2) or k1 < 10 order by k1, k2;
    """

    
    sql "drop table if exists ${table_tbl1};"
    sql "drop table if exists ${table_tbl2};"
    sql "drop table if exists ${table_tbl3};"

    sql """
        CREATE TABLE `${table_tbl1}` (
            `unit_name` varchar(1080) NULL,
            `cur_unit_name` varchar(1080) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`unit_name`)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        CREATE TABLE `${table_tbl2}` (
            `org_code` varchar(150) NOT NULL ,
            `org_name` varchar(300) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`org_code`)
        DISTRIBUTED BY HASH(`org_code`) BUCKETS 4
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        CREATE TABLE `${table_tbl3}` (
            `id` bigint NOT NULL,
            `acntm_name` varchar(500) NULL ,
            `vendor_name` varchar(500) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into ${table_tbl1} (unit_name, cur_unit_name) values
            ('v1', 'o1'),
            ('v2', 'o2'),
            ('v3', 'o3'),
            ('v4', 'o4'),
            ('v5', 'o5'),
            (null, 'o1'),
            ('v1', 'o1'),
            ('v2', 'o2'),
            ('v3', 'o3'),
            ('v4', 'o4'),
            ('v5', 'o5'),
            (null, 'o1'),
            (null, 'o2'),
            (null, 'o3'),
            (null, 'o4'),
            (null, 'o5'),
            ('v1', 'o1'),
            ('v2', 'o2'),
            ('v3', 'o3'),
            ('v4', 'o4'),
            ('v5', 'o5');
    """

    sql """
        insert into ${table_tbl2}(org_code, org_name) values
            ('v1', 'o1'),
            ('v2', 'o2'),
            ('v3', 'o3'),
            ('v4', 'o4'),
            ('v5', 'o5'),
            ('v1', null),
            ('v2', null),
            ('v3', null),
            ('v4', null),
            ('v5', null);
    """

    sql """
        insert into ${table_tbl3} (id, vendor_name, acntm_name)
            values(1, 'o1', 'v1'),
            (2, 'o2', 'v2'),
            (3, 'o3', 'v3'),
            (4, 'o4', 'v4'),
            (5, 'o5', 'v5'),
            (6, null, 'v1'),
            (7, null, 'v2'),
            (8, null, 'v3'),
            (9, null, 'v4'),
            (10, null, 'v5');
    """

    sql " analyze table ${table_tbl1} with sync;"
    sql " analyze table ${table_tbl2} with sync;"
    sql " analyze table ${table_tbl3} with sync;"

    sql "set disable_join_reorder=0;"
    qt_test_right_semi_mark_join """
        select
            ${table_tbl3}.id,
            ${table_tbl3}.acntm_name,
            ${table_tbl3}.vendor_name,
            ${table_tbl3}.vendor_name in (
                select
                    ${table_tbl1}.unit_name
                from
                    ${table_tbl2}
                    join ${table_tbl1} on ${table_tbl1}.cur_unit_name = ${table_tbl2}.org_name
                where
                    ${table_tbl2}.org_code = ${table_tbl3}.acntm_name
            ) v1,
            ${table_tbl3}.vendor_name not in (
                select
                    ${table_tbl1}.unit_name
                from
                    ${table_tbl2}
                    join ${table_tbl1} on ${table_tbl1}.cur_unit_name = ${table_tbl2}.org_name
                where
                    ${table_tbl2}.org_code = ${table_tbl3}.acntm_name
            ) v2
        from
            ${table_tbl3} order by 1,2,3,4,5;
    """

    sql "set disable_join_reorder=1;"
    qt_test_right_semi_mark_join_2 """
        select
            ${table_tbl3}.id,
            ${table_tbl3}.acntm_name,
            ${table_tbl3}.vendor_name,
            ${table_tbl3}.vendor_name in (
                select
                    ${table_tbl1}.unit_name
                from
                    ${table_tbl2}
                    join ${table_tbl1} on ${table_tbl1}.cur_unit_name = ${table_tbl2}.org_name
                where
                    ${table_tbl2}.org_code = ${table_tbl3}.acntm_name
            ) v1,
            ${table_tbl3}.vendor_name not in (
                select
                    ${table_tbl1}.unit_name
                from
                    ${table_tbl2}
                    join ${table_tbl1} on ${table_tbl1}.cur_unit_name = ${table_tbl2}.org_name
                where
                    ${table_tbl2}.org_code = ${table_tbl3}.acntm_name
            ) v2
        from
            ${table_tbl3} order by 1,2,3,4,5;
    """

    sql "set disable_join_reorder=0;"
    qt_test_right_semi_mark_join_no_null """
        select
            ${table_tbl3}.id,
            ${table_tbl3}.acntm_name,
            ${table_tbl3}.vendor_name,
            ${table_tbl3}.vendor_name in (
                select
                    ${table_tbl1}.unit_name
                from
                    ${table_tbl2}
                    join ${table_tbl1} on ${table_tbl1}.cur_unit_name = ${table_tbl2}.org_name
                where
                    ${table_tbl2}.org_code = ${table_tbl3}.acntm_name
                    and ${table_tbl1}.unit_name is not null
            ) v1,
            ${table_tbl3}.vendor_name not in (
                select
                    ${table_tbl1}.unit_name
                from
                    ${table_tbl2}
                    join ${table_tbl1} on ${table_tbl1}.cur_unit_name = ${table_tbl2}.org_name
                where
                    ${table_tbl2}.org_code = ${table_tbl3}.acntm_name
                    and ${table_tbl1}.unit_name is not null
            ) v2
        from
            ${table_tbl3} order by 1,2,3,4,5;
    """

    sql "set disable_join_reorder=1;"
    qt_test_right_semi_mark_join_no_null_2 """
        select
            ${table_tbl3}.id,
            ${table_tbl3}.acntm_name,
            ${table_tbl3}.vendor_name,
            ${table_tbl3}.vendor_name in (
                select
                    ${table_tbl1}.unit_name
                from
                    ${table_tbl2}
                    join ${table_tbl1} on ${table_tbl1}.cur_unit_name = ${table_tbl2}.org_name
                where
                    ${table_tbl2}.org_code = ${table_tbl3}.acntm_name
                    and ${table_tbl1}.unit_name is not null
            ) v1,
            ${table_tbl3}.vendor_name not in (
                select
                    ${table_tbl1}.unit_name
                from
                    ${table_tbl2}
                    join ${table_tbl1} on ${table_tbl1}.cur_unit_name = ${table_tbl2}.org_name
                where
                    ${table_tbl2}.org_code = ${table_tbl3}.acntm_name
                    and ${table_tbl1}.unit_name is not null
            ) v2
        from
            ${table_tbl3} order by 1,2,3,4,5;
    """
}
