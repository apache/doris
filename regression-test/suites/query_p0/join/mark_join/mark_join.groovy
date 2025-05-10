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
    sql "drop table if exists t1;"
    sql "drop table if exists t2;"
    sql """
        create table t1 (
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
        create table t2 (
            k1 int null,
            k2 int null,
            k3 bigint null,
        k4 varchar(100) null
        )
        duplicate key (k1,k2,k3)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
    """

    sql "insert into t1 select 1,1,1,'a';"
    sql "insert into t1 select 2,2,2,'b';"
    sql "insert into t1 select 3,-3,null,'c';"
    sql "insert into t1 select 3,3,null,'c';"

    sql "insert into t2 select 1,1,1,'a';"
    sql "insert into t2 select 2,2,2,'b';"
    sql "insert into t2 select 3,-3,null,'c';"
    sql "insert into t2 select 3,3,null,'c';"

    qt_test """
    select * from t1 where exists (select t2.k3 from t2 where t1.k2 = t2.k2) or k1 < 10 order by k1, k2;
    """
    qt_test """
    select * from t1 where not exists (select t2.k3 from t2 where t1.k2 = t2.k2) or k1 < 10 order by k1, k2;
    """
    qt_test """
    select * from t1 where t1.k1 not in (select t2.k3 from t2 where t2.k2 = t1.k2) or k1 < 10 order by k1, k2;
    """

    sql "drop table if exists tbl1;"
    sql "drop table if exists tbl2;"
    sql "drop table if exists tbl3;"

    sql """
        CREATE TABLE `tbl1` (
            `unit_name` varchar(1080) NULL,
            `cur_unit_name` varchar(1080) NOT NULL,
            `cur_unit` varchar(1080) NOT NULL,
            `sub_unit` varchar(270) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`unit_name`)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        CREATE TABLE `tbl2` (
            `org_code` varchar(150) NOT NULL ,
            `segment` varchar(300) NULL ,
            `org_name` varchar(300) NULL ,
            `is_merged` varchar(3) NULL ,
            `management_level` int NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`org_code`)
        DISTRIBUTED BY HASH(`org_code`) BUCKETS 4
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        CREATE TABLE `tbl3` (
            `id` bigint NOT NULL AUTO_INCREMENT(100000),
            `period_code` varchar(255) NULL,
            `acntm_name` varchar(500) NULL ,
            `vendor_name` varchar(500) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`, `period_code`)
        DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into tbl1 values('u1', 'o1', 'u1', 's1');
    """

    sql """
        insert into tbl2(org_code, segment, org_name, is_merged, management_level) values('1', 's1', 'o1', '是', 2);
    """

    sql """
        insert into tbl3 (id, period_code, vendor_name, acntm_name) values(1, '2025-01-31', 'o1', 'company');
    """

    qt_test_right_semi_mark_join """
        select
            acntm_name,
            vendor_name,
            case
                when 1 = 1
                and vendor_name in (
                    select
                        sub.unit_name
                    from
                    tbl2 a
                        join (
                            select
                                segment,
                                org_name
                            from
                                tbl2
                            where
                                management_level = 2
                                and is_merged = '是'
                        ) b on a.segment = b.segment
                        join tbl1 sub on sub.cur_unit_name = b.org_name
                    where
                        a.org_name = t.acntm_name
                ) then 'Y'
                else 'N'
            end as tmp
        from
            tbl3 t
            join tbl2 co on co.org_name = t.vendor_name
        where
            period_code = '2025-01-31'
            and acntm_name = 'company';
    """
}
