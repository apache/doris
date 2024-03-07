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

suite("join_order") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    sql """ drop table if exists outerjoin_A_order;"""
    sql """
        create table outerjoin_A_order ( a1 bigint not null, a2 bigint not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a1) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """
    sql """ drop table if exists outerjoin_B_order;"""
    sql """
        create table outerjoin_B_order ( b int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(b) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """
    sql """ drop table if exists outerjoin_C_order;"""
    sql """
        create table outerjoin_C_order ( c int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(c) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """
    sql """ drop table if exists outerjoin_D_order;"""
    sql """
        create table outerjoin_D_order ( d1 int not null, d2 int not null, d3 int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(d1) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """
    sql """ drop table if exists outerjoin_E_order;"""
    sql """
        create table outerjoin_E_order ( e1 int not null, e2 int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(e1) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """insert into outerjoin_A_order values( 1,2 );"""
    sql """insert into outerjoin_B_order values( 1 );"""
    sql """insert into outerjoin_C_order values( 1 );"""
    sql """insert into outerjoin_D_order values( 1,2,3 );"""
    sql """insert into outerjoin_E_order values( 1,2 );"""

    qt_sql"""SELECT count(*)
            FROM outerjoin_A_order t1
            LEFT JOIN outerjoin_D_order dcbc
                ON t1.a1 = dcbc.d1
            LEFT JOIN outerjoin_C_order dcso
                ON dcbc.d2 = dcso.c
            LEFT JOIN outerjoin_B_order dcii
                ON t1.a2 = dcii.b
            LEFT JOIN outerjoin_E_order dcssm
                ON dcii.b = dcssm.e1
                    AND dcbc.d3 = dcssm.e2;
        """

    sql 'set disable_join_reorder=true;'
    explain {
        sql("select * from outerjoin_A_order, outerjoin_B_order, outerjoin_C_order where outerjoin_A_order.a1 = outerjoin_C_order.c and outerjoin_B_order.b = outerjoin_C_order.c;")
        contains "CROSS JOIN"
    }

    sql 'set disable_join_reorder=false;'
    explain {
        sql("select * from outerjoin_A_order, outerjoin_B_order, outerjoin_C_order where outerjoin_A_order.a1 = outerjoin_C_order.c and outerjoin_B_order.b = outerjoin_C_order.c;")
        notContains "CROSS JOIN"
    }

    qt_sql2 """SELECT 
                    subq_0.`c1`,
                    subq_0.`c0`
                FROM 
                    (SELECT ref_1.b AS c0,
                        ref_7.a1 AS c1
                    FROM outerjoin_B_order AS ref_1
                    INNER JOIN outerjoin_A_order AS ref_7
                        ON (true) order by ref_7.a2) AS subq_0 order by 1, 2;"""
   
    sql """drop table if exists test_table_t1;"""
    sql """drop table if exists test_table_t2;"""
    sql """drop table if exists test_table_t3;"""

    sql """ create table test_table_t1
                    (k1 bigint, k2 bigint)
                    ENGINE=OLAP
            DUPLICATE KEY(k1, k2)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(k2) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            );"""

    sql """ create table test_table_t2
                    (k1 int not null, k2 varchar(128), k3 bigint, v1 bigint, v2 bigint)
                    ENGINE=OLAP
            DUPLICATE KEY(k1, k2)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(k2) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            );"""

    sql """ create table test_table_t3
                    (k1 bigint, k2 bigint)
                    ENGINE=OLAP
            DUPLICATE KEY(k1, k2)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(k2) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            );"""

    sql """insert into test_table_t1 values (1,null);"""
    sql """insert into test_table_t2 values (1,'abc',2,3,4);"""
    sql """insert into test_table_t3 values (1,null),(1,4), (1,2), (2,3), (2,4), (3,7), (3,9),(null,null);"""

    qt_select1 """SELECT 
                    count( 
                    (SELECT max(`k1`)
                    FROM test_table_t3) )
                        OVER (partition by ref_560.`k2`
                    ORDER BY  ref_560.`k1`) AS c3
                    FROM test_table_t1 AS ref_559
                    RIGHT JOIN test_table_t2 AS ref_560
                        ON (ref_560.`k1` = ref_559.`k1` )
                    WHERE ref_559.`k2` is null;
                    """

    sql """drop table if exists test_table_t1;"""
    sql """drop table if exists test_table_t2;"""
    sql """drop table if exists test_table_t3;"""

    sql """
        drop table if exists table_3_undef_undef;
    """

    sql """
        drop table if exists table_21_undef_undef;
    """

    sql """
        drop table if exists table_22_undef_undef;
    """

    sql """
        create table table_3_undef_undef (
            `pk` int,
            `col_int_undef_signed` int  ,
            `col_varchar_10__undef_signed` varchar(10)  ,
            `col_varchar_1024__undef_signed` varchar(1024)  
        )engine=olap
        distributed by hash(pk) buckets 10
        properties(
            'replication_num' = '1'
        );
    """

    sql """
        insert into table_3_undef_undef values (0,1,"right","me"),(1,8,'q',"have"),(2,7,'o','e');
    """

    sql """
    create table table_21_undef_undef (
        `pk` int,
        `col_int_undef_signed` int  ,
        `col_varchar_10__undef_signed` varchar(10)  ,
        `col_varchar_1024__undef_signed` varchar(1024)  
    )engine=olap
    distributed by hash(pk) buckets 10
    properties(
        'replication_num' = '1'
    );
    """

    sql """
        insert into table_21_undef_undef values (0,7,'y','b'),(1,null,'j','w'),(2,4,"this","she"),(3,null,'w','r'),(4,1,'i','j'),(5,null,'j','h'),(6,null,'k','h'),(7,null,'o',"when"),(8,null,"out",'n'),(9,8,"out",'h'),(10,null,'c','j'),(11,null,'y','z'),(12,null,'m',"so"),(13,null,"so",'m'),(14,2,"not","and"),(15,0,"about","really"),(16,null,'p',"that's"),(17,4,'z','y'),(18,6,'t','f'),(19,7,'k','w'),(20,9,'a',"for");
    """

    sql """
    create table table_22_undef_undef (
        `pk` int,
        `col_int_undef_signed` int  ,
        `col_varchar_10__undef_signed` varchar(10)  ,
        `col_varchar_1024__undef_signed` varchar(1024)  
    )
    engine=olap
    distributed by hash(pk) buckets 10
    properties(
        'replication_num' = '1'
    );
    """

    sql """
        insert into table_22_undef_undef values (0,null,"can","why"),(1,null,"had","yeah"),(2,null,"ok",'y'),(3,null,"this",'w'),(4,null,'f',"not"),(5,7,'v',"really"),(6,5,"for",'y'),(7,null,'o',"of"),(8,0,'x','q'),(9,null,"about",'h'),(10,8,"you","this"),(11,null,'i','y'),(12,null,"this","who"),(13,4,"see",'h'),(14,8,"because","him"),(15,1,"good",'r'),(16,6,"know","know"),(17,3,"what",'e'),(18,null,'h',"then"),(19,null,'l','z'),(20,4,'l',"i"),(21,null,'f','q');
    """

    order_qt_test_order_with_both_comma_and_join """
        SELECT t1.`pk`
        FROM table_21_undef_undef AS t1,
            table_3_undef_undef AS alias1
            FULL OUTER JOIN table_22_undef_undef AS alias2 ON alias1.`pk` = alias2.`pk`
    """

    sql """
        drop table if exists table_3_undef_undef;
    """

    sql """
        drop table if exists table_21_undef_undef;
    """

    sql """
        drop table if exists table_22_undef_undef;
    """
}
