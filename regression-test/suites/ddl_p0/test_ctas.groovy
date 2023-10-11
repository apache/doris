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

suite("test_ctas") {
    def dbname = "test_ctas";
    sql """drop database if exists ${dbname}"""
    sql """create database ${dbname}"""
    sql """use ${dbname}"""
    sql """clean label from ${dbname}"""

    try {
        sql """
    CREATE TABLE IF NOT EXISTS `test_ctas` (
      `test_varchar` varchar(150) NULL,
      `test_text` text NULL,
      `test_datetime` datetime NULL,
      `test_default_timestamp` datetime DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=OLAP
    UNIQUE KEY(`test_varchar`)
    DISTRIBUTED BY HASH(`test_varchar`) BUCKETS 3
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    )
    """

        sql """ INSERT INTO test_ctas(test_varchar, test_text, test_datetime) VALUES ('test1','test11','2022-04-27 16:00:33'),('test2','test22','2022-04-27 16:00:54') """

        sql """
    CREATE TABLE IF NOT EXISTS `test_ctas1`
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    ) as select * from test_ctas;
    """

        def res = sql """SHOW CREATE TABLE `test_ctas1`"""
        assertTrue(res.size() != 0)

        qt_select """select count(*) from test_ctas1"""

        sql """
    CREATE TABLE IF NOT EXISTS `test_ctas2`
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    ) as select test_varchar, lpad(test_text,10,'0') as test_text, test_datetime, test_default_timestamp from test_ctas;
    """

        res = sql """SHOW CREATE TABLE `test_ctas2`"""
        assertTrue(res.size() != 0)

        qt_select """select count(*) from test_ctas2"""

        sql """
    CREATE TABLE test_ctas_json_object (
    c1 varchar(10) NULL,
    v1 DECIMAL(18,6) NULL COMMENT "",
    v2 DECIMAL(18,6) NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(c1)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(c1) BUCKETS 3
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    )
    """

        sql """ insert into test_ctas_json_object(c1, v1, v2) values ('r1', 1.1, 1.2),('r2', 2.1, 2.2) """

        sql """
    CREATE TABLE IF NOT EXISTS `test_ctas_json_object1`
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    ) as select c1, json_object('title', 'Amount', 'value', v1) from test_ctas_json_object;
    """

        qt_select """select * from test_ctas_json_object1 order by c1;"""

        sql """create table a properties("replication_num"="1") as select null as c;"""
        test {
            sql "select * from a"
            result([[null]])
        }

        test {
            sql """show load from ${dbname}"""
            rowNum 6
        }

        sql """
            create table if not exists test_tbl_81748325
            (
                `col1`        varchar(66) not null ,
                `col2`      bigint      not null ,
                `col3`  varchar(66) not null ,
                `col4`  varchar(42) not null ,
                `col5` bigint      not null ,
                `col6`         bigint      not null ,
                `col7`        datetime    not null ,
                `col8`            varchar(66) not null, 
                `col9`            varchar(66)          ,
                `col10`            varchar(66)          ,
                `col11`            varchar(66)          ,
                `col12`              text                 
            )
            UNIQUE KEY (`col1`,`col2`,`col3`,`col4`,`col5`,`col6`)
            DISTRIBUTED BY HASH(`col4`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """

        sql """
            create table `test_tbl_3156019`
            UNIQUE KEY (col4,col3,from_address,to_address)
            DISTRIBUTED BY HASH (col4) BUCKETS 1
            PROPERTIES (
                  "replication_allocation" = "tag.location.default: 1"
            )
           as 
            select
                col4                                as col4,
                col3                                as col3,
                concat('0x', substring(col9, 27))             as from_address,
                concat('0x', substring(col10, 27))             as to_address,
                col7                                      as date_time,
                now()                                           as update_time,
                '20230318'                                      as pt,
                col8                                            as amount
            from test_tbl_81748325
            where col4 = '43815251'
              and substring(col8, 1, 10) = '1451601';
        """

        sql """
            DROP TABLE IF EXISTS tbl_3210581
        """

        sql """
            CREATE TABLE tbl_3210581 (col1 varchar(11451) not null, col2 int not null, col3 int not null)
            UNIQUE KEY(`col1`)
            DISTRIBUTED BY HASH(col1)
            BUCKETS 3
            PROPERTIES(
                "replication_num"="1"
            )
        """

        sql """
            DROP TABLE IF EXISTS ctas_113815;
        """

        sql """
            create table ctas_113815
            PROPERTIES('replication_num' = '1')
            as 
            select     group_concat(col1 ORDER BY col1) from     `tbl_3210581`
            group by `col2`;
        """

    } finally {
        sql """ DROP TABLE IF EXISTS test_ctas """

        sql """ DROP TABLE IF EXISTS test_ctas1 """

        sql """ DROP TABLE IF EXISTS test_ctas2 """

        sql """ DROP TABLE IF EXISTS test_ctas_json_object """
        
        sql """ DROP TABLE IF EXISTS test_ctas_json_object1 """

        sql """drop table if exists a"""

        sql """DROP TABLE IF EXISTS test_tbl_81748325"""

        sql """DROP TABLE IF EXISTS test_tbl_3156019"""

        sql """
            DROP TABLE IF EXISTS tbl_3210581
        """

        sql """
            DROP TABLE IF EXISTS ctas_113815
        """
    }
    
    try {
        sql '''create table a (
                id int not null,
                        name varchar(20) not null
        )
        distributed by hash(id) buckets 4
        properties (
                "replication_num"="1"
        );
        '''

        sql '''create table b (
                id int not null,
                        age int not null
        )
        distributed by hash(id) buckets 4
        properties (
                "replication_num"="1"
        );
        '''

        sql 'insert into a values(1, \'ww\'), (2, \'zs\');'
        sql 'insert into b values(1, 22);'

        sql 'set enable_nereids_planner=false'

        sql 'create table c properties("replication_num"="1") as select b.id, a.name, b.age from a left join b on a.id = b.id;'
        
        String descC = sql 'desc c'
        assertTrue(descC.contains('Yes'))
        String descB = sql 'desc b'
        assertTrue(descB.contains('No'))

        sql '''create table test_date_v2 
        properties (
                "replication_num"="1"
        ) as select to_date('20250829');
        '''
        String desc = sql 'desc test_date_v2'
        assertTrue(desc.contains('Yes'))
    } finally {
        sql 'drop table a'
        sql 'drop table b'
        sql 'drop table c'
        sql 'drop table test_date_v2'
    }
}

