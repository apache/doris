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

suite("test_insert") {
    // todo: test insert, such as insert values, insert select, insert txn
    sql "show load"
    def test_baseall = "baseall"
    def test_bigtable = "bigtable"
    def insert_tbl = "test_insert_tbl"

    sql """ DROP TABLE IF EXISTS ${insert_tbl}"""
    sql """
     CREATE TABLE ${insert_tbl} (
       `k1` char(5) NULL,
       `k2` int(11) NULL,
       `k3` tinyint(4) NULL,
       `k4` int(11) NULL
     ) ENGINE=OLAP
     DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`)
     COMMENT 'OLAP'
     DISTRIBUTED BY HASH(`k1`) BUCKETS 5
     PROPERTIES (
       "replication_num"="1"
     );
    """

    sql """ 
    INSERT INTO ${insert_tbl}
    SELECT a.k6, a.k3, b.k1
    	, sum(b.k1) OVER (PARTITION BY a.k6 ORDER BY a.k3) AS w_sum
    FROM ${test_baseall} a
    	JOIN ${test_bigtable} b ON a.k1 = b.k1 + 5
    ORDER BY a.k6, a.k3, a.k1, w_sum
    """

    qt_sql1 "select * from ${insert_tbl} order by 1, 2, 3, 4"

    def insert_tbl_dft = "test_insert_dft_tbl"
    sql """ DROP TABLE IF EXISTS ${insert_tbl_dft}"""
    
    // `k7` should be float type, and bug exists now, https://github.com/apache/doris/pull/20867
    // `k9` should be char(16), and bug exists now as error msg raised:"can not cast from origin type TINYINT to target type=CHAR(16)" when doing insert
    // "`k13` datetime default CURRENT_TIMESTAMP" might have cast error in strict mode when doing insert:
    // [INTERNAL_ERROR]Invalid value in strict mode for function CAST, source column String, from type String to type DateTimeV2
    sql """
        CREATE TABLE ${insert_tbl_dft} (
            `k1` boolean default "true",
            `k2` tinyint default "10",
            `k3` smallint default "10000",
            `k4` int default "10000000",
            `k5` bigint default "92233720368547758",
            `k6` largeint default "19223372036854775807",
            	  
            `k8` double default "3.14159",

            `k10` varchar(64) default "hello world, today is 15/06/2023",
            `k11` date default "2023-06-15",
            `k12` datetime default "2023-06-15 16:10:15" 
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5
        PROPERTIES (
            "replication_num"="1"
        );
    """
    
    sql """ set enable_nereids_planner=true """
    sql """ set enable_nereids_dml=true """
    sql """ insert into ${insert_tbl_dft} values() """

    sql """ set enable_nereids_planner=false """
    sql """ set enable_nereids_dml=false """
    sql """ insert into ${insert_tbl_dft} values() """
    
    qt_select """ select k1, k2, k3, k4, k5, k6, k8, k10, k11, k12 from ${insert_tbl_dft} """
    
    sql 'drop table if exists t3'
    sql '''
        create table t3 (
            id int default "10"
        ) distributed by hash(id) buckets 13
        properties(
            'replication_num'='1'
        );
    '''
    
    sql 'insert into t3 values(default)'
    
    test {
        sql 'select * from t3'
        result([[10]])
    }

    def tableName = "test_insert_into_error"
    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
        `gjjzjid` varchar(640) NULL,
        `id` varchar(1000) NULL,
        `gjjczbs` varchar(1000) NULL,
        `gjjtbsj` datetime(6) NULL,
        `jgsxmlbm` varchar(1000) NULL,
        `jgsxjcssqdbm` varchar(1000) NULL,
        `jcxwmc` varchar(1000) NULL,
        `jcxwbh` varchar(1000) NULL,
        `ssjg` varchar(1000) NULL,
        `ssjgbm` varchar(1000) NULL,
        `swtbm` varchar(1000) NULL,
        `swtbmbm` varchar(1000) NULL,
        `xzxdrmc` varchar(1000) NULL,
        `xzxdrxz` varchar(1000) NULL,
        `xzxdrzjlx` varchar(1000) NULL,
        `xzxdrbm` varchar(1000) NULL,
        `qyid` varchar(1000) NULL,
        `zcdz` varchar(1000) NULL,
        `xzqhdm` varchar(1000) NULL,
        `jcxs` varchar(1000) NULL,
        `jclb` varchar(1000) NULL,
        `jcjggb` varchar(1000) NULL,
        `jlxjy` varchar(1000) NULL,
        `jcsj` varchar(1000) NULL,
        `bsry` varchar(1000) NULL,
        `bssj` varchar(1000) NULL,
        `qylxr` varchar(1000) NULL,
        `qylxfs` varchar(1000) NULL,
        `jcjbqk` text NULL,
        `yzqxsl` varchar(1000) NULL,
        `zyqxsl` varchar(1000) NULL,
        `ybqxsl` varchar(1000) NULL,
        `wdyqxsl` varchar(1000) NULL,
        `qtxysmdwt` varchar(1000) NULL,
        `bz` varchar(1000) NULL,
        `cjr` varchar(1000) NULL,
        `cjsj` varchar(1000) NULL,
        `xgr` varchar(1000) NULL,
        `xgsj` varchar(1000) NULL,
        `pid` varchar(1000) NULL,
        `zzjgid` varchar(1000) NULL,
        `sfyyjcxwbh` varchar(1000) NULL,
        `jgsxmlmc` varchar(1000) NULL,
        `jcsjjs` varchar(1000) NULL,
        `tcqy` varchar(1000) NULL,
        `zhpdjl` varchar(1000) NULL,
        `cljy` varchar(1000) NULL,
        `lhjcbm` varchar(1000) NULL,
        `lhjcbmbm` varchar(1000) NULL,
        `sfsyzglshxcjc` varchar(1000) NULL,
        `yjcxwbh` varchar(1000) NULL,
        `ssjgcj` varchar(1000) NULL,
        `ssjgxzqh` varchar(1000) NULL,
        `sfdm` varchar(1000) NULL,
        `shxydmnb` varchar(1000) NULL,
        `thyy` varchar(1000) NULL,
        `sqhtsj` varchar(1000) NULL,
        `tjsj` varchar(1000) NULL,
        `bsrylxcf` varchar(1000) NULL,
        `sjly` varchar(1000) NULL,
        `sjpzlb` varchar(1000) NULL,
        `sjpzlbqt` varchar(1000) NULL,
        `rwly` varchar(1000) NULL,
        `sfsjtc` varchar(1000) NULL,
        `fclj` varchar(1000) NULL,
        `sfsjfc` varchar(1000) NULL,
        `jgdxlxs` varchar(1000) NULL,
        `jcsjfw` varchar(1000) NULL,
        `jcsjqtfw` varchar(1000) NULL,
        `yyxsjcbcxx` varchar(1000) NULL,
        `sftyksld` varchar(1000) NULL,
        `xzxdrxkzbh` varchar(1000) NULL,
        `ypscqyxkzbh` varchar(1000) NULL,
        `jcfwdl` varchar(1000) NULL,
        `jcsjqknr` varchar(1000) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`gjjzjid`)
        COMMENT '信息表'
        DISTRIBUTED BY HASH(`gjjzjid`) BUCKETS AUTO
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "function_column.sequence_col" = "GJJTBSJ",
        "enable_single_replica_compaction" = "false"
        );
        """

        try {
            sql """
            INSERT INTO ${tableName} (`gjjzjid`,`id`,`gjjczbs`,`gjjtbsj`,`jgsxmlbm`,`jgsxjcssqdbm`,`jcxwmc`,`jcxwbh`,`ssjg`,`ssjgbm`,`swtbm`,`swtbmbm`,`xzxdrmc`,`xzxdrxz`,`xzxdrzjlx`,`xzxdrbm`,`qyid`,`zcdz`,`xzqhdm`,`jcxs`,`jclb`,`jcjggb`,`jlxjy`,`jcsj`,`bsry`,`bssj`,`qylxr`,`qylxfs`,`jcjbqk`,`yzqxsl`,`zyqxsl`,`ybqxsl`,`wdyqxsl`,`qtxysmdwt`,`bz`,`cjr`,`cjsj`,`xgr`,`xgsj`,`pid`,`zzjgid`,`sfyyjcxwbh`,`jgsxmlmc`,`jcsjjs`,`tcqy`,`zhpdjl`,`cljy`,`lhjcbm`,`lhjcbmbm`,`sfsyzglshxcjc`,`yjcxwbh`,`ssjgcj`,`ssjgxzqh`,`sfdm`,`shxydmnb`,`thyy`,`sqhtsj`,`tjsj`,`bsrylxcf`,`sjly`,`sjpzlb`,`sjpzlbqt`,`rwly`,`sfsjtc`,`fclj`,`sfsjfc`,`jgdxlxs`,`jcsjfw`,`jcsjqtfw`,`yyxsjcbcxx`,`sftyksld`,`xzxdrxkzbh`,`ypscqyxkzbh`,`jcfwdl`,`jcsjqknr`) VALUES('111111111111111','111111111111','I','2023-11-27 18:10:46.000000','11111111','1111111111','测试数据测试','测试数据测试','测试数据测试测试数据测试','111111111111111',null,null,'测试数据测试','02','001','111111111','1111111111111111','地址AAAAAAAAAAA','111111','0201','01','02','1','111111','张三','20230802000000','张三','0991-1234567','检测测试数据AAAA',0,0,2,0,null,null,'a1333caa946a4f1c99536a16b77f7368','20231106161214','a1333caa946a4f1c99536a16b77f7368','20231106164803','A3A05AAA0A470921127B3653913806F4','2182c9de517e4722b944f6f29bff08a7','1','测试数据bbbbbbbb','20230727000000','0','1','aaa',null,null,'0',null,'2','650000','AAAAAAAAAAAAA','AAAAAAAAAAAAAAA',null,null,'20231106164803','0991-1234567',null,'09,11',null,'07','0',null,'0','01,04','a56,a57',null,null,'0','测试数据','测试数据','01',null);
            """
        } catch(Exception e) {
            logger.info(e.getMessage())
            assertTrue(e.getMessage().contains("output_tuple_slot_num 78 should be equal to output_expr_num 77"))
        }
    } finally {
        sql """ DROP TABLE IF EXISTS ${tableName} """
    }
}
