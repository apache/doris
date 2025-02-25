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

suite("test_from_iso8601_date") {

    def dbName = "test_from_iso8601_date"
    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE $dbName"


    qt_test_31 """SELECT from_iso8601_date('0000');"""         // 0000-01-01
    qt_test_32 """SELECT from_iso8601_date('0001');"""         // 0001-01-01
    qt_test_33 """SELECT from_iso8601_date('1900');"""         // 1900-01-01
    qt_test_34 """SELECT from_iso8601_date('1970');"""         // 1970-01-01
    qt_test_35 """SELECT from_iso8601_date('9999');"""         // 9999-01-01

    qt_test_36 """SELECT from_iso8601_date('0000-01-01');"""   // 0000-01-01
    qt_test_37 """SELECT from_iso8601_date('0000-02-28');"""   // 0000-02-28
    qt_test_38 """SELECT from_iso8601_date('0001-02-28');"""   // 0001-02-28
    qt_test_39 """SELECT from_iso8601_date('1900-02-28');"""   // 1900-02-28
    qt_test_40 """SELECT from_iso8601_date('1970-01-01');"""   // 1970-01-01
    qt_test_41 """SELECT from_iso8601_date('9999-12-31');"""   // 9999-12-31

    qt_test_42 """SELECT from_iso8601_date('00000228');"""     // 0000-02-28
    qt_test_43 """SELECT from_iso8601_date('00010228');"""     // 0001-02-28
    qt_test_44 """SELECT from_iso8601_date('19000228');"""     // 1900-02-28
    qt_test_45 """SELECT from_iso8601_date('19700101');"""     // 1970-01-01
    qt_test_46 """SELECT from_iso8601_date('99991231');"""     // 9999-12-31

    qt_test_47 """SELECT from_iso8601_date('0000-01');"""      // 0000-01-01
    qt_test_48 """SELECT from_iso8601_date('0000-02');"""      // 0000-02-01
    qt_test_49 """SELECT from_iso8601_date('0001-03');"""      // 0001-03-01
    qt_test_50 """SELECT from_iso8601_date('1900-03');"""      // 1900-03-01
    qt_test_51 """SELECT from_iso8601_date('1970-01');"""      // 1970-01-01
    qt_test_52 """SELECT from_iso8601_date('9999-12');"""      // 9999-12-01

    qt_test_53 """SELECT from_iso8601_date('0000-W01');"""     // 0000-01-03
    qt_test_54 """SELECT from_iso8601_date('0000-W09');"""     // 0000-02-28
    qt_test_55 """SELECT from_iso8601_date('0001-W09');"""     // 0001-02-26
    qt_test_56 """SELECT from_iso8601_date('1900-W08');"""     // 1900-02-19
    qt_test_57 """SELECT from_iso8601_date('1970-W01');"""     // 1969-12-29
    qt_test_58 """SELECT from_iso8601_date('9999-W52');"""     // 9999-12-27


    qt_test_59 """SELECT from_iso8601_date('0000-W01-1');"""   // 0000-01-03
    qt_test_60 """SELECT from_iso8601_date('0000-W09-6');"""   // 0000-03-04          0000-03-05 
    qt_test_61 """SELECT from_iso8601_date('0001-W09-6');"""   // 0001-03-03
    qt_test_62 """SELECT from_iso8601_date('1900-W08-7');"""   // 1900-02-25
    qt_test_63 """SELECT from_iso8601_date('1970-W01-1');"""   // 1969-12-29
    qt_test_64 """SELECT from_iso8601_date('9999-W52-5');"""   // 9999-12-31




    qt_test_65 """SELECT from_iso8601_date('0000-059');"""     // 0000-02-28
    qt_test_66 """SELECT from_iso8601_date('0001-060');"""     // 0001-03-01
    qt_test_67 """SELECT from_iso8601_date('1900-059');"""     // 1900-02-28
    qt_test_68 """SELECT from_iso8601_date('1970-001');"""     // 1970-01-01
    qt_test_69 """SELECT from_iso8601_date('9999-365');"""     // 9999-12-31

    qt_test_70 """SELECT from_iso8601_date('0000-060');"""     // 0000-02-29         0000-03-01
    qt_test_71 """SELECT from_iso8601_date('0000-061');"""     // 0000-03-01         0000-03-02
    qt_test_72 """SELECT from_iso8601_date('0000-062');"""     // 0000-03-02         0000-03-03

    qt_test_73 """SELECT from_iso8601_date('0000-02-29');"""   // 0000-02-29         NULL 
    qt_test_74 """SELECT from_iso8601_date('0000-03-01');"""   // 0000-03-01
    qt_test_75 """SELECT from_iso8601_date('0001-02-29');"""   // NULL       
    qt_test_76 """SELECT from_iso8601_date('0001-03-01');"""   // 0001-03-01

    qt_test_77 """SELECT from_iso8601_date('1900-02-29');"""   // NULL
    qt_test_78 """SELECT from_iso8601_date('1900-03-01');"""   // 1900-03-01
    qt_test_79 """SELECT from_iso8601_date('1970-02-28');"""   // 1970-02-28
    qt_test_80 """SELECT from_iso8601_date('1970-03-01');"""   // 1970-03-01
    qt_test_81 """SELECT from_iso8601_date('9999-02-29');"""   // NULL
    qt_test_82 """SELECT from_iso8601_date('9999-03-01');"""   // 9999-03-01

    qt_test_83 """SELECT from_iso8601_date('2009-W01-1');"""  // 2008-12-29
    qt_test_84 """SELECT from_iso8601_date('2009-W53-7')"""          // 2010-01-03

    qt_test_85 """SELECT from_iso8601_date(NULL);"""
    qt_test_86 """SELECT from_iso8601_date(nullable("2023-04-05"));"""


    qt_test_101 """ SELECT from_iso8601_date("20230");"""
    qt_test_102 """ SELECT from_iso8601_date("0230");"""
    qt_test_103 """ SELECT from_iso8601_date("202334");"""
    qt_test_104 """ SELECT from_iso8601_date("902030");"""
    qt_test_105 """ SELECT from_iso8601_date("2003--33");"""
    qt_test_106 """ SELECT from_iso8601_date("abcdd");"""
    qt_test_107 """ SELECT from_iso8601_date("7855462");"""
    qt_test_108 """ SELECT from_iso8601_date("010-03-02");"""
    qt_test_109 """ SELECT from_iso8601_date("2021/03/04");"""
    qt_test_110 """ SELECT from_iso8601_date("2121W1");"""
    qt_test_111 """ SELECT from_iso8601_date("2121W00");"""
    qt_test_112 """ SELECT from_iso8601_date("ssss");"""
    qt_test_113 """ SELECT from_iso8601_date("5555555");"""
    qt_test_114 """ SELECT from_iso8601_date("555500");"""
    qt_test_115 """ SELECT from_iso8601_date("5555001");"""
    qt_test_116 """ SELECT from_iso8601_date("5555W001");"""
    qt_test_116 """ SELECT from_iso8601_date("5555-001");"""
    qt_test_117 """ SELECT from_iso8601_date("5555-W001");"""
    qt_test_118 """ SELECT from_iso8601_date("555-001");"""
    qt_test_119 """ SELECT from_iso8601_date("99999-02-01");"""
    qt_test_120 """ SELECT from_iso8601_date("");"""



    sql """
    CREATE TABLE IF NOT EXISTS `tb2` (
        `k0` int null comment "",

        `k1` string,
        `k2` char(10),
        `k3` varchar(10),

        `k11` string  not null ,
        `k22` char(10) not null ,
        `k33` varchar(10) not null 

        ) engine=olap
        DISTRIBUTED BY HASH(`k0`) BUCKETS 5 properties("replication_num" = "1")
        """
    sql """insert into tb2 values (1, "2023-02-03","2023-02-03","2023-02-03" ,  "2023-02-03","2023-02-03","2023-02-03" );"""
    sql """insert into tb2 values (2, null,null,null,  "2023-02-03","2023-02-03","2023-02-03" );"""

    qt_test_87  """ select from_iso8601_date(k1),from_iso8601_date(k2),from_iso8601_date(k3),from_iso8601_date(k11),from_iso8601_date(k22),from_iso8601_date(k33) from tb2 order by  k0;"""
    qt_test_88 """ select from_iso8601_date(nullable(k1)),from_iso8601_date(k2),from_iso8601_date(k3),from_iso8601_date(nullable(k11)),from_iso8601_date(k22),from_iso8601_date(k33) from tb2 order by  k0; """ 
    qt_test_89 """ select from_iso8601_date(NULL)   from tb2 order by  k0; """ 



    sql """ drop table tb2 """ 

}