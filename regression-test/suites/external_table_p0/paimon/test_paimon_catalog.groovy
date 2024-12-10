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

suite("test_paimon_catalog", "p0,external,doris,external_docker,external_docker_doris") {

    String file_ctl_name = "paimon_file_catalog";
    String hms_ctl_name = "paimon_hms_catalog";

    // This is only for testing creating catalog
    sql """DROP CATALOG IF EXISTS ${file_ctl_name}"""
    sql """
        CREATE CATALOG ${file_ctl_name} PROPERTIES (
            "type" = "paimon",
            "warehouse" = "hdfs://HDFS8000871/user/paimon",
            "dfs.nameservices"="HDFS8000871",
            "dfs.ha.namenodes.HDFS8000871"="nn1,nn2",
            "dfs.namenode.rpc-address.HDFS8000871.nn1"="172.21.0.1:4007",
            "dfs.namenode.rpc-address.HDFS8000871.nn2"="172.21.0.2:4007",
            "dfs.client.failover.proxy.provider.HDFS8000871"="org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
            "hadoop.username"="hadoop"
        );
    """

    // This is only for testing creating catalog
    sql """DROP CATALOG IF EXISTS ${hms_ctl_name}"""
    sql """
        CREATE CATALOG ${hms_ctl_name} PROPERTIES (
            "type" = "paimon",
            "paimon.catalog.type"="hms",
            "warehouse" = "hdfs://HDFS8000871/user/zhangdong/paimon2",
            "hive.metastore.uris" = "thrift://172.21.0.44:7004",
            "dfs.nameservices"="HDFS8000871",
            "dfs.ha.namenodes.HDFS8000871"="nn1,nn2",
            "dfs.namenode.rpc-address.HDFS8000871.nn1"="172.21.0.1:4007",
            "dfs.namenode.rpc-address.HDFS8000871.nn2"="172.21.0.2:4007",
            "dfs.client.failover.proxy.provider.HDFS8000871"="org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
            "hadoop.username"="hadoop"
        );
    """

    sql """drop catalog ${file_ctl_name}""";
    sql """drop catalog ${hms_ctl_name}""";

    String enabled = context.config.otherConfigs.get("enablePaimonTest")
        if (enabled != null && enabled.equalsIgnoreCase("true")) {
            def qt_all_type = { String table_name ->
                qt_all """select * from ${table_name} order by c1"""
                qt_predict_like_1 """select * from ${table_name} where c13 like '%3%' order by c1"""
                qt_predict_like_2 """select * from ${table_name} where c13 like '13%' order by c1"""
                qt_predict_like_3 """select * from ${table_name} where c13 like '13' order by c1"""
                qt_predict_like_4 """select * from ${table_name} where c13 like '130str' order by c1"""
                qt_predict_like_5 """select * from ${table_name} where c13 like '130str%' order by c1"""
                qt_c1 """select * from ${table_name} where c1=10;"""
                qt_c2 """select * from ${table_name} where c2=2;"""
                qt_c3 """select * from ${table_name} where c3=3;"""
                qt_c4 """select * from ${table_name} where c4=4;"""
                qt_c5 """select * from ${table_name} where c5=5;"""
                qt_c6 """select * from ${table_name} where c6=6;"""
                qt_c7 """select * from ${table_name} where c7=7;"""
                qt_c8 """select * from ${table_name} where c8=8;"""
                qt_c9 """select * from ${table_name} where c9<10;"""
                qt_c10  """select * from ${table_name} where c10=10.1;"""
                qt_c11  """select * from ${table_name} where c11=11.1;"""
                qt_c12  """select * from ${table_name} where c12='2020-02-02';"""
                qt_c13  """select * from ${table_name} where c13='13str';"""
                qt_c14  """select * from ${table_name} where c14='14varchar';"""
                qt_c15  """select * from ${table_name} where c15='a';"""
                qt_c16  """select * from ${table_name} where c16=true;"""
                qt_c18  """select * from ${table_name} where c18='2023-08-13 09:32:38.53';"""
                qt_c101 """select * from ${table_name} where c1 is not null or c2 is not null order by c1"""
            }

            def c19 = """select * from auto_bucket order by user_id;"""
            def c20 = """select * from auto_bucket where dt="b";"""
            def c21 = """select * from auto_bucket where dt="b" and hh="c";"""
            def c22 = """select * from auto_bucket where dt="d";"""
            def c23 = """select * from complex_tab order by c1;"""
            def c24 = """select * from complex_tab where c1=1;"""
            def c25 = """select c3['a_test'], c3['b_test'], c3['bbb'], c3['ccc'] from complex_tab order by c3['a_test'], c3['b_test']"""
            def c26 = """select array_max(c2) c from complex_tab order by c"""
            def c27 = """select * from complex_all order by c1"""
            def c28 = """select array_min(c2) c from complex_all order by c"""
            def c29 = """select array_min(c3) c from complex_all order by c"""
            def c30= """select array_min(c4) c from complex_all order by c"""
            def c31= """select array_min(c5) c from complex_all order by c"""
            def c32= """select array_min(c6) c from complex_all order by c"""
            def c33= """select array_min(c7) c from complex_all order by c"""
            def c34= """select array_min(c8) c from complex_all order by c"""
            def c35= """select array_min(c9) c from complex_all order by c"""
            def c36= """select array_min(c10) c from complex_all order by c"""
            def c37= """select array_max(c11) c from complex_all order by c"""
            def c38= """select array_max(c12) c from complex_all order by c"""
            def c39= """select array_max(c13) c from complex_all order by c"""
            def c40= """select array_size(c14) c from complex_all order by c"""
            def c41= """select array_size(c15) c from complex_all order by c"""
            def c42= """select array_size(c16) c from complex_all order by c"""
            def c43= """select array_max(c17) c from complex_all order by c"""
            def c44= """select array_size(c18) c from complex_all order by c"""
            def c45= """select array_max(c19) c from complex_all order by c"""

            def c46= """select c20[0] c from complex_all order by c"""
            def c47= """select c21[0] c from complex_all order by c"""
            def c48= """select c22[0] c from complex_all order by c"""
            def c49= """select c23[0] c from complex_all order by c"""
            def c50= """select c24[1] c from complex_all order by c"""
            def c51= """select c25[0] c from complex_all order by c"""
            def c52= """select c26[0] c from complex_all order by c"""
            def c53= """select c27[0] c from complex_all order by c"""
            def c54= """select c28[0] c from complex_all order by c"""
            def c55= """select c29[0] c from complex_all order by c"""
            def c56= """select c30[0] c from complex_all order by c"""
            def c57= """select c31[0] c from complex_all order by c"""
            def c58= """select c32[0] c from complex_all order by c"""
            def c59= """select c33[0] c from complex_all order by c"""
            def c60= """select c34[0] c from complex_all order by c"""
            def c61= """select c35[0] c from complex_all order by c"""
            def c62= """select c36[0] c from complex_all order by c"""
            def c63= """select c37[0] c from complex_all order by c"""

            def c64= """select c38[2] c from complex_all order by c"""
            def c65= """select c39[4] c from complex_all order by c"""
            def c66= """select c40[5] c from complex_all order by c"""
            def c67= """select c41[7] c from complex_all order by c"""
            def c68= """select c42[9] c from complex_all order by c"""
            def c69= """select c43[10] c from complex_all order by c"""
            def c70= """select c44[12] c from complex_all order by c"""
            def c71= """select c45[13] c from complex_all order by c"""
            def c72= """select c46[14] c from complex_all order by c"""
            def c73= """select c47[16] c from complex_all order by c"""
            def c74= """select c48[17] c from complex_all order by c"""
            def c75= """select c49[19] c from complex_all order by c"""
            def c76= """select c50[21] c from complex_all order by c"""
            def c77= """select c51[22] c from complex_all order by c"""
            def c78= """select c52[25] c from complex_all order by c"""
            def c79= """select c53[27] c from complex_all order by c"""
            def c80= """select c54[29] c from complex_all order by c"""
            def c81= """select c55[30] c from complex_all order by c"""

            def c82= """select c56[2] c from complex_all order by c"""
            def c83= """select c57[4] c from complex_all order by c"""
            def c84= """select c58[5] c from complex_all order by c"""
            def c85= """select c59[7] c from complex_all order by c"""
            def c86= """select c60[9] c from complex_all order by c"""
            def c87= """select c61[10] c from complex_all order by c"""
            def c88= """select c62[12] c from complex_all order by c"""
            def c89= """select c63[13] c from complex_all order by c"""
            def c90= """select c64[14] c from complex_all order by c"""
            def c91= """select c65[16] c from complex_all order by c"""
            def c92= """select c66[17] c from complex_all order by c"""
            def c93= """select c67[19] c from complex_all order by c"""
            def c94= """select c68[21] c from complex_all order by c"""
            def c95= """select c69[22] c from complex_all order by c"""
            def c96= """select c70[25] c from complex_all order by c"""
            def c97= """select c71[27] c from complex_all order by c"""
            def c98= """select c72[29] c from complex_all order by c"""
            def c99= """select c73[30] c from complex_all order by c"""

            def c100= """select * from array_nested order by c1;"""

            def c102= """select * from row_native_test order by id;"""
            def c103= """select * from row_jni_test order by id;"""

            def c104= """select * from deletion_vector_orc;"""
            def c105= """select * from deletion_vector_parquet;"""
            def c106= """ select * from tb_with_upper_case """
            def c107= """ select id from tb_with_upper_case where id > 1 """
            def c108= """ select id from tb_with_upper_case where id = 1 """
            def c109= """ select id from tb_with_upper_case where id < 1 """

            def c110 = """select count(*) from deletion_vector_orc;"""
            def c111 = """select count(*) from deletion_vector_parquet;"""
            def c112 = """select count(*) from deletion_vector_orc where id > 2;"""
            def c113 = """select count(*) from deletion_vector_parquet where id > 2;"""
            def c114 = """select * from deletion_vector_orc where id > 2;"""
            def c115 = """select * from deletion_vector_parquet where id > 2;"""

            String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
            String catalog_name = "ctl_test_paimon_catalog"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type" = "paimon",
                "paimon.catalog.type"="filesystem",
                "warehouse" = "hdfs://${externalEnvIp}:${hdfs_port}/user/doris/paimon1"
            );"""
            sql """use `${catalog_name}`.`db1`"""

            def test_cases = { String force, String cache ->
                sql """ set force_jni_scanner=${force} """
                sql """ set enable_file_cache=${cache} """
                qt_all_type("all_table")
                qt_all_type("all_table_with_parquet")

                qt_c19 c19
                qt_c20 c20
                qt_c21 c21
                qt_c22 c22
                qt_c23 c23
                qt_c24 c24
                qt_c25 c25
                qt_c26 c26
                qt_c27 c27
                qt_c28 c28
                qt_c29 c29
                qt_c30 c30
                qt_c31 c31
                qt_c32 c32
                qt_c33 c33
                qt_c34 c34
                qt_c35 c35
                qt_c36 c36
                qt_c37 c37
                qt_c38 c38
                qt_c39 c39
                qt_c40 c40
                qt_c41 c41
                qt_c42 c42
                qt_c43 c43
                qt_c44 c44
                qt_c45 c45
                qt_c46 c46
                qt_c47 c47
                qt_c48 c48
                qt_c49 c49
                qt_c50 c50
                qt_c51 c51
                qt_c52 c52
                qt_c53 c53
                qt_c54 c54
                qt_c55 c55
                qt_c56 c56
                qt_c57 c57
                qt_c58 c58
                qt_c59 c59
                qt_c60 c60
                qt_c61 c61
                qt_c62 c62
                qt_c63 c63
                qt_c64 c64
                qt_c65 c65
                qt_c66 c66
                qt_c67 c67
                qt_c68 c68
                qt_c69 c69
                qt_c70 c70
                qt_c71 c71
                qt_c72 c72
                qt_c73 c73
                qt_c74 c74
                qt_c75 c75
                qt_c76 c76
                qt_c77 c77
                qt_c78 c78
                qt_c79 c79
                qt_c80 c80
                qt_c81 c81
                qt_c82 c82
                qt_c83 c83
                qt_c84 c84
                qt_c85 c85
                qt_c85 c86
                qt_c86 c87
                qt_c86 c88
                qt_c89 c89
                qt_c90 c90
                qt_c91 c91
                qt_c92 c92
                qt_c93 c93
                qt_c94 c94
                qt_c95 c95
                qt_c96 c96
                qt_c97 c97
                qt_c98 c98
                qt_c99 c99
                qt_c100 c100
                qt_c102 c102
                qt_c103 c103
                qt_c104 c104
                qt_c105 c105
                qt_c106 c106
                qt_c107 c107
                qt_c108 c108
                qt_c109 c109

                qt_c110 c110
                qt_c111 c111
                qt_c112 c112
                qt_c113 c113
                qt_c114 c114
                qt_c115 c115
            }

            test_cases("false", "false")
            test_cases("false", "true")
            test_cases("true", "false")
            test_cases("true", "true")
            sql """ set force_jni_scanner=false; """

            // test view from jion paimon
            sql """ switch internal """
            String view_db = "test_view_for_paimon"
            sql """ drop database if exists ${view_db}"""
            sql """ create database if not exists ${view_db}"""
            sql """use ${view_db}"""
            sql """ create view test_tst_1 as select * from ${catalog_name}.`db1`.all_table; """
            sql """ create view test_tst_2 as select * from ${catalog_name}.`db1`.all_table_with_parquet; """
            sql """ create view test_tst_5 as select * from ${catalog_name}.`db1`.array_nested; """
            sql """ create table test_tst_6 properties ("replication_num" = "1") as 
                        select f.c2,f.c3,c.c1 from 
                            (select a.c2,b.c3 from test_tst_1 a inner join test_tst_2 b on a.c2=b.c2) f
                    inner join test_tst_5 c on f.c2=c.c1;
                """
            def view1 = """select * from test_tst_6 order by c1"""

            // qt_view1 view1
        }
}


