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

    String enabled = context.config.otherConfigs.get("enablePaimonTest")
        if (enabled != null && enabled.equalsIgnoreCase("true")) {
            def all = """select * from all_table;"""
            def c1 = """select * from all_table where c1=1;"""
            def c2 = """select * from all_table where c2=2;"""
            def c3 = """select * from all_table where c3=3;"""
            def c4 = """select * from all_table where c4=4;"""
            def c5 = """select * from all_table where c5=5;"""
            def c6 = """select * from all_table where c6=6;"""
            def c7 = """select * from all_table where c7=7;"""
            def c8 = """select * from all_table where c8=8;"""
            def c9 = """select * from all_table where c9<10;"""
            def c10 = """select * from all_table where c10=10.1;"""
            def c11 = """select * from all_table where c11=11.1;"""
            def c12 = """select * from all_table where c12='2020-02-02';"""
            def c13 = """select * from all_table where c13='13str';"""
            def c14 = """select * from all_table where c14='14varchar';"""
            def c15 = """select * from all_table where c15='a';"""
            def c16 = """select * from all_table where c16=true;"""
            def c18 = """select * from all_table where c18='2023-08-13 09:32:38.53';"""
            def c19 = """select * from auto_bucket order by user_id;"""
            def c20 = """select * from auto_bucket where dt="b";"""
            def c21 = """select * from auto_bucket where dt="b" and hh="c";"""
            def c22 = """select * from auto_bucket where dt="d";"""
            def c23 = """select * from complex_tab order by c1;"""
            def c24 = """select * from complex_tab where c1=1;"""
            def c26 = """select array_max(c2) from complex_tab"""
            def c25 = """select c3['a_test'], c3['b_test'], c3['bbb'], c3['ccc'] from complex_tab"""

            def c27 = """select * from complex_all order by c1;"""
            def c28 = """select array_min(c2) from complex_all"""
            def c29 = """select array_min(c3) from complex_all"""
            def c30= """select array_min(c4) from complex_all"""
            def c31= """select array_min(c5) from complex_all"""
            def c32= """select array_min(c6) from complex_all"""
            def c33= """select array_min(c7) from complex_all"""
            def c34= """select array_min(c8) from complex_all"""
            def c35= """select array_min(c9) from complex_all"""
            def c36= """select array_min(c10) from complex_all"""
            def c37= """select array_max(c11) from complex_all"""
            def c38= """select array_max(c12) from complex_all"""
            def c39= """select array_max(c13) from complex_all"""
            def c40= """select array_size(c14) from complex_all"""
            def c41= """select array_size(c15) from complex_all"""
            def c42= """select array_size(c16) from complex_all"""
            def c43= """select array_max(c17) from complex_all"""
            def c44= """select array_size(c18) from complex_all"""
            def c45= """select array_max(c19) from complex_all"""

            def c46= """select c20[0] from complex_all"""
            def c47= """select c21[0] from complex_all"""
            def c48= """select c22[0] from complex_all"""
            def c49= """select c23[0] from complex_all"""
            def c50= """select c24[1] from complex_all"""
            def c51= """select c25[0] from complex_all"""
            def c52= """select c26[0] from complex_all"""
            def c53= """select c27[0] from complex_all"""
            def c54= """select c28[0] from complex_all"""
            def c55= """select c29[0] from complex_all"""
            def c56= """select c30[0] from complex_all"""
            def c57= """select c31[0] from complex_all"""
            def c58= """select c32[0] from complex_all"""
            def c59= """select c33[0] from complex_all"""
            def c60= """select c34[0] from complex_all"""
            def c61= """select c35[0] from complex_all"""
            def c62= """select c36[0] from complex_all"""
            def c63= """select c37[0] from complex_all"""

            def c64= """select c38[2] from complex_all"""
            def c65= """select c39[4] from complex_all"""
            def c66= """select c40[5] from complex_all;"""
            def c67= """select c41[7] from complex_all;"""
            def c68= """select c42[9] from complex_all"""
            def c69= """select c43[10] from complex_all"""
            def c70= """select c44[12] from complex_all"""
            def c71= """select c45[13] from complex_all"""
            def c72= """select c46[14] from complex_all;"""
            def c73= """select c47[16] from complex_all;"""
            def c74= """select c48[17] from complex_all;"""
            def c75= """select c49[19] from complex_all;"""
            def c76= """select c50[21] from complex_all;"""
            def c77= """select c51[22] from complex_all;"""
            def c78= """select c52[25] from complex_all;"""
            def c79= """select c53[27] from complex_all;"""
            def c80= """select c54[29] from complex_all;"""
            def c81= """select c55[30] from complex_all;"""

            def c82= """select c56[2] from complex_all"""
            def c83= """select c57[4] from complex_all"""
            def c84= """select c58[5] from complex_all;"""
            def c85= """select c59[7] from complex_all;"""
            def c86= """select c60[9] from complex_all"""
            def c87= """select c61[10] from complex_all"""
            def c88= """select c62[12] from complex_all"""
            def c89= """select c63[13] from complex_all"""
            def c90= """select c64[14] from complex_all;"""
            def c91= """select c65[16] from complex_all;"""
            def c92= """select c66[17] from complex_all;"""
            def c93= """select c67[19] from complex_all;"""
            def c94= """select c68[21] from complex_all;"""
            def c95= """select c69[22] from complex_all;"""
            def c96= """select c70[25] from complex_all;"""
            def c97= """select c71[27] from complex_all;"""
            def c98= """select c72[29] from complex_all;"""
            def c99= """select c73[30] from complex_all;"""

            def c100= """select * from array_nested order by c1;"""

            String hdfs_port = context.config.otherConfigs.get("hdfs_port")
            String catalog_name = "paimon1"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type" = "paimon",
                "paimon.catalog.type"="filesystem",
                "warehouse" = "hdfs://${externalEnvIp}:${hdfs_port}/user/doris/paimon1"
            );"""
            sql """use `${catalog_name}`.`db1`"""

            qt_all all
            qt_c1 c1
            qt_c2 c2
            qt_c3 c3
            qt_c4 c4
            qt_c5 c5
            qt_c6 c6
            qt_c7 c7
            qt_c8 c8
            qt_c9 c9
            qt_c10 c10
            qt_c11 c11
            qt_c12 c12
            qt_c13 c13
            qt_c14 c14
            qt_c15 c15
            qt_c16 c16
            qt_c18 c18
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
            qt_c80 c81
            qt_c80 c82
            qt_c80 c83
            qt_c80 c84
            qt_c80 c85
            qt_c80 c86
            qt_c80 c87
            qt_c80 c88
            qt_c80 c89
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
        }
}
