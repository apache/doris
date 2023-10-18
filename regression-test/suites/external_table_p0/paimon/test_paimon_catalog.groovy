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
            def c25 = """select * from complex_tab where c1=2;"""
            def c26 = """select * from complex_tab where c1=3;"""

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
        }
}
