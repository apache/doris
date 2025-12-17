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

suite("test_paimon_table", "p0,external,doris,external_docker,external_docker_doris,new_catalog_property") {

    String hms_ctl_name = "paimon_hms_catalog_test01";

    // This is only for testing creating catalog
    sql """DROP CATALOG IF EXISTS ${hms_ctl_name}"""
    sql """
        CREATE CATALOG ${hms_ctl_name} PROPERTIES (
            "type" = "paimon",
            "paimon.catalog.type"="hms",
            "warehouse" = "hdfs://HDFS8000871/user/zhangdong/paimon3",
            "hive.metastore.uris" = "thrift://172.21.0.44:7004",
            "dfs.nameservices"="HDFS8000871",
            "dfs.ha.namenodes.HDFS8000871"="nn1,nn2",
            "dfs.namenode.rpc-address.HDFS8000871.nn1"="172.21.0.1:4007",
            "dfs.namenode.rpc-address.HDFS8000871.nn2"="172.21.0.2:4007",
            "dfs.client.failover.proxy.provider.HDFS8000871"="org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
            "hadoop.username"="hadoop"
        );
    """
    sql """switch ${hms_ctl_name}"""
    String db_name = "test_db"
    sql """create database if not exists ${db_name}"""
    sql """use ${db_name}"""

    sql """drop table if exists ${db_name}.test01"""
    sql """
        CREATE TABLE ${db_name}.test01 (
        id int
    ) engine=paimon;
    """

    sql """drop table if exists ${db_name}.test02"""
    sql """
        CREATE TABLE ${db_name}.test02 (
        id int
    ) engine=paimon
    properties("primary-key"=id);
    """

    sql """drop table if exists ${db_name}.test03"""
    sql """
        CREATE TABLE ${db_name}.test03 (
        c0 int,
        c1 bigint,
        c2 float,
        c3 double,
        c4 string,
        c5 date,
        c6 decimal(10,5),
        c7 datetime
    ) engine=paimon
    properties("primary-key"=c0);
    """

    sql """drop table if exists ${db_name}.test04"""
    sql """
        CREATE TABLE ${db_name}.test04 (
        c0 int,
        c1 bigint,
        c2 float,
        c3 double,
        c4 string,
        c5 date,
        c6 decimal(10,5),
        c7 datetime
    ) engine=paimon
    partition by (c1) ()
    properties("primary-key"=c0);
    """

    sql """ drop table if exists ${db_name}.test01"""
    sql """ drop table if exists ${db_name}.test02"""
    sql """ drop table if exists ${db_name}.test03"""
    sql """ drop table if exists ${db_name}.test04"""
    sql """ drop database if exists ${db_name}"""
    sql """DROP CATALOG IF EXISTS ${hms_ctl_name}"""
}


