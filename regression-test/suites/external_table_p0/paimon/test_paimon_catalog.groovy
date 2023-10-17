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
}
