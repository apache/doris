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

suite("test_outfile_p2", "p2") {

    String dfsNameservices=context.config.otherConfigs.get("dfsNameservices")
    String dfsHaNamenodesHdfsCluster=context.config.otherConfigs.get("dfsHaNamenodesHdfsCluster")
    String dfsNamenodeRpcAddress1=context.config.otherConfigs.get("dfsNamenodeRpcAddress1")
    String dfsNamenodeRpcAddress2=context.config.otherConfigs.get("dfsNamenodeRpcAddress2")
    String dfsNamenodeRpcAddress3=context.config.otherConfigs.get("dfsNamenodeRpcAddress3")
    String dfsNameservicesPort=context.config.otherConfigs.get("dfsNameservicesPort")
    String hadoopSecurityAuthentication =context.config.otherConfigs.get("hadoopSecurityAuthentication")
    String hadoopKerberosKeytabPath =context.config.otherConfigs.get("hadoopKerberosKeytabPath")
    String hadoopKerberosPrincipal =context.config.otherConfigs.get("hadoopKerberosPrincipal")


    def table_outfile_name = "test_outfile_hdfs"
    // create table and insert
    sql """ DROP TABLE IF EXISTS ${table_outfile_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_outfile_name} (
        `id` int(11) NULL,
        `name` string NULL
        )
        DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
    """

    sql """insert into ${table_outfile_name} values(1, 'abc');"""

    qt_sql_1 """select * from ${table_outfile_name} order by id"""

    // use a simple sql to make sure there is only one fragment
    // #21343
    sql """
        SELECT * FROM ${table_outfile_name}
        INTO OUTFILE "hdfs://${dfsNameservices}/user/outfile_test/" 
        FORMAT AS parquet
        PROPERTIES
        (
            "dfs.data.transfer.protection" = "integrity",
            'dfs.nameservices'="${dfsNameservices}",
            'dfs.ha.namenodes.hdfs-cluster'="${dfsHaNamenodesHdfsCluster}",
            'dfs.namenode.rpc-address.hdfs-cluster.nn1'="${dfsNamenodeRpcAddress1}:${dfsNameservicesPort}",
            'dfs.namenode.rpc-address.hdfs-cluster.nn2'="${dfsNamenodeRpcAddress2}:${dfsNameservicesPort}",
            'dfs.namenode.rpc-address.hdfs-cluster.nn3'="${dfsNamenodeRpcAddress3}:${dfsNameservicesPort}",
            'hadoop.security.authentication'="${hadoopSecurityAuthentication}",
            'hadoop.kerberos.keytab'="${hadoopKerberosKeytabPath}",   
            'hadoop.kerberos.principal'="${hadoopKerberosPrincipal}",
            'dfs.client.failover.proxy.provider.hdfs-cluster'="org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        );
    """
    
}
