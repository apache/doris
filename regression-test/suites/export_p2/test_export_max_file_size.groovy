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

suite("test_export_max_file_size", "p2") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    String dfsNameservices=context.config.otherConfigs.get("dfsNameservices")
    String dfsHaNamenodesHdfsCluster=context.config.otherConfigs.get("dfsHaNamenodesHdfsCluster")
    String dfsNamenodeRpcAddress1=context.config.otherConfigs.get("dfsNamenodeRpcAddress1")
    String dfsNamenodeRpcAddress2=context.config.otherConfigs.get("dfsNamenodeRpcAddress2")
    String dfsNamenodeRpcAddress3=context.config.otherConfigs.get("dfsNamenodeRpcAddress3")
    String dfsNameservicesPort=context.config.otherConfigs.get("dfsNameservicesPort")
    String hadoopSecurityAuthentication =context.config.otherConfigs.get("hadoopSecurityAuthentication")
    String hadoopKerberosKeytabPath =context.config.otherConfigs.get("hadoopKerberosKeytabPath")
    String hadoopKerberosPrincipal =context.config.otherConfigs.get("hadoopKerberosPrincipal")


    def table_export_name = "test_export_max_file_size"
    // create table and insert
    sql """ DROP TABLE IF EXISTS ${table_export_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_export_name} (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `date` DATE NOT NULL COMMENT "数据灌入日期时间",
        `datetime` DATETIME NOT NULL COMMENT "数据灌入日期时间",
        `city` VARCHAR(20) COMMENT "用户所在城市",
        `age` INT COMMENT "用户年龄",
        `sex` INT COMMENT "用户性别",
        `bool_col` boolean COMMENT "",
        `int_col` int COMMENT "",
        `bigint_col` bigint COMMENT "",
        `largeint_col` largeint COMMENT "",
        `float_col` float COMMENT "",
        `double_col` double COMMENT "",
        `char_col` CHAR(10) COMMENT "",
        `decimal_col` decimal COMMENT ""
        )
        DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
    """

    // Used to store the data exported before
    def table_load_name = "test_load"
    sql """ DROP TABLE IF EXISTS ${table_load_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_load_name} (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `date` DATE NOT NULL COMMENT "数据灌入日期时间",
        `datetime` DATETIME NOT NULL COMMENT "数据灌入日期时间",
        `city` VARCHAR(20) COMMENT "用户所在城市",
        `age` INT COMMENT "用户年龄",
        `sex` INT COMMENT "用户性别",
        `bool_col` boolean COMMENT "",
        `int_col` int COMMENT "",
        `bigint_col` bigint COMMENT "",
        `largeint_col` largeint COMMENT "",
        `float_col` float COMMENT "",
        `double_col` double COMMENT "",
        `char_col` CHAR(10) COMMENT "",
        `decimal_col` decimal COMMENT ""
        )
        DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
    """

    def load_data_path = "/user/export_test/exp_max_file_size.csv"
    sql """ 
            insert into ${table_export_name}
            select * from hdfs(
                "uri" = "hdfs://${dfsNameservices}${load_data_path}",
                "format" = "csv",
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

    
    def waiting_export = { export_label ->
        while (true) {
            def res = sql """ show export where label = "${export_label}" """
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                return res[0][11]
            } else if (res[0][2] == "CANCELLED") {
                throw new IllegalStateException("""export failed: ${res[0][10]}""")
            } else {
                sleep(5000)
            }
        }
    }

    def outFilePath = """/user/export_test/test_max_file_size/exp_"""

    // 1. csv test
    def test_export = {format, file_suffix, isDelete ->
        def uuid = UUID.randomUUID().toString()
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "hdfs://${dfsNameservices}${outFilePath}"
            PROPERTIES(
                "label" = "${uuid}",
                "format" = "${format}",
                "max_file_size" = "5MB",
                "delete_existing_files"="${isDelete}"
            )
            with HDFS (
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

        def outfile_info = waiting_export.call(uuid)
        def json = parseJson(outfile_info)
        assert json instanceof List
        assertEquals("3", json.fileNumber[0][0])
        def outfile_url = json.url[0][0]

        for (int j = 0; j < json.fileNumber[0][0].toInteger(); ++j ) {
            // check data correctness
            sql """ 
                insert into ${table_load_name}
                select * from hdfs(
                    "uri" = "${outfile_url}${j}.csv",
                    "format" = "csv",
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
    }

    // begin test
    test_export('csv', 'csv', true);
    order_qt_select """ select * from ${table_load_name} order by user_id limit 1000 """
    order_qt_select_cnt """ select count(*) from ${table_load_name} """

}
