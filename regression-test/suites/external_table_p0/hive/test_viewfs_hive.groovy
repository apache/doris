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

suite("test_viewfs_hive", "p0,external,hive,external_docker,external_docker_hive") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (!"true".equalsIgnoreCase(enabled)) {
        return;
    }
    for (String hivePrefix : ["hive2", "hive3"]) {
        String catalog_name = "${hivePrefix}_test_viewfs_hive"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfsPort = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String nameNodeHost = externalEnvIp

        sql """drop catalog if exists ${catalog_name};"""

        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'fs.viewfs.mounttable.my-cluster.link./ns1' = 'hdfs://${externalEnvIp}:${hdfsPort}/',
                'fs.viewfs.mounttable.my-cluster.homedir' = '/ns1',
                'fs.defaultFS' = 'viewfs://my-cluster'
            );
        """
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        
        sql """ use viewfs """ 

        // The location of table is on viewfs.
        qt_viewfs """ select * from test_viewfs order by id"""

        // The location of partition table is on viewfs.
        qt_viewfs_partition1 """ select * from test_viewfs_partition order by id""" 
        qt_viewfs_partition2 """ select * from test_viewfs_partition where part_col = 20230101 order by id""" 
        qt_viewfs_partition3 """ select * from test_viewfs_partition where part_col = 20230201 order by id""" 

        // The location of partition table contains hdfs and viewfs locations partitions.
        qt_viewfs_mixed_partition1 """ select * from test_viewfs_mixed_partition order by id""" 
        qt_viewfs_mixed_partition2 """ select * from test_viewfs_mixed_partition where part_col = 20230101 order by id""" 
        qt_viewfs_mixed_partition3 """ select * from test_viewfs_mixed_partition where part_col = 20230201 order by id"""

        sql """drop catalog if exists ${catalog_name};"""
    }
}
