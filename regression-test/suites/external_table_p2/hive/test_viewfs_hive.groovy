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

suite("test_viewfs_hive", "p2,external,hive,external_remote,external_remote_hive") {
    Boolean ignoreP2 = true;
    if (ignoreP2) {
        logger.info("disable p2 test");
        return;
    }

    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String nameNodeHost = context.config.otherConfigs.get("extHiveHmsHost")
        String hdfsPort = context.config.otherConfigs.get("extHdfsPort")
        String catalog_name = "test_viewfs_hive"

        sql """drop catalog if exists ${catalog_name};"""

        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}',
                'fs.viewfs.mounttable.my-cluster.link./ns1' = 'hdfs://${nameNodeHost}:${hdfsPort}/',
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
    }
}
