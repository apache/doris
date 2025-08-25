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

suite("test_paimon_hms_catalog", "p2,external,paimon,new_catalog_property") {

    def testQuery = { String catalogProperties, String prefix, String dbName ->
        def catalog_name = "test_paimon_on_hms_${prefix}_catalog"
        sql """
            DROP CATALOG IF EXISTS ${catalog_name};
        """
        sql """
            CREATE CATALOG IF NOT EXISTS ${catalog_name} PROPERTIES (
                ${catalogProperties}
            );
        """
        sql """
            switch ${catalog_name};
        """
        sql """
            show databases;
        """
        sql """
            use ${dbName};
        """
        sql """
            show tables;
        """
        sql """set force_jni_scanner=false"""
        "order_qt_${prefix}" """
            SELECT * FROM external_test_table;
        """
         sql """set force_jni_scanner=true"""
         "order_qt_${prefix}" """
            SELECT * FROM external_test_table;
        """
    }
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }
    String extHiveHmsHost = context.config.otherConfigs.get("externalEnvIp")
    String extHiveHmsPort = context.config.otherConfigs.get("hive3HmsPort")
    String extHiveHdfsHost = context.config.otherConfigs.get("hive3HdfsPort")

    /****************OSS*******************/
    String oss_ak = context.config.otherConfigs.get("aliYunAk")
    String oss_sk = context.config.otherConfigs.get("aliYunSk")
    String oss_endpoint = "oss-cn-beijing.aliyuncs.com"
    String oss_warehouse = "oss://doris-regression-bj/regression/paimon_warehouse"
    String oss_warehouse_properties = """
        'warehouse' = '${oss_warehouse}',
    """
    String oss_storage_properties = """
      'oss.access_key' = '${oss_ak}',
      'oss.secret_key' = '${oss_sk}',
      'oss.endpoint' = '${oss_endpoint}'
    """
    /****************COS*******************/
    String cos_ak = context.config.otherConfigs.get("txYunAk")
    String cos_sk = context.config.otherConfigs.get("txYunSk")
    String cos_endpoint = "cos.ap-beijing.myqcloud.com"
    String cos_region = "ap-beijing"
    String cos_warehouse = "cosn://sdb-qa-datalake-test-1308700295/paimon_warehouse"
    String cos_warehouse_properties = """
        'warehouse' = '${cos_warehouse}',
    """

    String cos_storage_properties = """
      'cos.access_key' = '${cos_ak}',
      'cos.secret_key' = '${cos_sk}',
      'cos.endpoint' = '${cos_endpoint}'
    """

    /****************OBS*******************/
    String obs_ak = context.config.otherConfigs.get("hwYunAk")
    String obs_sk = context.config.otherConfigs.get("hwYunSk")
    String obs_endpoint = "obs.cn-north-4.myhuaweicloud.com"
    String obs_warehouse = "obs://doris-build/regression/paimon_warehouse"
    String obs_warehouse_properties = """
        'warehouse' = '${obs_warehouse}',
    """

    String obs_storage_properties = """
      'obs.access_key' = '${obs_ak}',
      'obs.secret_key' = '${obs_sk}',
      'obs.endpoint' = '${obs_endpoint}'
    """
    /****************HDFS*******************/
    String hdfs_warehouse = "hdfs://${extHiveHmsHost}:${extHiveHdfsHost}/user/hive/warehouse"
    String hdfs_warehouse_properties = """
        'warehouse' = '${hdfs_warehouse}',
    """
    String hdfs_storage_properties = """
      'fs.defaultFS' = 'hdfs://${extHiveHmsHost}:${extHiveHdfsHost}'
    """
    /**************** AWS S3*******************/
    String s3_warehouse = "s3://selectdb-qa-datalake-test-hk/paimon_warehouse"
    String aws_ak = context.config.otherConfigs.get("AWSAK")
    String aws_sk = context.config.otherConfigs.get("AWSSK")
    String aws_endpoint = "s3.ap-east-1.amazonaws.com"
    String s3_warehouse_properties = """
        'warehouse' = '${s3_warehouse}',
    """
    String s3_storage_properties = """
        's3.access_key' = '${aws_ak}',
        's3.secret_key' = '${aws_sk}',
        's3.endpoint' = '${aws_endpoint}'
    """

    /*--------HMS START -----------*/
    String paimon_hms_catalog_properties = """
     'type'='paimon',
     'paimon.catalog.type'='hms',
     'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}',
    """
    testQuery(paimon_hms_catalog_properties + hdfs_warehouse_properties + hdfs_storage_properties, "hdfs", "hdfs_db")
    testQuery(paimon_hms_catalog_properties + oss_warehouse_properties + oss_storage_properties, "oss", "ali_db")
    testQuery(paimon_hms_catalog_properties + obs_warehouse_properties + obs_storage_properties, "obs", "hw_db")
    testQuery(paimon_hms_catalog_properties + cos_warehouse_properties + cos_storage_properties, "cos", "tx_db")
    testQuery(paimon_hms_catalog_properties + s3_warehouse_properties + s3_storage_properties, "s3", "aws_db")

    String paimon_fs_hdfs_support = """
        'fs.hdfs.support' = 'true',
    """
    String paimon_fs_oss_support = """
        'fs.oss.support' = 'true',
    """
    String paimon_fs_obs_support = """
        'fs.obs.support' = 'true',
    """
    String paimon_fs_cos_support = """
        'fs.cos.support' = 'true',
    """
    String paimon_fs_s3_support = """
        'fs.s3.support' = 'true',
    """
    testQuery(paimon_hms_catalog_properties + paimon_fs_hdfs_support + hdfs_warehouse_properties + hdfs_storage_properties, "support_hdfs", "hdfs_db")
    testQuery(paimon_hms_catalog_properties + paimon_fs_oss_support + oss_warehouse_properties + oss_storage_properties, "support_oss", "ali_db")
    testQuery(paimon_hms_catalog_properties + paimon_fs_obs_support + obs_warehouse_properties + obs_storage_properties, "support_obs", "hw_db")
    testQuery(paimon_hms_catalog_properties + paimon_fs_cos_support + cos_warehouse_properties + cos_storage_properties, "support_cos", "tx_db")
    testQuery(paimon_hms_catalog_properties + paimon_fs_s3_support + s3_warehouse_properties + s3_storage_properties, "support_s3", "aws_db")
}

