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

suite("test_paimon_s3", "p0,external,doris,external_docker,external_docker_doris,new_catalog_property") {
    def testQuery = { String catalogProperties, String prefix, String dbName ->
        def catalog_name = "test_paimon_on_fs_${prefix}_catalog"
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
            SELECT * FROM hive_test_table;
        """
        sql """set force_jni_scanner=true"""
        "order_qt_${prefix}" """
            SELECT * FROM hive_test_table;
        """

    }
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String s3_warehouse = "s3a://selectdb-qa-datalake-test-hk/paimon_warehouse"
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
        String paimon_fs_catalog_properties = """
         'type'='paimon',
         "paimon.catalog.type"="filesystem",
    """
        testQuery(paimon_fs_catalog_properties + s3_warehouse_properties + s3_storage_properties, "aws", "aws_db")

    }

}


