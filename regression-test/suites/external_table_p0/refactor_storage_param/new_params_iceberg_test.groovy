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

suite("new_params_iceberg_test", "p0,external,hive,external_docker,external_docker_hive") {


    def testQueryAndInsert = { String catalogProperties, String prefix ->

        def catalog_name = "${prefix}_catalog"
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

        def db_name = prefix + "_db"
        sql """
            DROP DATABASE IF EXISTS ${db_name} FORCE;
        """
        sql """
            CREATE DATABASE IF NOT EXISTS ${db_name};
        """

        def dbResult = sql """
            show databases  like "${db_name}";
        """
        assert dbResult.size() == 1

        sql """
            use ${db_name};
        """
        def table_name = prefix + "_table"
        sql """
            CREATE TABLE ${table_name} (
            user_id            BIGINT       NOT NULL COMMENT "user id",
            name               VARCHAR(20)           COMMENT "name",
            age                INT                   COMMENT "age"
        );
        """
        sql """
            insert into ${table_name} values (1, 'a', 10);
        """
        // query
        def queryResult = sql """
            SELECT * FROM ${table_name};
        """
        assert queryResult.size() == 1

        sql """
            DROP TABLE ${table_name};
        """
        sql """
            DROP DATABASE ${db_name} FORCE;
        """

        def dropResult = sql """
            show databases  like "${db_name}";
        """
        assert dropResult.size() == 0
    }

    /*--------only execute query---------*/
    def testQuery = { String catalog_properties, String prefix, String db_name, String table_name, int data_count ->

        def catalog_name = "${prefix}_catalog"
        sql """
            DROP CATALOG IF EXISTS ${catalog_name};
        """
        sql """
            CREATE CATALOG IF NOT EXISTS ${catalog_name} PROPERTIES (
                ${catalog_properties}
            );
        """
        sql """
            switch ${catalog_name};
        """


        def dbResult = sql """
            show databases  like "${db_name}";
        """
        assert dbResult.size() == 1

        sql """
            use ${db_name};
        """

        // query
        def queryResult = sql """
            SELECT count(1) FROM ${table_name};
        """
        assert queryResult.get(0).get(0) == data_count
    }

    /* REST catalog env and base properties */
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String iceberg_rest_type_prop = """
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'iceberg.rest.uri' = 'http://${externalEnvIp}:${rest_port}',
    """

    /*-----S3------*/
    String s3_ak = context.config.otherConfigs.get("AWSAK")
    String s3_sk = context.config.otherConfigs.get("AWSSK")
    String s3_endpoint = "s3.ap-northeast-1.amazonaws.com"
    String s3_region = "ap-northeast-1"
    String s3_parent_path = "selectdb-qa-datalake-test/"
    String s3_storage_properties = """
      's3.access_key' = '${s3_ak}',
      's3.secret_key' = '${s3_sk}',
      's3.endpoint' = '${s3_endpoint}'
    """
    String s3_region_param = """
     's3.region' = '${s3_region}',
    """
    /****************OSS*******************/
    String oss_ak = context.config.otherConfigs.get("aliYunAk")
    String oss_sk = context.config.otherConfigs.get("aliYunSk")
    String oss_endpoint = "oss-cn-hongkong.aliyuncs.com"
    String oss_parent_path = "doris-regression-hk/iceberg"
    String oss_region = "cn-hongkong"
    String oss_region_param = """
     'oss.region' = '${oss_region}',
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
    String cos_region_param = """
     'cos.region' = '${cos_region}',
    """

    String cos_parent_path = "doris-build-1308700295";

    String cos_storage_properties = """
      'cos.access_key' = '${cos_ak}',
      'cos.secret_key' = '${cos_sk}',
      'cos.endpoint' = '${cos_endpoint}'
    """

    /****************HDFS*******************/
    //simple
    String hdfs_parent_path = 'hdfs://hdfs-cluster/user/iceberg/warehouse'
    String hdfs_properties = """
        "hadoop.username" = "hadoop",
        "dfs.nameservices" = "hdfs-cluster",
        "dfs.namenode.rpc-address.hdfs-cluster.nn2" = "master-1-1.c-9dd8ca709bf0e6d3.cn-beijing.emr.aliyuncs.com:9000",
        "dfs.namenode.rpc-address.hdfs-cluster.nn1" = "master-1-1.c-9dd8ca709bf0e6d3.cn-beijing.emr.aliyuncs.com:9000",
        "dfs.ha.namenodes.hdfs-cluster" = "nn1,nn2",
        "dfs.client.failover.proxy.provider.hdfs-cluster" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
    """
    // REST-only configuration
    /**************** REST on various storages *******************/

    // -------- REST on OSS --------
    String warehouse = """
     'warehouse' = 'oss://${oss_parent_path}/iceberg-rest-oss-warehouse',
    """
    testQueryAndInsert(iceberg_rest_type_prop + warehouse + oss_storage_properties, "iceberg_rest_on_oss")
    testQueryAndInsert(iceberg_rest_type_prop + warehouse + oss_region_param + oss_storage_properties, "iceberg_rest_on_oss_region")

    // -------- REST on COS --------
    warehouse = """
     'warehouse' = 'cos://${cos_parent_path}/iceberg-rest-cos-warehouse',
    """
    testQueryAndInsert(iceberg_rest_type_prop + warehouse + cos_storage_properties, "iceberg_rest_on_cos")
    testQueryAndInsert(iceberg_rest_type_prop + warehouse + cos_region_param + cos_storage_properties, "iceberg_rest_on_cos_region")
    warehouse = """
     'warehouse' = 'cosn://${cos_parent_path}/iceberg-rest-cos-warehouse',
    """
    testQueryAndInsert(iceberg_rest_type_prop + warehouse + cos_storage_properties, "iceberg_rest_on_cosn")
    testQueryAndInsert(iceberg_rest_type_prop + warehouse + cos_region_param + cos_storage_properties, "iceberg_rest_on_cosn_region")

    // -------- REST on HDFS --------
    warehouse = """
     'warehouse' = '${hdfs_parent_path}/iceberg-rest-hdfs-warehouse',
    """
    testQueryAndInsert(iceberg_rest_type_prop + warehouse + hdfs_properties, "iceberg_rest_on_hdfs")

    // -------- REST on S3 --------
    warehouse = """
     'warehouse' = 's3://${s3_parent_path}/iceberg-rest-s3-warehouse',
    """
    testQueryAndInsert(iceberg_rest_type_prop + warehouse + s3_storage_properties, "iceberg_rest_on_s3")
    testQueryAndInsert(iceberg_rest_type_prop + warehouse + s3_region_param + s3_storage_properties, "iceberg_rest_on_s3_region")
    warehouse = """
     'warehouse' = 's3a://${s3_parent_path}/iceberg-rest-s3-warehouse',
    """
    testQueryAndInsert(iceberg_rest_type_prop + warehouse + s3_storage_properties, "iceberg_rest_on_s3a")
    testQueryAndInsert(iceberg_rest_type_prop + warehouse + s3_region_param + s3_storage_properties, "iceberg_rest_on_s3a_region")
}