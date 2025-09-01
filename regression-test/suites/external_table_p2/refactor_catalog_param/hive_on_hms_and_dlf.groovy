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
import static groovy.test.GroovyAssert.shouldFail;
import java.util.concurrent.ThreadLocalRandom

suite("hive_on_hms_and_dlf", "p2,external,new_catalog_property") {


    def testQueryAndInsert = { String catalogProperties, String prefix, String dbLocation ->

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

        def db_name = prefix + "_db" + System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(1000)
        sql """
            DROP DATABASE IF EXISTS ${db_name} FORCE;
        """
        sql """
            CREATE DATABASE IF NOT EXISTS ${db_name}
            PROPERTIES ('location'='${dbLocation}');

        """

        def dbResult = sql """
            show databases  like "${db_name}";
        """
        assert dbResult.size() == 1

        sql """
            use ${db_name};
        """
        def table_name = prefix + ThreadLocalRandom.current().nextInt(1000) + "_table"
        sql """
            CREATE TABLE ${table_name} (
            user_id            BIGINT      COMMENT "user id",
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
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String keytab_root_dir = "/keytabs"
    /*-----S3------*/
    String s3_ak = context.config.otherConfigs.get("AWSAK")
    String s3_sk = context.config.otherConfigs.get("AWSSK")
    String s3_endpoint = "http://s3.ap-east-1.amazonaws.com/"
    String s3_region = "ap-east-1"
    String s3_parent_path = "selectdb-qa-datalake-test-hk/refactor-test"
    String s3_storage_properties = """
      's3.access_key' = '${s3_ak}',
      's3.secret_key' = '${s3_sk}',
      's3.endpoint' = '${s3_endpoint}'
    """
    String s3_storage_properties_not_endpoint = """
      's3.access_key' = '${s3_ak}',
      's3.secret_key' = '${s3_sk}'
    """
    String s3_region_param = """
     's3.region' = '${s3_region}',
    """
    /****************OSS*******************/
    String oss_ak = context.config.otherConfigs.get("aliYunAk")
    String oss_sk = context.config.otherConfigs.get("aliYunSk")
    String oss_endpoint = "oss-cn-beijing.aliyuncs.com"
    String oss_parent_path = "doris-regression-bj/refactor-test"
    String oss_region = "cn-beijing"
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
    String hdfs_parent_path = "hdfs://${externalEnvIp}:8320/user/hive/warehouse"
    String hdfs_properties = """
                "hadoop.username" = "doris"
    """
    // kerberos
    String hdfs_kerberos_properties = """
                "fs.defaultFS" = "hdfs://${externalEnvIp}:8520",
                "hadoop.security.authentication" = "kerberos",
             
                "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@LABS.TERADATA.COM",
                "hadoop.kerberos.keytab" = "${keytab_root_dir}/hive-presto-master.keytab"
    """
    String dlf_access_key = context.config.otherConfigs.get("dlf_access_key")
    String dlf_secret_key = context.config.otherConfigs.get("dlf_secret_key")
    /**************** DLF *******************/

    String dlf_oss_properties = """
               "oss.endpoint" = "oss-cn-beijing.aliyuncs.com",
               "oss.access_key" = "${dlf_access_key}",
               "oss.secret_key" = "${dlf_secret_key}",
    """


    String hms_kerberos_old_prop = """
                "hive.metastore.uris" = "thrift://${externalEnvIp}:9583",
                "hadoop.security.authentication" = "kerberos",
                "hadoop.kerberos.principal"="hive/presto-master.docker.cluster@LABS.TERADATA.COM",
                "hadoop.kerberos.keytab" = "${keytab_root_dir}/hive-presto-master.keytab",
                "hadoop.security.auth_to_local" = "RULE:[2:\\\$1@\\\$0](.*@LABS.TERADATA.COM)s/@.*//
                                   RULE:[2:\\\$1@\\\$0](.*@OTHERLABS.TERADATA.COM)s/@.*//
                                   RULE:[2:\\\$1@\\\$0](.*@OTHERREALM.COM)s/@.*//
                                   DEFAULT",
                "hive.metastore.sasl.enabled " = "true",
                "hive.metastore.kerberos.principal" = "hive/hadoop-master@LABS.TERADATA.COM",
     """


    String hms_kerberos_new_prop = """
                "hive.metastore.uris" = "thrift://${externalEnvIp}:9583",
                "hive.metastore.client.principal"="hive/presto-master.docker.cluster@LABS.TERADATA.COM",
                "hive.metastore.client.keytab" = "${keytab_root_dir}/hive-presto-master.keytab",
                "hive.metastore.service.principal" = "hive/hadoop-master@LABS.TERADATA.COM",
                "hive.metastore.sasl.enabled " = "true",
                "hive.metastore.authentication.type"="kerberos",
                "hadoop.security.auth_to_local" = "RULE:[2:\\\$1@\\\$0](.*@LABS.TERADATA.COM)s/@.*//
                                   RULE:[2:\\\$1@\\\$0](.*@OTHERLABS.TERADATA.COM)s/@.*//
                                   RULE:[2:\\\$1@\\\$0](.*@OTHERREALM.COM)s/@.*//
                                   DEFAULT",
     """
    /*************** HIVE HMS*******************/

    String hms_properties = """
        "type"="hms",
        "hive.metastore.uris" = "thrift://${externalEnvIp}:9383",
    """
    String hms_type_properties = """
        "type"="hms",
    """
    //COS
    String db_location = "cosn://${cos_parent_path}/hive/hms/" + System.currentTimeMillis()
    testQueryAndInsert(hms_properties + cos_storage_properties, "hive_hms_cos_test", db_location)
    testQueryAndInsert(hms_properties + cos_region_param + cos_storage_properties, "hive_hms_cos_test_region", db_location)
    testQueryAndInsert(hms_type_properties + hms_kerberos_old_prop + cos_storage_properties, "hive_hms_on_cos_kerberos_old", db_location)
    testQueryAndInsert(hms_type_properties + hms_kerberos_new_prop + cos_storage_properties, "hive_hms_on_cos_kerberos_new", db_location)

    db_location = "cos://${cos_parent_path}/hive/hms/" + System.currentTimeMillis()
    testQueryAndInsert(hms_properties + cos_storage_properties, "hive_hms_cos_test", db_location)

    //OSS
    db_location = "oss://${oss_parent_path}/hive/hms/" + System.currentTimeMillis()
    testQueryAndInsert(hms_properties + oss_storage_properties, "hive_hms_oss_test", db_location)
    testQueryAndInsert(hms_properties + oss_region_param + oss_storage_properties, "hive_hms_oss_test_region", db_location)
    testQueryAndInsert(hms_type_properties + hms_kerberos_old_prop + oss_storage_properties, "hive_hms_on_oss_kerberos_old", db_location)
    testQueryAndInsert(hms_type_properties + hms_kerberos_new_prop + oss_storage_properties, "hive_hms_on_oss_kerberos_new", db_location)

    //s3
   db_location = "s3a://${s3_parent_path}/hive/hms/"+System.currentTimeMillis()
    testQueryAndInsert(hms_properties + s3_storage_properties, "hive_hms_s3_test", db_location)
    db_location = "s3a://${s3_parent_path}/hive/hms/"+System.currentTimeMillis()
    testQueryAndInsert(hms_properties + s3_region_param + s3_storage_properties, "hive_hms_s3_test_region", db_location)
    db_location = "s3a://${s3_parent_path}/hive/hms/"+System.currentTimeMillis()
    testQueryAndInsert(hms_properties + s3_region_param + s3_storage_properties_not_endpoint, "hive_hms_s3_test_region", db_location)
    //db_location = "s3a://${s3_parent_path}/hive/hms/"+System.currentTimeMillis()
    //testQueryAndInsert(hms_type_properties + hms_kerberos_old_prop  + s3_storage_properties, "hive_hms_on_s3_kerberos_old",db_location)
    //db_location = "s3a://${s3_parent_path}/hive/hms/"+System.currentTimeMillis()
    //testQueryAndInsert(hms_type_properties + hms_kerberos_new_prop + s3_storage_properties, "hive_hms_on_s3_kerberos_new",db_location)
    db_location = "s3://${s3_parent_path}/hive/hms/"+System.currentTimeMillis()
    testQueryAndInsert(hms_properties + s3_storage_properties, "hive_hms_s3_test", db_location)
    //HDFS
    db_location = "${hdfs_parent_path}/hive/hms/" + System.currentTimeMillis()
    testQueryAndInsert(hms_properties + hdfs_properties, "hive_hms_hdfs_test", db_location)
    //kerberos
    db_location = "hdfs://${externalEnvIp}:8520/hive/hms/" + System.currentTimeMillis()

    testQueryAndInsert(hms_type_properties + hms_kerberos_new_prop + hdfs_kerberos_properties, "hive_hms_hdfs_kerberos_test", db_location)

    /**************** DLF *******************/
    String dlf_warehouse = "oss://selectdb-qa-datalake-test/hive-dlf-oss-warehouse"
    String hive_dlf_catalog_base_properties = """
            'type'='hms',
            'hive.metastore.type'='dlf',
            "dlf.uid" = "203225413946383283",
            "dlf.catalog.id" = "p2_regression_case",
            "dlf.access_key" = "${dlf_access_key}",
            "dlf.secret_key" = "${dlf_secret_key}"
   """
    String dlf_region = "cn-beijing"
    String dlf_region_param = """
            "dlf.region" = "${dlf_region}",
    """
    String dlf_endpoint = "dlf.cn-beijing.aliyuncs.com"
    String dlf_endpoint_properties = """
             "dlf.endpoint" = "${dlf_endpoint}",
    """
    db_location = "${dlf_warehouse}/hive/dlf-oss/" + System.currentTimeMillis()
    testQueryAndInsert(dlf_region_param + hive_dlf_catalog_base_properties, "hive_dlf_oss_test", db_location)
    db_location = "${dlf_warehouse}/hive/dlf-oss/" + System.currentTimeMillis()
    testQueryAndInsert(dlf_region_param + dlf_oss_properties + hive_dlf_catalog_base_properties, "hive_dlf_oss_test_with_oss", db_location)
 
    // not has region param
    db_location = "${dlf_warehouse}/hive/dlf-oss/" + System.currentTimeMillis()
    testQueryAndInsert(dlf_endpoint_properties + hive_dlf_catalog_base_properties, "hive_dlf_oss_test", db_location)
}