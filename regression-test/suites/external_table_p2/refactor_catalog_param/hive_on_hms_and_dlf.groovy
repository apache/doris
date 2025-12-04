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

    /*--------test partition table insert---------*/
    def testPartitionTableInsert = { String catalogProperties, String prefix, String dbLocation ->
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

        def table_name = prefix + ThreadLocalRandom.current().nextInt(1000) + "_partition_table"

        // Create partitioned table
        sql """
            CREATE TABLE ${table_name} (
                id INT COMMENT 'id',
                name VARCHAR(20) COMMENT 'name',
                age INT COMMENT 'age',
                pt1 VARCHAR(20) COMMENT 'partition key'
            ) ENGINE=hive
            PARTITION BY LIST (pt1) ()
            PROPERTIES (
                'file_format'='orc',
                'compression'='zlib'
            );
        """

        // Test 1: Insert into new partition
        sql """
            insert into ${table_name} values (1, 'alice', 20, 'p1');
        """
        def result1 = sql """
            SELECT * FROM ${table_name} ORDER BY id;
        """
        assert result1.size() == 1
        assert result1[0][0] == 1

        // Test 2: Insert into existing partition (APPEND mode)
        sql """
            insert into ${table_name} values (2, 'bob', 25, 'p1');
        """
        def result2 = sql """
            SELECT * FROM ${table_name} WHERE pt1='p1' ORDER BY id;
        """
        assert result2.size() == 2

        // Test 3: Insert into another new partition
        sql """
            insert into ${table_name} values (3, 'charlie', 30, 'p2');
        """
        def result3 = sql """
            SELECT * FROM ${table_name} ORDER BY id;
        """
        assert result3.size() == 3

        // Test 4: Multiple inserts to verify scheme consistency
        sql """
            insert into ${table_name} values (4, 'david', 35, 'p1'), (5, 'eve', 28, 'p2');
        """
        def result4 = sql """
            SELECT COUNT(*) FROM ${table_name};
        """
        assert result4[0][0] == 5

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

    /*--------test insert overwrite---------*/
    def testInsertOverwrite = { String catalogProperties, String prefix, String dbLocation ->
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

        def table_name = prefix + ThreadLocalRandom.current().nextInt(1000) + "_overwrite_table"

        // Create non-partitioned table for insert overwrite test
        sql """
            CREATE TABLE ${table_name} (
                id INT COMMENT 'id',
                name VARCHAR(20) COMMENT 'name',
                age INT COMMENT 'age'
            ) ENGINE=hive
            PROPERTIES (
                'file_format'='parquet'
            );
        """

        // Test 1: Initial insert
        sql """
            insert into ${table_name} values (1, 'alice', 20), (2, 'bob', 25);
        """
        def result1 = sql """
            SELECT COUNT(*) FROM ${table_name};
        """
        assert result1[0][0] == 2

        // Test 2: Insert overwrite - should replace all data
        sql """
            insert overwrite table ${table_name} values (3, 'charlie', 30);
        """
        def result2 = sql """
            SELECT * FROM ${table_name};
        """
        assert result2.size() == 1
        assert result2[0][0] == 3

        // Test 3: Another insert overwrite with multiple rows
        sql """
            insert overwrite table ${table_name} values (4, 'david', 35), (5, 'eve', 28), (6, 'frank', 40);
        """
        def result3 = sql """
            SELECT COUNT(*) FROM ${table_name};
        """
        assert result3[0][0] == 3

        // Test 4: Verify data integrity after overwrite
        def result4 = sql """
            SELECT * FROM ${table_name} ORDER BY id;
        """
        assert result4.size() == 3
        assert result4[0][0] == 4
        assert result4[1][0] == 5
        assert result4[2][0] == 6

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
    String oss_endpoint = context.config.otherConfigs.get("aliYunEndpoint")
    String bucket = context.config.otherConfigs.get("aliYunBucket")
    String oss_parent_path = "${bucket}/refactor-test"
    String oss_region = context.config.otherConfigs.get("aliYunRegion")
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

    /***************OBS*******************/
    String obs_ak = context.config.otherConfigs.get("hwYunAk")
    String obs_sk = context.config.otherConfigs.get("hwYunSk")
    String obs_endpoint =context.config.otherConfigs.get("hwYunEndpoint")
    String obs_region = context.config.otherConfigs.get("hwYunRegion")
    String obs_region_param = """
              'obs.region' = '${obs_region}',
    """

    String obs_parent_path = context.config.otherConfigs.get("hwYunBucket")

    String obs_storage_properties = """
              'obs.access_key' = '${obs_ak}',
              'obs.secret_key' = '${obs_sk}',
              'obs.endpoint' = '${obs_endpoint}'
    """
    /***************GCS*******************/
    String gcs_ak = context.config.otherConfigs.get("GCSAk")
    String gcs_sk = context.config.otherConfigs.get("GCSSk")
    String gcs_endpoint = "storage.googleapis.com"

    String gcs_parent_path = "selectdb-qa-datalake-test";
    String gcs_storage_old_properties = """
              'gs.access_key' = '${gcs_ak}',
              'gs.secret_key' = '${gcs_sk}',
              'gs.endpoint' = '${gcs_endpoint}'
    """
    String gcs_storage_new_properties = """
              'fs.gcs.support' = 'true',
              'gs.access_key' = '${gcs_ak}',
              'gs.secret_key' = '${gcs_sk}'
            
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
               "oss.endpoint" = "${oss_endpoint}",
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
    //OBS
    String db_location = "obs://${obs_parent_path}/hive/hms/" + System.currentTimeMillis()
    testQueryAndInsert(hms_properties + obs_storage_properties, "hive_hms_obs_test", db_location)
    testQueryAndInsert(hms_properties + obs_region_param + obs_storage_properties, "hive_hms_obs_test_region", db_location)
    testQueryAndInsert(hms_type_properties + hms_kerberos_old_prop + obs_storage_properties, "hive_hms_on_obs_kerberos_old", db_location)
    testQueryAndInsert(hms_type_properties + hms_kerberos_new_prop + obs_storage_properties, "hive_hms_on_obs_kerberos_new", db_location)

    //OBS - Partition table tests
    db_location = "obs://${obs_parent_path}/hive/hms/partition/" + System.currentTimeMillis()
    testPartitionTableInsert(hms_properties + obs_storage_properties, "hive_hms_obs_partition_test", db_location)
    //OBS - Insert overwrite tests (verifies scheme preservation in listDirectories)
    db_location = "obs://${obs_parent_path}/hive/hms/overwrite/" + System.currentTimeMillis()
    testInsertOverwrite(hms_properties + obs_storage_properties, "hive_hms_obs_overwrite_test", db_location)
    //GCS
    if(context.config.otherConfigs.get("enableGCS")){
        db_location = "gs://${gcs_parent_path}/hive/hms/" + System.currentTimeMillis()
        testQueryAndInsert(hms_properties + gcs_storage_old_properties, "hive_hms_gcs_test", db_location)
        testQueryAndInsert(hms_properties + gcs_storage_new_properties, "hive_hms_gcs_test_new", db_location)
        testQueryAndInsert(hms_type_properties + hms_kerberos_old_prop + gcs_storage_old_properties, "hive_hms_on_gcs_kerberos_old", db_location)
        testQueryAndInsert(hms_type_properties + hms_kerberos_new_prop + gcs_storage_new_properties, "hive_hms_on_gcs_kerberos_new", db_location)
        //GCS - Insert overwrite tests
        db_location = "gs://${gcs_parent_path}/hive/hms/overwrite/" + System.currentTimeMillis()
        testInsertOverwrite(hms_properties + gcs_storage_new_properties, "hive_hms_gcs_overwrite_test", db_location)
    }

    //COS
    db_location = "cosn://${cos_parent_path}/hive/hms/" + System.currentTimeMillis()
    testQueryAndInsert(hms_properties + cos_storage_properties, "hive_hms_cos_test", db_location)
    testQueryAndInsert(hms_properties + cos_region_param + cos_storage_properties, "hive_hms_cos_test_region", db_location)
    testQueryAndInsert(hms_type_properties + hms_kerberos_old_prop + cos_storage_properties, "hive_hms_on_cos_kerberos_old", db_location)
    testQueryAndInsert(hms_type_properties + hms_kerberos_new_prop + cos_storage_properties, "hive_hms_on_cos_kerberos_new", db_location)

    //COS - Partition table tests
    db_location = "cosn://${cos_parent_path}/hive/hms/partition/" + System.currentTimeMillis()
    testPartitionTableInsert(hms_properties + cos_storage_properties, "hive_hms_cos_partition_test", db_location)
    //COS - Insert overwrite tests
    db_location = "cosn://${cos_parent_path}/hive/hms/overwrite/" + System.currentTimeMillis()
    testInsertOverwrite(hms_properties + cos_storage_properties, "hive_hms_cos_overwrite_test", db_location)

    db_location = "cos://${cos_parent_path}/hive/hms/" + System.currentTimeMillis()
    testQueryAndInsert(hms_properties + cos_storage_properties, "hive_hms_cos_test", db_location)

    //OSS
    db_location = "oss://${oss_parent_path}/hive/hms/" + System.currentTimeMillis()
    testQueryAndInsert(hms_properties + oss_storage_properties, "hive_hms_oss_test", db_location)
    testQueryAndInsert(hms_properties + oss_region_param + oss_storage_properties, "hive_hms_oss_test_region", db_location)
    testQueryAndInsert(hms_type_properties + hms_kerberos_old_prop + oss_storage_properties, "hive_hms_on_oss_kerberos_old", db_location)
    testQueryAndInsert(hms_type_properties + hms_kerberos_new_prop + oss_storage_properties, "hive_hms_on_oss_kerberos_new", db_location)

    //OSS - Partition table tests (fix for partition path scheme mismatch)
    db_location = "oss://${oss_parent_path}/hive/hms/partition/" + System.currentTimeMillis()
    testPartitionTableInsert(hms_properties + oss_storage_properties, "hive_hms_oss_partition_test", db_location)
    testPartitionTableInsert(hms_properties + oss_region_param + oss_storage_properties, "hive_hms_oss_partition_test_region", db_location)
    //OSS - Insert overwrite tests
    db_location = "oss://${oss_parent_path}/hive/hms/overwrite/" + System.currentTimeMillis()
    testInsertOverwrite(hms_properties + oss_storage_properties, "hive_hms_oss_overwrite_test", db_location)

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
    //S3 - Insert overwrite tests
    db_location = "s3://${s3_parent_path}/hive/hms/overwrite/"+System.currentTimeMillis()
    testInsertOverwrite(hms_properties + s3_storage_properties, "hive_hms_s3_overwrite_test", db_location)
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
    String dlf_endpoint = context.config.otherConfigs.get("dlf_endpoint")
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