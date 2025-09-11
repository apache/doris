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
suite("iceberg_on_hms_and_filesystem_and_dlf", "p2,external,new_catalog_property") {


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
        def branch_name = prefix + "_branch"
        def tag_name = prefix + "_tag"
        sql """
            ALTER TABLE ${table_name} CREATE BRANCH ${branch_name};
        """
        sql """
            ALTER TABLE ${table_name} CREATE TAG ${tag_name};
        """
        sql """
            INSERT OVERWRITE  TABLE ${table_name} VALUES (1, 'a', 10),(2, 'b', 20), (3, 'c', 30)
        """
        def originalQueryResult = sql """
            SELECT * FROM ${table_name};
        """
        assert originalQueryResult.size() == 3
        sql """
            insert into ${table_name}@branch(${branch_name}) values (4, 'd', 40)
        """
        def branchQueryResult = sql """
            SELECT * FROM ${table_name}@branch(${branch_name});
        """
        assert branchQueryResult.size() == 2


        def tagQueryResult = sql """
            SELECT * FROM ${table_name}@tag(${tag_name});
        """
        assert tagQueryResult.size() == 1
        sql """
            ALTER TABLE ${table_name} drop branch ${branch_name};
        """
        sql """
            ALTER TABLE ${table_name} drop tag ${tag_name};
        """
        try {
            def sys_query_result = sql """
            SELECT * FROM ${table_name}\$files;
        """
            println sys_query_result
            println "iceberg_meta_result SUCCESS" + catalog_name

            def iceberg_meta_result = sql """
        SELECT snapshot_id FROM iceberg_meta(
                'table' = '${catalog_name}.${db_name}.${table_name}',
                'query_type' = 'snapshots'
        ) order by committed_at desc;
        
        """
            def first_snapshot_id = iceberg_meta_result.get(0).get(0);
            def time_travel =sql """
            SELECT * FROM ${table_name} FOR VERSION AS OF ${first_snapshot_id};
        """
            println time_travel

            println "iceberg_time_travel SUCCESS" + catalog_name
        }catch (Exception e) {
            println catalog_name + "system info error"
        }


        sql """
            DROP TABLE ${table_name};
        """
        //partition table
        table_name = prefix + "_partition_table"
        sql """
            CREATE TABLE ${table_name} (
              `ts` DATETIME COMMENT 'ts',
              `col1` BOOLEAN COMMENT 'col1',
              `col2` INT COMMENT 'col2',
              `col3` BIGINT COMMENT 'col3',
              `col4` FLOAT COMMENT 'col4',
              `col5` DOUBLE COMMENT 'col5',
              `col6` DECIMAL(9,4) COMMENT 'col6',
              `col7` STRING COMMENT 'col7',
              `col8` DATE COMMENT 'col8',
              `col9` DATETIME COMMENT 'col9',
              `pt1` STRING COMMENT 'pt1',
              `pt2` STRING COMMENT 'pt2'
            )
            PARTITION BY LIST (day(ts), pt1, pt2) ()
            PROPERTIES (
              'write-format'='orc',
              'compression-codec'='zlib'
            );
        """

        sql """
            INSERT OVERWRITE  TABLE ${table_name} values 
            ('2023-01-01 00:00:00', true, 1, 1, 1.0, 1.0, 1.0000, '1', '2023-01-01', '2023-01-01 00:00:00', 'a', '1'),
            ('2023-01-02 00:00:00', false, 2, 2, 2.0, 2.0, 2.0000, '2', '2023-01-02', '2023-01-02 00:00:00', 'b', '2'),
            ('2023-01-03 00:00:00', true, 3, 3, 3.0, 3.0, 3.0000, '3', '2023-01-03', '2023-01-03 00:00:00', 'c', '3');
        """
        def partitionQueryResult = sql """
            SELECT * FROM ${table_name} WHERE pt1='a' and pt2='1';
        """
        assert partitionQueryResult.size() == 1

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
    String hdfs_parent_path = "hdfs://${externalEnvIp}:8320/user/iceberg/warehouse"
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

    String hms_prop = """
              'hive.metastore.uris' = 'thrift://${externalEnvIp}:9383',
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

    String hms_kerberos_old_prop_not_include_kerberos_prop = """
                "hive.metastore.uris" = "thrift://${externalEnvIp}:9583",
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
    //**************** HMS START*******************/


    String iceberg_hms_type_prop = """
                 'type'='iceberg',
                 'iceberg.catalog.type'='hms',
    """
    /*--------HMS on OSS-----------*/
    String warehouse = """
                  'warehouse' = 'oss://${oss_parent_path}/iceberg-hms-warehouse',
    """
    // has endpoint not region
    testQueryAndInsert(iceberg_hms_type_prop + hms_prop
            + warehouse + oss_storage_properties, "iceberg_hms_on_oss")

    testQueryAndInsert(iceberg_hms_type_prop + hms_prop
            + warehouse + oss_region_param + oss_storage_properties, "iceberg_hms_on_oss")

    //old kerberos
    testQueryAndInsert(iceberg_hms_type_prop + hms_kerberos_old_prop + warehouse + oss_storage_properties, "iceberg_hms_on_oss_kerberos_old")
    //new kerberos
    testQueryAndInsert(iceberg_hms_type_prop + hms_kerberos_new_prop+ warehouse + oss_storage_properties, "iceberg_hms_on_oss_kerberos_new")


    /*--------HMS on COS-----------*/
    warehouse = """
                   'warehouse' = 'cosn://${cos_parent_path}/iceberg-hms-cos-warehouse',
    """
    testQueryAndInsert(iceberg_hms_type_prop + hms_prop
            + warehouse + cos_storage_properties, "iceberg_hms_on_cos")

    testQueryAndInsert(iceberg_hms_type_prop + hms_prop
            + warehouse + cos_region_param + cos_storage_properties, "iceberg_hms_on_cos")

    warehouse = """
     'warehouse' = 'cos://${cos_parent_path}/iceberg-hms-cos-warehouse',
    """
    testQueryAndInsert(iceberg_hms_type_prop + hms_prop
            + warehouse + cos_storage_properties, "iceberg_hms_on_cos")
    //kerberos
    testQueryAndInsert(iceberg_hms_type_prop + hms_kerberos_old_prop + warehouse + cos_storage_properties, "iceberg_hms_on_cos_kerberos_old")
    testQueryAndInsert(iceberg_hms_type_prop + hms_kerberos_new_prop + warehouse + cos_storage_properties, "iceberg_hms_on_cos_kerberos_new")



    /*--------HMS on S3-----------*/

    warehouse = """
       'warehouse' = 's3a://${s3_parent_path}/iceberg-hms-s3-warehouse',
      """
    testQueryAndInsert(iceberg_hms_type_prop + hms_prop
            + warehouse + s3_storage_properties, "iceberg_hms_on_s3")
    testQueryAndInsert(iceberg_hms_type_prop + hms_prop
            + warehouse + s3_region_param + s3_storage_properties, "iceberg_hms_on_s3")
    //kerberos Docker HMS c
    // testQueryAndInsert(iceberg_hms_type_prop + hms_kerberos_old_prop+ warehouse + s3_storage_properties, "iceberg_hms_on_s3_kerberos_old")
    // testQueryAndInsert(iceberg_hms_type_prop + hms_kerberos_new_prop + warehouse + s3_storage_properties, "iceberg_hms_on_s3_kerberos_new")
    warehouse = """
       'warehouse' = 's3://${s3_parent_path}/iceberg-hms-s3-warehouse',
      """
    testQueryAndInsert(iceberg_hms_type_prop + hms_prop
            + warehouse + s3_storage_properties, "iceberg_hms_on_s3")

    /*--------HMS on HDFS-----------*/
    warehouse = """
     'warehouse' = '${hdfs_parent_path}/iceberg-hms-hdfs-warehouse',
    """
    testQueryAndInsert(iceberg_hms_type_prop + hms_prop
            + warehouse + hdfs_properties, "iceberg_hms_on_hdfs")
    warehouse = """
     'warehouse' = 'hdfs://${externalEnvIp}:8520/iceberg-hms-hdfs-warehouse',
    """
    //old kerberos
    testQueryAndInsert(iceberg_hms_type_prop + hms_kerberos_old_prop_not_include_kerberos_prop+ warehouse + hdfs_kerberos_properties, "iceberg_hms_on_hdfs_kerberos_old")
    //new  kerberos
    testQueryAndInsert(iceberg_hms_type_prop + hms_kerberos_new_prop + warehouse + hdfs_kerberos_properties, "iceberg_hms_on_hdfs_kerberos_hdfs")


    /*--------HMS END-----------*/


    /*--------FILESYSTEM START -----------*/
    String iceberg_file_system_catalog_properties = """
     'type'='iceberg',
     'iceberg.catalog.type'='hadoop',
    """
    /**  COS **/
    warehouse = """
     'warehouse' = 'cos://${cos_parent_path}/iceberg-fs-cos-warehouse',
    """
    testQueryAndInsert(iceberg_file_system_catalog_properties + warehouse + cos_storage_properties, "iceberg_fs_on_cos")
    testQueryAndInsert(iceberg_file_system_catalog_properties + warehouse + cos_region_param + cos_storage_properties, "iceberg_fs_on_cos_region")
    warehouse = """
     'warehouse' = 'cosn://${cos_parent_path}/iceberg-fs-cos-warehouse',
    """
    testQueryAndInsert(iceberg_file_system_catalog_properties + warehouse + cos_storage_properties, "iceberg_fs_on_cos")
    testQueryAndInsert(iceberg_file_system_catalog_properties + warehouse + cos_region_param + cos_storage_properties, "iceberg_fs_on_cos_region")

    /**  OSS **/
    warehouse = """
        'warehouse' = 'oss://${oss_parent_path}/iceberg-fs-oss-warehouse',
        """
    testQueryAndInsert(iceberg_file_system_catalog_properties + warehouse + oss_storage_properties, "iceberg_fs_on_oss")
    testQueryAndInsert(iceberg_file_system_catalog_properties + warehouse + oss_region_param + oss_storage_properties, "iceberg_fs_on_oss_region")
    /**  HDFS **/
    warehouse = """
        'warehouse' = '${hdfs_parent_path}/iceberg-fs-hdfs-warehouse',
    """
    testQueryAndInsert(iceberg_file_system_catalog_properties + warehouse + hdfs_properties, "iceberg_fs_on_hdfs")
    //kerberos
    warehouse = """
        'warehouse' = 'hdfs://${externalEnvIp}:8520/iceberg-fs-hdfs-warehouse',
    """
    testQueryAndInsert(iceberg_file_system_catalog_properties + warehouse + hdfs_kerberos_properties, "iceberg_fs_on_hdfs_kerberos")


    /*  *//**  S3   **/
    warehouse = """
        'warehouse' = 's3://${s3_parent_path}/iceberg-fs-s3-warehouse',
    """
    testQueryAndInsert(iceberg_file_system_catalog_properties + warehouse + s3_storage_properties, "iceberg_fs_on_glue")
    testQueryAndInsert(iceberg_file_system_catalog_properties + warehouse + s3_region_param + s3_storage_properties, "iceberg_fs_on_glue_region")
    warehouse = """
        'warehouse' = 's3a://${s3_parent_path}/iceberg-fs-s3-warehouse',
    """
    testQueryAndInsert(iceberg_file_system_catalog_properties + warehouse + s3_storage_properties, "iceberg_fs_on_glue")
    testQueryAndInsert(iceberg_file_system_catalog_properties + warehouse + s3_region_param + s3_storage_properties, "iceberg_fs_on_glue_region")

    /*--------FILESYSTEM END-----------*/



    String dlf_access_key = context.config.otherConfigs.get("dlf_access_key")
    String dlf_secret_key = context.config.otherConfigs.get("dlf_secret_key")
/**************** DLF *******************/
    String dlf_warehouse = "oss://selectdb-qa-datalake-test/hive-dlf-oss-warehouse"
    String hive_dlf_type_properties = """
            'type'='hms',
            'hive.metastore.type'='dlf',
            """
    String dlf_catalog_base_properties = """
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
    String dlf_oss_properties = """
               "oss.endpoint" = "oss-cn-beijing.aliyuncs.com",
               "oss.access_key" = "${dlf_access_key}",
               "oss.secret_key" = "${dlf_secret_key}",
    """
    /*ICEBERG DLF*/
    String iceberg_dlf_type_properties = """
            'type'='iceberg',
            'iceberg.catalog.type'='dlf',
            'warehouse'='${dlf_warehouse}/iceberg/dlf-oss/',
            """

    String iceberg_dlf_type_properties_no_warehouse = """
            'type'='iceberg',
            'iceberg.catalog.type'='dlf',
            """

    testQuery(iceberg_dlf_type_properties + dlf_region_param +  dlf_catalog_base_properties, "iceberg_dlf_catalog", "regression_iceberg", "tb_simple", 2)

    testQuery(iceberg_dlf_type_properties+dlf_region_param+dlf_endpoint_properties + dlf_catalog_base_properties, "iceberg_dlf_catalog", "regression_iceberg", "tb_simple", 2)
    testQuery(iceberg_dlf_type_properties+dlf_oss_properties + dlf_region_param + dlf_catalog_base_properties, "iceberg_dlf_catalog", "regression_iceberg", "tb_simple", 2)
    testQuery(iceberg_dlf_type_properties_no_warehouse+dlf_oss_properties + dlf_region_param + dlf_catalog_base_properties, "iceberg_dlf_catalog", "regression_iceberg", "tb_simple", 2)

    // region is required
    shouldFail {
        testQuery(iceberg_dlf_type_properties+dlf_catalog_base_properties, "iceberg_dlf_catalog", "regression_iceberg", "tb_simple", 2)
    }


}