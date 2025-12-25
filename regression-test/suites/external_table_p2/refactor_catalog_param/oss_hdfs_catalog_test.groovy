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

suite("oss_hdfs_catalog_test", "p2,external,new_catalog_property") {
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
        if (dbLocation == null) {
            sql """
            CREATE DATABASE IF NOT EXISTS ${db_name}

        """
        }else {
        sql """
            CREATE DATABASE IF NOT EXISTS ${db_name}
            PROPERTIES ('location'='${dbLocation}');

        """

        }

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
    
    def testQuery = { String catalog_name,String catalogProperties,String queryTable,String expectedCount, boolean force_jni_scanner = false ->
        
        sql """
            DROP CATALOG IF EXISTS ${catalog_name};
        """
        if (force_jni_scanner) {
            sql """ set force_jni_scanner=true; """
        }else {
            sql """ set force_jni_scanner=false; """
        }
        sql """
            CREATE CATALOG IF NOT EXISTS ${catalog_name} PROPERTIES (
                ${catalogProperties}
            );
        """
        sql """
            switch ${catalog_name};
        """

        def queryResult = sql """
            SELECT count(1) FROM ${queryTable};
        """
        assert queryResult[0][0] == expectedCount.toInteger()

        sql """
            DROP CATALOG IF EXISTS ${catalog_name};
        """
    }

    /****************OSS_HDFS*******************/
    String oss_hdfs_ak = context.config.otherConfigs.get("ossHdfsAk")
    String oss_hdfs_sk = context.config.otherConfigs.get("ossHdfsSk")
    String oss_hdfs_endpoint = context.config.otherConfigs.get("ossHdfsEndpoint")
    String oss_hdfs_bucket = context.config.otherConfigs.get("ossHdfsBucket")
    String oss_hdfs_parent_path = "${oss_hdfs_bucket}/refactor-test"
    String oss_hdfs_region = context.config.otherConfigs.get("ossHdfsRegion")
    

    String old_oss_hdfs_storage_properties = """
              'oss.access_key' = '${oss_hdfs_ak}',
              'oss.secret_key' = '${oss_hdfs_sk}',
              'oss.endpoint' = '${oss_hdfs_endpoint}'
             

    """
    String usingOSSHDFSProps="""
          'oss.hdfs.enabled'='true',
    """
    String new_oss_hdfs_storage_properties = """
              'fs.oss.support' = 'true',
              'oss.hdfs.access_key' = '${oss_hdfs_ak}',
              'oss.hdfs.secret_key' = '${oss_hdfs_sk}',
              'oss.hdfs.endpoint' = '${oss_hdfs_endpoint}',
              'oss.hdfs.region'='${oss_hdfs_region}'
    """
    //**************** Paimon DLF ON OSS_HDFS *******************/

    String query_table_paimon_dlf = context.config.otherConfigs.get("paimonDlfWarehouseOnOssHdfsQueryTable")
    String query_count_paimon_dlf = context.config.otherConfigs.get("paimonDlfWarehouseOnOssHdfsQueryCount")
    String paimon_dlf_old_catalog_properties = context.config.otherConfigs.get("paimonDlfOnOssHdfsCatalogOldProperties")
    String paimon_dlf_new_catalog_properties1 = context.config.otherConfigs.get("paimonDlfOnOssHdfsCatalogNewProperties1")
    String paimon_dlf_new_catalog_properties2 = context.config.otherConfigs.get("paimonDlfOnOssHdfsCatalogNewProperties2")

    testQuery("paimon_dlf_oss_hdfs_old_catalog",paimon_dlf_old_catalog_properties ,query_table_paimon_dlf,query_count_paimon_dlf,true)
    testQuery("paimon_dlf_oss_hdfs_old_catalog",paimon_dlf_old_catalog_properties ,query_table_paimon_dlf,query_count_paimon_dlf,false)
    testQuery("paimon_dlf_oss_hdfs_new_catalog1",paimon_dlf_new_catalog_properties1 ,query_table_paimon_dlf,query_count_paimon_dlf,true)
    testQuery("paimon_dlf_oss_hdfs_new_catalog1",paimon_dlf_new_catalog_properties1 ,query_table_paimon_dlf,query_count_paimon_dlf,false)
    testQuery("paimon_dlf_oss_hdfs_new_catalog2",paimon_dlf_new_catalog_properties2 ,query_table_paimon_dlf,query_count_paimon_dlf,true)
    testQuery("paimon_dlf_oss_hdfs_new_catalog2",paimon_dlf_new_catalog_properties2 ,query_table_paimon_dlf,query_count_paimon_dlf,false)

    //**************** Paimon FILESYSTEM ON OSS_HDFS *******************/
    String paimon_fs_warehouse = context.config.otherConfigs.get("paimonFsWarehouseOnOssHdfs")
    String query_table_paimon_fs = context.config.otherConfigs.get("paimonFsWarehouseOnOssHdfsQueryTable")
    String query_count_paimon_fs = context.config.otherConfigs.get("paimonFsWarehouseOnOssHdfsQueryCount")
    String paimon_file_system_catalog_properties = """
     'type'='paimon',
     'paimon.catalog.type'='filesystem',
     'warehouse'='${paimon_fs_warehouse}',
    """
    testQuery("paimon_fs_oss_hdfs_catalog",paimon_file_system_catalog_properties + old_oss_hdfs_storage_properties,query_table_paimon_fs,query_count_paimon_fs,true)
    testQuery("paimon_fs_oss_hdfs_catalog",paimon_file_system_catalog_properties + old_oss_hdfs_storage_properties,query_table_paimon_fs,query_count_paimon_fs,false)
    testQuery("paimon_fs_oss_hdfs_region_catalog",paimon_file_system_catalog_properties + usingOSSHDFSProps + old_oss_hdfs_storage_properties,query_table_paimon_fs,query_count_paimon_fs,true)
    testQuery("paimon_fs_oss_hdfs_region_catalog",paimon_file_system_catalog_properties + usingOSSHDFSProps + old_oss_hdfs_storage_properties,query_table_paimon_fs,query_count_paimon_fs,false)
    testQuery("paimon_fs_oss_hdfs_new_catalog",paimon_file_system_catalog_properties + new_oss_hdfs_storage_properties,query_table_paimon_fs,query_count_paimon_fs,true)
    testQuery("paimon_fs_oss_hdfs_new_catalog",paimon_file_system_catalog_properties + new_oss_hdfs_storage_properties,query_table_paimon_fs,query_count_paimon_fs,false)

    //**************** ICEBERG FILESYSTEM ON OSS_HDFS *******************/
    String iceberg_file_system_catalog_properties = """
     'type'='iceberg',
     'iceberg.catalog.type'='hadoop',
    """

    String warehouse = """
     'warehouse' = 'oss://${oss_hdfs_parent_path}/iceberg-fs-oss-hdfs-warehouse',
    """
    testQueryAndInsert(iceberg_file_system_catalog_properties + warehouse + old_oss_hdfs_storage_properties, "iceberg_fs_on_oss_hdfs",null)

    testQueryAndInsert(iceberg_file_system_catalog_properties + warehouse + usingOSSHDFSProps +old_oss_hdfs_storage_properties, "iceberg_fs_on_oss_hdfs_region",null)

    testQueryAndInsert(iceberg_file_system_catalog_properties + warehouse + new_oss_hdfs_storage_properties, "iceberg_fs_on_oss_hdfs",null)

    String enable_oss_hdfs_hms_catalog_test = context.config.otherConfigs.get("enableOssHdfsHmsCatalogTest")
    if (enable_oss_hdfs_hms_catalog_test == null || !enable_oss_hdfs_hms_catalog_test.equalsIgnoreCase("true")) {
        return
    }

    /*HMS props*/
    String hms_props = context.config.otherConfigs.get("emrHmsProps")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsuri ="thrift://${externalEnvIp}:9083"
    String hms_properties = """
        "type"="hms",
        ${hms_props},
    """
    //**************** HIVE HMS ON OSS_HDFS *******************/

    String db_location
    //OSS-HDFS
    db_location = "oss://${oss_hdfs_parent_path}/hive/hms/" + System.currentTimeMillis()
    testQueryAndInsert(hms_properties + old_oss_hdfs_storage_properties, "hive_hms_oss_hdfs_test_old", db_location)
    db_location = "oss://${oss_hdfs_parent_path}/hive/hms/" + System.currentTimeMillis()
    testQueryAndInsert(hms_properties +usingOSSHDFSProps+ old_oss_hdfs_storage_properties, "hive_hms_oss_hdfs_test_old", db_location)
    db_location = "oss://${oss_hdfs_parent_path}/hive/hms/" + System.currentTimeMillis()
    testQueryAndInsert(hms_properties + new_oss_hdfs_storage_properties, "hive_hms_oss_hdfs_test_new", db_location)

    //**************** ICEBERG HMS ON OSS_HDFS *******************/
    String iceberg_hms_type_prop = """
                 'type'='iceberg',
                 'iceberg.catalog.type'='hms',
                 'hive.metastore.uris'='${hmsuri}',
                 
    """

    // Basic HMS with OSS storage
     warehouse = """
                  'warehouse' = 'oss://${oss_hdfs_parent_path}/iceberg-hms-oss-hdfs-warehouse/',
    """
    testQueryAndInsert(iceberg_hms_type_prop  + warehouse + old_oss_hdfs_storage_properties, "iceberg_hms_on_oss_hdfs_old",null)
    testQueryAndInsert(iceberg_hms_type_prop  + oss_hdfs_endpoint + warehouse + old_oss_hdfs_storage_properties, "iceberg_hms_on_oss_hdfs_old",null)

    testQueryAndInsert(iceberg_hms_type_prop  + warehouse + new_oss_hdfs_storage_properties , "iceberg_hms_on_oss_hdfs_new",null)
    
}