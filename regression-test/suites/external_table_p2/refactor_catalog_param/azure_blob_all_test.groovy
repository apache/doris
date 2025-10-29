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

suite("azure_blob_all_test", "p2,external,new_catalog_property") {


    String abfsAzureAccountName = context.config.otherConfigs.get("abfsAccountName")
    String abfsAzureAccountKey = context.config.otherConfigs.get("abfsAccountKey")
    String abfsContainer = context.config.otherConfigs.get("abfsContainer")
    String abfsEndpoint = context.config.otherConfigs.get("abfsEndpoint")
    def abfs_azure_config_props = """
        "provider" = "azure",
        "azure.endpoint"="${abfsEndpoint}",
        "azure.account_name" = "${abfsAzureAccountName}",
        "azure.account_key" = "${abfsAzureAccountKey}" 
    """
    
    // Iceberg FS 
    
    def testIcebergTest = { String storage_props,String iceberg_fs_catalog_name, String protocol,String hdfsLocationType ->
        
        sql """
         drop catalog if exists ${iceberg_fs_catalog_name};
        """
        sql"""
        create catalog ${iceberg_fs_catalog_name} properties(
        'type'='iceberg',
        'iceberg.catalog.type'='hadoop',
        'warehouse'='${protocol}://${abfsContainer}@${abfsAzureAccountName}.${hdfsLocationType}.core.windows.net/regression/external/azure/${protocol}/iceberg_fs_warehouse/',
        ${storage_props}
        );
        """

        sql """ 
         switch ${iceberg_fs_catalog_name} 
            """

        sql """
        drop database if exists ${iceberg_fs_catalog_name}_db_test;
        """
        sql """
        create database ${iceberg_fs_catalog_name}_db_test;
    """
        sql """
        use ${iceberg_fs_catalog_name}_db_test;
    """
        sql """
        create table ${iceberg_fs_catalog_name}_table_test (id int, name string)
    """
        sql """
        insert into ${iceberg_fs_catalog_name}_table_test values(1, 'iceberg_fs_abfs_test');
    """
        def query_result = sql """
        select count(1) from ${iceberg_fs_catalog_name}_table_test;
    """

        assert query_result[0][0] == 1

        sql """
        drop table if exists ${iceberg_fs_catalog_name}_table_test;
    """
        sql """
        drop database if exists ${iceberg_fs_catalog_name}_db_test;
    """
        sql """
        drop catalog if exists ${iceberg_fs_catalog_name};
        """
    }


    //abfs
    testIcebergTest(abfs_azure_config_props, "iceberg_fs_abfs_catalog", "abfs","dfs")
    testIcebergTest(abfs_azure_config_props, "iceberg_fs_abfss_catalog", "abfss","dfs")
    
   
    
    //abfss
    def testPaimonTest = { String storage_props,String paimon_catalog_name, String protocol,String hdfsLocationType,String queryTbl ->
        sql """
         drop catalog if exists ${paimon_catalog_name};
        """
        sql"""
        create catalog ${paimon_catalog_name} properties(
        'type'='paimon',
        'paimon.catalog.type'='filesystem',
        'warehouse'='${protocol}://${abfsContainer}@${abfsAzureAccountName}.${hdfsLocationType}.core.windows.net/regression/azure/${protocol}/paimon_fs_warehouse/',
        ${abfs_azure_config_props}
    );
    """

        sql """ 
         switch ${paimon_catalog_name} 
    """

        def query_result =sql """
        select * from ${paimon_catalog_name}.${queryTbl}
        """
        println query_result

        sql """
         drop catalog if exists ${paimon_catalog_name};
        """
    }
    
    // Paimon FS
    sql """
    set force_jni_scanner=false;
    """
  
    def paimon_fs_abfss_db_tbl = "paimon_fs_abfss_test_db.external_test_table"
    def paimon_fs_abfs_db_tbl = "paimon_fs_abfs_test_db.external_test_table"
    testPaimonTest(abfs_azure_config_props, "paimon_fs_abfs_catalog", "abfs","dfs",paimon_fs_abfs_db_tbl)
    testPaimonTest(abfs_azure_config_props, "paimon_fs_abfss_catalog", "abfss","dfs",paimon_fs_abfss_db_tbl)

    // TODO: Enable this once BE's HDFS dependency management is fully ready.
    //       This module requires higher-version JARs to support JDK 17 access.
   /*    sql """
    set force_jni_scanner=true;
    """
    testPaimonTest(abfs_azure_config_props, "paimon_fs_abfs_catalog", "abfs","dfs",paimon_fs_abfs_db_tbl)
    testPaimonTest(abfs_azure_config_props, "paimon_fs_abfss_catalog", "abfss","dfs",paimon_fs_abfss_db_tbl)*/
    
    
}