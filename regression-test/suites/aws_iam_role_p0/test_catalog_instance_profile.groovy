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

import com.google.common.base.Strings;

suite("test_catalog_instance_profile_with_role") {

    if (Strings.isNullOrEmpty(context.config.otherConfigs.get("hiveGlueInstanceProfileQueryTableName"))) {
        return
    }
    sql """ ADMIN SET FRONTEND CONFIG ("aws_credentials_provider_version"="v2"); """
    String hiveGlueQueryTableName = context.config.otherConfigs.get("hiveGlueInstanceProfileQueryTableName")
    String hiveGlueExpectCounts = context.config.otherConfigs.get("hiveGlueInstanceProfileExpectCounts")
    String icebergFsQueryTableName = context.config.otherConfigs.get("icebergFsInstanceProfileQueryTableName")
    String icebergFsExpectCounts = context.config.otherConfigs.get("icebergFsInstanceProfileExpectCounts")
    String icebergFsWarehouse = context.config.otherConfigs.get("icebergFsInstanceProfileWarehouse")
    String region = context.config.otherConfigs.get("awsInstanceProfileRegion")
    //query method
    def createCatalogAndQuery = { catalogProps, catalogName, queryTableName, expectCounts ->
        sql """drop catalog if exists ${catalogName}"""
        sql """
            ${catalogProps}
        """
        def result = sql """
         select count(1) from ${catalogName}.${queryTableName};
        """
        println("result: ${result}")
        def countValue = result[0][0]
        assertTrue(countValue == expectCounts.toInteger())
        sql """drop catalog if exists ${catalogName}"""
    }
    def assertCatalogAndQueryException = { catalogProps, catalogName, queryTableName ->
        sql """drop catalog if exists ${catalogName}"""
        sql """
            ${catalogProps}
        """
        boolean failed = false
        try {
            sql """
         switch ${catalogName};
        """
            sql """
                show databases;
               """
            sql """
             select count(1) from ${catalogName}.${queryTableName};
            """
        }catch (Exception e){
            failed = true
        }
        assertTrue(failed)
    }
    String hiveGlueCatalogProps = """
        create catalog hive_glue_catalog_instance_profile properties(
            "type"="hms",
            "hive.metastore.type"="glue",
            "glue.region"="${region}",
            "glue.endpoint" = "https://glue.${region}.amazonaws.com"
        );
    """
    createCatalogAndQuery(hiveGlueCatalogProps, "hive_glue_catalog_instance_profile", hiveGlueQueryTableName, hiveGlueExpectCounts)
    hiveGlueCatalogProps = """
        create catalog hive_glue_catalog_instance_profile properties(
            "type"="hms",
            "hive.metastore.type"="glue",
            "glue.credentials_provider_type"="INSTANCE_PROFILE",
            "glue.region"="${region}",
            "glue.endpoint" = "https://glue.${region}.amazonaws.com"
        );
    """
    createCatalogAndQuery(hiveGlueCatalogProps, "hive_glue_catalog_instance_profile", hiveGlueQueryTableName, hiveGlueExpectCounts)
    hiveGlueCatalogProps = """
        create catalog hive_glue_catalog_instance_profile properties(
            "type"="hms",
            "hive.metastore.type"="glue",
            "glue.credentials_provider_type"="CONTAINER",
            "glue.region"="${region}",
            "glue.endpoint" = "https://glue.${region}.amazonaws.com"
        );
    """
    assertCatalogAndQueryException(hiveGlueCatalogProps,"hive_glue_catalog_instance_profile", hiveGlueQueryTableName)
    String icebergFsCatalogProps = """
        create catalog iceberg_fs_catalog_instance_profile properties(
            "type"="iceberg",
            "warehouse"="${icebergFsWarehouse}",
            "iceberg.catalog.type"="hadoop",
            "s3.region" = "${region}",
            "s3.endpoint" = "https://s3.${region}.amazonaws.com"
        );
    """
    createCatalogAndQuery(icebergFsCatalogProps, "iceberg_fs_catalog_instance_profile", icebergFsQueryTableName, icebergFsExpectCounts)
    icebergFsCatalogProps = """
        create catalog iceberg_fs_catalog_instance_profile properties(
            "type"="iceberg",
            "warehouse"="${icebergFsWarehouse}",
            "iceberg.catalog.type"="hadoop",
            "s3.credentials_provider_type"="INSTANCE_PROFILE",
            "s3.region" = "${region}",
            "s3.endpoint" = "https://s3.${region}.amazonaws.com"
        );
    """
    createCatalogAndQuery(icebergFsCatalogProps, "iceberg_fs_catalog_instance_profile", icebergFsQueryTableName, icebergFsExpectCounts)
    icebergFsCatalogProps = """
        create catalog iceberg_fs_catalog_instance_profile properties(
            "type"="iceberg",
            "warehouse"="${icebergFsWarehouse}",
            "iceberg.catalog.type"="hadoop",
            "s3.credentials_provider_type"="CONTAINER",
            "s3.region" = "${region}",
            "s3.endpoint" = "https://s3.${region}.amazonaws.com"
        );
    """
    assertCatalogAndQueryException(icebergFsCatalogProps,"iceberg_fs_catalog_instance_profile",icebergFsQueryTableName)
    
}
