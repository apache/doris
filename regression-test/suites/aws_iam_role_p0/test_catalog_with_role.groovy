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

suite("test_catalog_with_role") {
    if (Strings.isNullOrEmpty(context.config.otherConfigs.get("awsLakesRoleArn"))) {
        logger.info("skip ${name} case, because awsRoleArn is null or empty")
        return
    }
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
    String suiteName = "test_catalog_with_role"
    String awsLakersRoleArn = context.config.otherConfigs.get("awsLakesRoleArn")
    String awsLakesRegion = context.config.otherConfigs.get("awsLakesRegion")
    String icebergFSOnS3CatalogWarehouse = context.config.otherConfigs.get("icebergFSOnS3CatalogWarehouse")
    //String hmsMetastoreHost = context.config.hmsMetastoreHost
    //String icebergHmsTableName = context.config.icebergHmsTableName
    String icebergGlueTableName = context.config.otherConfigs.get("icebergGlueTableName")

    String icebergFSOnS3TableName = context.config.otherConfigs.get("icebergFSOnS3TableName")
    //String hiveHmsTableName = context.config.hiveHmsTableName
    String hiveGlueTableName = context.config.otherConfigs.get("hiveGlueTableName")
    String expectCounts = context.config.otherConfigs.get("awsLakesRoleArnQueryExpectCounts")

    String awsS3Property = """
          "s3.region" = "${awsLakesRegion}",
          "s3.role_arn" = "${awsLakersRoleArn}"
    """
    String awsGlueProperties = """
          "glue.region"="${awsLakesRegion}",
          "glue.endpoint" = "https://glue.${awsLakesRegion}.amazonaws.com",
          "glue.role_arn" = "${awsLakersRoleArn}"
    """
    // start test
    String catalogName = "${suiteName}_iceberg_hadoop_on_s3"
    //1. iceberg hadoop catalog on s
    String icebergFSCatalogProps = """
       create catalog if not exists ${catalogName} properties(
         "type"="iceberg",
         "iceberg.catalog.type"="hadoop",
         "warehouse"="${icebergFSOnS3CatalogWarehouse}",
          ${awsS3Property}
       );
     """
    createCatalogAndQuery(icebergFSCatalogProps, catalogName, icebergFSOnS3TableName, expectCounts)
    catalogName = "${suiteName}_iceberg_glue"
    String icebergGlueCatalogProps = """
       create catalog if not exists ${catalogName} properties(
         "type"="iceberg",
         "iceberg.catalog.type"="glue",
            ${awsGlueProperties}
       );
     """
    createCatalogAndQuery(icebergGlueCatalogProps, catalogName, icebergGlueTableName, expectCounts)
    catalogName = "${suiteName}_hive_glue"
    String hiveGlueCatalogProps = """
       create catalog if not exists ${catalogName} properties (
            "type"="hms",
            "hive.metastore.type" = "glue",
            ${awsGlueProperties}
       );
     """
    createCatalogAndQuery(hiveGlueCatalogProps, catalogName, hiveGlueTableName, expectCounts)
/*********** HMS ***************/
// The following HMS-related cases are commented out because 
// they require a Docker environment for the Hive Metastore service.

/*
String catalogName = "${suiteName}iceberg_hive_on_s3"
String icebergHmsCatalogProps = """
    create catalog if not exists ${catalogName} properties (
        "type" = "iceberg",
        "iceberg.catalog.type" = "hive",
        "hive.metastore.uris" = "thrift://${hmsMetastoreHost}:9083",
        ${awsS3Property}
    );
"""
createCatalogAndQuery(icebergHmsCatalogProps, catalogName, icebergHmsTableName, expectCounts)

String hiveHmsCatalogProps = """
    create catalog if not exists ${catalogName} properties (
        "type" = "hms",
        "hive.metastore.uris" = "thrift://${hmsMetastoreHost}:9083",
        ${awsS3Property}
    );
"""
createCatalogAndQuery(hiveHmsCatalogProps, catalogName, hiveHmsTableName, expectCounts)
*/


}