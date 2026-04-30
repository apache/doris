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

suite("test_iceberg_s3tables_catalog_credentials_provider") {

    List<String> requiredConfigNames = [
            "icebergS3TablesCatalog1Props",
            "icebergS3TablesCatalog2Props",
            "icebergS3TablesCatalog3Props",
            "icebergS3TablesPartitionDbTableName",
            "icebergS3TablesNonPartitionDbTableName",
            "icebergS3TablesExpectCounts"
    ]
    if (requiredConfigNames.any { Strings.isNullOrEmpty(context.config.otherConfigs.get(it)) }) {
        return
    }

    sql """ ADMIN SET FRONTEND CONFIG ("aws_credentials_provider_version"="v2"); """

    String catalog1Props = context.config.otherConfigs.get("icebergS3TablesCatalog1Props")
    String catalog2Props = context.config.otherConfigs.get("icebergS3TablesCatalog2Props")
    String catalog3Props = context.config.otherConfigs.get("icebergS3TablesCatalog3Props")
    String partitionDbTableName = context.config.otherConfigs.get("icebergS3TablesPartitionDbTableName")
    String nonPartitionDbTableName = context.config.otherConfigs.get("icebergS3TablesNonPartitionDbTableName")
    String expectCounts = context.config.otherConfigs.get("icebergS3TablesExpectCounts")
    String expectedWebIdentityError = "Either the environment variable AWS_WEB_IDENTITY_TOKEN_FILE "
            + "or the javaproperty aws.webIdentityTokenFile must be set."

    def createCatalogAndQueryTables = {
            catalogProps, catalogName, partitionQueryDbTableName, nonPartitionQueryDbTableName, expectedCounts ->
        sql """drop catalog if exists ${catalogName}"""
        try {
            sql """
                create catalog ${catalogName} properties (
                    ${catalogProps}
                );
            """
            def partitionResult = sql """
                select count(1) from ${catalogName}.${partitionQueryDbTableName};
            """
            def nonPartitionResult = sql """
                select count(1) from ${catalogName}.${nonPartitionQueryDbTableName};
            """
            def partitionCount = partitionResult[0][0]
            def nonPartitionCount = nonPartitionResult[0][0]
            assertTrue(partitionCount == expectedCounts.toInteger())
            assertTrue(nonPartitionCount == expectedCounts.toInteger())
            assertTrue(partitionCount == nonPartitionCount)
        } finally {
            sql """drop catalog if exists ${catalogName}"""
        }
    }

    def createCatalogAndShowDatabasesException = { catalogProps, catalogName, expectedMessage ->
        sql """drop catalog if exists ${catalogName}"""
        try {
            sql """
                create catalog ${catalogName} properties (
                    ${catalogProps}
                );
            """
            sql """switch ${catalogName};"""
            try {
                sql """show databases;"""
                assertTrue(false)
            } catch (Exception e) {
                assertTrue(e.getMessage().contains(expectedMessage))
            }
        } finally {
            sql """drop catalog if exists ${catalogName}"""
        }
    }

    createCatalogAndQueryTables(
            catalog1Props,
            "iceberg_s3tables_catalog_1",
            partitionDbTableName,
            nonPartitionDbTableName,
            expectCounts)
    createCatalogAndQueryTables(
            catalog2Props,
            "iceberg_s3tables_catalog_2",
            partitionDbTableName,
            nonPartitionDbTableName,
            expectCounts)
    createCatalogAndShowDatabasesException(
            catalog3Props,
            "iceberg_s3tables_catalog_3",
            expectedWebIdentityError)
}
