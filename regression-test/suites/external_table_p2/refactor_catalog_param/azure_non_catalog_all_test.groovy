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
suite("azure_non_catalog_all_test", "p2,external,new_catalog_property") {

    // create internal table
    def createDBAndTbl = { String dbName , String table->

        sql """
                drop database if exists ${dbName}
            """

        sql """
            create database ${dbName}
        """

        sql """
             use ${dbName}
             """
        sql """
        CREATE TABLE ${table}(
            user_id            BIGINT       NOT NULL COMMENT "user id",
            name               VARCHAR(20)           COMMENT "name",
            age                INT                   COMMENT "age"
        )
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 10
        PROPERTIES (
            "replication_num" = "1"
        );
    """
        sql """
        insert into ${table} values (1, 'a', 10);
    """

        def insertResult = sql """
        SELECT count(1) FROM ${table}
    """

        println "insertResult: ${insertResult}"

        assert insertResult.get(0).get(0) == 1
    }
    
    createDBAndTbl("azure_blob_test", "azure_blob_tbl")
    String export_table_name = "azure_blob_test.azure_blob_tbl"
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
    //outfile s3 only support s3://
    def location_prefix = "s3://${abfsContainer}"
    def full_export_path = "${location_prefix}/regression/azure/outfile/"
    def res = sql """
            SELECT * FROM ${export_table_name}
            INTO OUTFILE "${full_export_path}"
            FORMAT AS parquet
            PROPERTIES (
                ${abfs_azure_config_props}
            );
        """
    def outfile_url = res[0][3];
    println "outfile_url: ${outfile_url}"
    //tvf
    res = sql """
            SELECT * FROM S3 (
                "uri" = "${outfile_url}",
     
                "format" = "parquet",
                ${abfs_azure_config_props}
            )
        """
    assert res.size() == 1
    def blob_path = outfile_url.substring(location_prefix.length())
    def s3_tvf_abfs_uri = "abfs://${abfsContainer}@${abfsAzureAccountName}.dfs.core.windows.net/"+blob_path;
    res = sql """
            SELECT * FROM S3 (
                "uri" = "${s3_tvf_abfs_uri}",
     
                "format" = "parquet",
                ${abfs_azure_config_props}
            )
        """
    assert res.size() == 1
    def se_tvf_abfss_uri = "abfss://${abfsContainer}@${abfsAzureAccountName}.dfs.core.windows.net/"+blob_path;
    res = sql """
            SELECT * FROM S3 (
                "uri" = "${se_tvf_abfss_uri}",
     
                "format" = "parquet",
                ${abfs_azure_config_props}
            )
        """
    assert res.size() == 1
    
    // test export
    
    
    
    
    
    
}