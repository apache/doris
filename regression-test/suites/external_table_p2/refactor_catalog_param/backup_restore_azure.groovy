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
import org.awaitility.Awaitility;
import static java.util.concurrent.TimeUnit.SECONDS;
import static groovy.test.GroovyAssert.shouldFail

suite("refactor_storage_backup_restore_azure", "p2,external,new_catalog_property") {
    




    def s3table = "test_backup_restore_azure";

    def databaseQueryResult = sql """
       select database();
    """
    println databaseQueryResult
    def currentDBName = databaseQueryResult.get(0).get(0)
    println currentDBName
    // cos

    def createDBAndTbl = { String dbName ->

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
        CREATE TABLE ${s3table}(
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
        insert into ${s3table} values (1, 'a', 10);
    """

        def insertResult = sql """
        SELECT count(1) FROM ${s3table}
    """

        println "insertResult: ${insertResult}"

        assert insertResult.get(0).get(0) == 1
    }

    def createRepository = { String repoName, String endpointName, String endpoint, String accessKeyName, String accessKey, String secretKeyName, String secretKey,String location ->
        try {
            sql """
                drop repository  ${repoName};
            """
        } catch (Exception e) {
            // ignore exception, repo may not exist
        }

        sql """
            CREATE REPOSITORY  ${repoName}
            WITH S3
            ON LOCATION "${location}"
            PROPERTIES (
                "${endpointName}" = "${endpoint}",
                "${accessKeyName}" = "${accessKey}",
                "${secretKeyName}" = "${secretKey}",
                "provider"="azure"
            );
        """
    }

    def backupAndRestore = { String repoName, String dbName, String tableName, String backupLabel ->
        sql """
        BACKUP SNAPSHOT ${dbName}.${backupLabel}
        TO ${repoName}
        ON (${tableName})
    """
        Awaitility.await().atMost(120, SECONDS).pollInterval(5, SECONDS).until(
                {
                    def backupResult = sql """
                show backup from ${dbName} where SnapshotName = '${backupLabel}';
            """
                    println "backupResult: ${backupResult}"
                    return backupResult.get(0).get(3) == "FINISHED"
                })

        def querySnapshotResult = sql """
        SHOW SNAPSHOT ON ${repoName} WHERE SNAPSHOT =  '${backupLabel}';
        """
        println querySnapshotResult
        def snapshotTimes = querySnapshotResult.get(0).get(1).split('\n')
        def snapshotTime = snapshotTimes[0]

        sql """
        drop table  if exists ${tableName}; 
        """

        sql """
        RESTORE SNAPSHOT ${dbName}.${backupLabel}
        FROM ${repoName}
        ON (`${tableName}`)
        PROPERTIES
        (
            "backup_timestamp"="${snapshotTime}",
            "replication_num" = "1"
        );
        """
        Awaitility.await().atMost(60, SECONDS).pollInterval(5, SECONDS).until(
                {
                    try {

                        sql """
                        use ${dbName}
                        """
                        def restoreResult = sql """
                         SELECT count(1) FROM ${tableName}
                        """
                        println "restoreResult: ${restoreResult}"
                        def count = restoreResult.get(0).get(0)
                        println "count: ${count}"
                        return restoreResult.get(0).get(0) == 1
                    } catch (Exception e) {
                        // tbl not found
                        println "tbl not found" + e.getMessage()
                        return false
                    }
                })
        sql """
        drop repository  ${repoName};
        """
    }

    String abfsAzureAccountName = context.config.otherConfigs.get("abfsAccountName")
    String abfsAzureAccountKey = context.config.otherConfigs.get("abfsAccountKey")
    String abfsContainer = context.config.otherConfigs.get("abfsContainer")
    String abfsEndpoint = context.config.otherConfigs.get("abfsEndpoint")
    def s3repoName1 = "azure_repo_1_"+System.currentTimeMillis()
    def repoPrefix = "regression/azure/backup_restore"
    
    
    def s3repoName2 = "azure_repo_2_"+System.currentTimeMillis()
    createRepository("${s3repoName2}", "azure.endpoint", abfsEndpoint,  "azure.access_key", abfsAzureAccountName, "azure.secret_key", abfsAzureAccountKey,"s3://${abfsContainer}/${repoPrefix}/test_" + System.currentTimeMillis())
    def dbName2 = currentDBName + "azure_2"
    createDBAndTbl("${dbName2}")
    backupAndRestore("${s3repoName2}", dbName2, s3table, "backup_${s3repoName2}_test")
    
    def s3repoName3 = "azure_repo_3_"+System.currentTimeMillis()
    createRepository("${s3repoName3}", "azure.endpoint", abfsEndpoint,  "azure.account_name", abfsAzureAccountName, "azure.account_key", abfsAzureAccountKey,"abfs://${abfsContainer}@${abfsAzureAccountName}.dfs.core.windows.net/${repoPrefix}/test_" + System.currentTimeMillis())
    def dbName3 = currentDBName + "azure_3"
    createDBAndTbl("${dbName3}")
    backupAndRestore("${s3repoName3}", dbName3, s3table, "backup_${s3repoName3}_test")
    
    def s3repoName4 = "azure_repo_4_"+System.currentTimeMillis()
    createRepository("${s3repoName4}", "azure.endpoint", abfsEndpoint,  "azure.account_name", abfsAzureAccountName, "azure.account_key", abfsAzureAccountKey,"https://${abfsAzureAccountName}.blob.core.windows.net/${abfsContainer}/${repoPrefix}/test_" + System.currentTimeMillis())
    def dbName4 = currentDBName + "azure_4"
    createDBAndTbl("${dbName4}")
    backupAndRestore("${s3repoName4}", dbName4, s3table, "backup_${s3repoName4}_test")


}