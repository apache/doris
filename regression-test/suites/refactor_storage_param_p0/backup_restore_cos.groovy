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
suite("refactor_storage_backup_restore_cos") {
    String enabled= context.config.otherConfigs.get("enableBackUpRestoreCOSTest");
    if (enabled == null && enabled.equalsIgnoreCase("false")){
        return 
    }
    def s3table = "test_backup_restore_cos";
    sql """
        drop table  if exists ${s3table}; 
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
    def databaseQueryResult = sql """
       select database();
    """
    println databaseQueryResult
    def currentDBName = databaseQueryResult.get(0).get(0)
    println currentDBName
    // cos
   
    String objectAccessKey = context.config.otherConfigs.get("cosAK");
    String objectSecretKey = context.config.otherConfigs.get("cosSK");
    String objectStorageEndpoint =context.config.otherConfigs.get("cosEndpoint");
    String objectStorageRegion = context.config.otherConfigs.get("cosRegion");
    String objectStorageFilePathPrefix =context.config.otherConfigs.get("cosFilePathPrefix");

    def objectStorageRepoName = "cos_repo_test_1";
    try {
        sql """
            drop repository  ${objectStorageRepoName};
        """
    } catch (Exception e) {
        //ignore exception, repo may not exist
    }

   
    shouldFail {
        sql """
            CREATE REPOSITORY  ${objectStorageRepoName}
        WITH S3
        ON LOCATION "${objectStorageFilePathPrefix}"
        PROPERTIES (
            "s3.endpoint" = "${objectStorageEndpoint}",
            "s3.region" = "${objectStorageRegion}",
            "s3.secret_key" = "${objectSecretKey}"
        );
        """
    }
    shouldFail {
        sql """
            CREATE REPOSITORY  ${objectStorageRepoName}
        WITH S3
        ON LOCATION "${objectStorageFilePathPrefix}"
        PROPERTIES (
            "s3.endpoint" = "${objectStorageEndpoint}",
            "s3.region" = "${objectStorageRegion}",
            "s3.secret_key" = "${objectSecretKey}"
        );
        """
    }
    sql """
        CREATE REPOSITORY  ${objectStorageRepoName}
        WITH S3
        ON LOCATION "${objectStorageFilePathPrefix}"
        PROPERTIES (
            "s3.endpoint" = "${objectStorageEndpoint}",
            "s3.region" = "${objectStorageRegion}",
            "s3.access_key" = "${objectAccessKey}",
            "s3.secret_key" = "${objectSecretKey}"
        );
    """
    
    def objectStorageBackupLabel = "oss_label_1" + System.currentTimeMillis()
    sql """
       BACKUP SNAPSHOT ${currentDBName}.${objectStorageBackupLabel}
        TO ${objectStorageRepoName}
        ON (${s3table})
        PROPERTIES ("type" = "full");
        
    """
    Awaitility.await().atMost(60, SECONDS).pollInterval(5, SECONDS).until(
            {
                def backupResult = sql """
                    show backup from ${currentDBName} where SnapshotName = '${objectStorageBackupLabel}';
                """
                println "backupResult: ${backupResult}"
                return backupResult.get(0).get(3) == "FINISHED"
            })

    def queryCosSnapshotResult = sql """
        SHOW SNAPSHOT ON ${objectStorageRepoName} WHERE SNAPSHOT =  '${objectStorageBackupLabel}';
    """
    println queryCosSnapshotResult
    def objectSecretSnapshotTime = queryCosSnapshotResult.get(0).get(1)
    sql """
        drop table  if exists ${s3table}; 
    """
    //restore
    sql """
        RESTORE SNAPSHOT ${currentDBName}.${objectStorageBackupLabel}
        FROM ${objectStorageRepoName}
        ON (`${s3table}`)
        PROPERTIES
        (
            "backup_timestamp"="${objectSecretSnapshotTime}",
            "replication_num" = "1"
        );
    """
    Awaitility.await().atMost(60, SECONDS).pollInterval(5, SECONDS).until(
            {
                try {
                    def restoreResult = sql """
                        SELECT count(1) FROM ${s3table}
                    """
                    println "restoreResult: ${restoreResult}"
                    return restoreResult.get(0).get(0) == 1
                } catch (Exception e) {
                    //tbl not found
                    return false
                }
            })


}