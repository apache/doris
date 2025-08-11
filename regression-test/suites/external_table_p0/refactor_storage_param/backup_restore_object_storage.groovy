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

suite("refactor_storage_backup_restore_object_storage", "p0,external,external_docker") {
    String enabled = context.config.otherConfigs.get("enableRefactorParamsTest")
    if (enabled == null || enabled.equalsIgnoreCase("false")) {
        return
    }
    def s3table = "test_backup_restore";

    def databaseQueryResult = sql """
       select database();
    """
    println databaseQueryResult
    def currentDBName = 'refactor_repo'
    println currentDBName
    // cos

    def createDBAndTbl = { String dbName ->

        sql """
                drop database if exists ${dbName}
            """

        sql """
            create database ${dbName}
        """

        sql  """
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

    def createRepository = { String repoName, String endpointName, String endpoint, String regionName, String region, String accessKeyName, String accessKey, String secretKeyName, String secretKey, String usePathStyle, String location ->
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
                "${regionName}" = "${region}",
                "${accessKeyName}" = "${accessKey}",
                "${secretKeyName}" = "${secretKey}",
                "use_path_style" = "${usePathStyle}"
            );
        """
    }

    def backupAndRestore = { String repoName, String dbName, String tableName, String backupLabel ->
        sql """
        BACKUP SNAPSHOT ${dbName}.${backupLabel}
        TO ${repoName}
        ON (${tableName})
    """
        Awaitility.await().atMost(60, SECONDS).pollInterval(5, SECONDS).until(
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
                        println "tbl not found"+e.getMessage()
                        return false
                    }
                })
    }



    def test_backup_restore= {String ak,String sk,String s3_endpoint,String region,String bucket,String objPrefix ->
        def s3repoName1 = "${objPrefix}_repo_1"
        createRepository("${s3repoName1}", "s3.endpoint", s3_endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "true", "s3://${bucket}/test_" + System.currentTimeMillis())

        def dbName1 = currentDBName + "${objPrefix}_1"
        createDBAndTbl("${dbName1}")
        backupAndRestore("${s3repoName1}", dbName1, s3table, "backup_${s3repoName1}_test")
        def s3repoName2 = "${objPrefix}_repo_2"
        createRepository("${s3repoName2}", "s3.endpoint", s3_endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "false", "s3://${bucket}/test_" + System.currentTimeMillis())
        def dbName2 = currentDBName + "${objPrefix}_2"
        createDBAndTbl("${dbName2}")
        backupAndRestore("${s3repoName2}", dbName2, s3table, "backup_${s3repoName2}_test")

        def s3repoName3 = "${objPrefix}_repo_3"
        createRepository("${s3repoName3}", "s3.endpoint", s3_endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "", "s3://${bucket}/test_" + System.currentTimeMillis())
        def dbName3 = currentDBName + "${objPrefix}_3"
        createDBAndTbl("${dbName3}")
        backupAndRestore("${s3repoName3}", dbName3, s3table, "backup_${s3repoName3}_test")

        def s3repoName4 = "${objPrefix}_s3_repo_4"
        createRepository("${s3repoName4}", "s3.endpoint", s3_endpoint, "s3.region", region, "AWS_ACCESS_KEY", ak, "AWS_SECRET_KEY", sk, "true", "s3://${bucket}/test_" + System.currentTimeMillis())
        def dbName4 = currentDBName + "${objPrefix}_4"
        createDBAndTbl("${dbName4}")
        backupAndRestore("${s3repoName4}", dbName4, s3table, "backup_${s3repoName4}_test")
        def s3repoName5 = "${objPrefix}_s3_repo_5"
        createRepository("${s3repoName5}", "s3.endpoint", s3_endpoint, "s3.region", region, "AWS_ACCESS_KEY", ak, "AWS_SECRET_KEY", sk, "false", "s3://${bucket}/test_" + System.currentTimeMillis())
        def dbName5 = currentDBName + "${objPrefix}_5"
        createDBAndTbl("${dbName5}")
        backupAndRestore("${s3repoName5}", dbName5, s3table, "backup_${s3repoName5}_test")
        def s3repoName6 = "${objPrefix}_s3_repo_6"
        createRepository("${s3repoName6}", "AWS_ENDPOINT", s3_endpoint, "AWS_REGION", region, "AWS_ACCESS_KEY", ak, "AWS_SECRET_KEY", sk, "false", "s3://${bucket}/test_" + System.currentTimeMillis())
        def dbName6 = currentDBName + "${objPrefix}_6"
        createDBAndTbl("${dbName6}")
        backupAndRestore("${s3repoName6}", dbName6, s3table, "backup_${s3repoName6}_test")
        def s3repoName7 = "${objPrefix}_s3_repo_7"
        createRepository("${s3repoName7}", "s3.endpoint", s3_endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "", "https://${bucket}/test_" + System.currentTimeMillis())
        def dbName7 = currentDBName + "${objPrefix}_7"
      
        createDBAndTbl("${dbName7}")
        backupAndRestore("${s3repoName7}", dbName7, s3table, "backup_${s3repoName7}_test")
        def failedRepoName = "s3_repo_failed"
        // wrong address
        shouldFail {
            createRepository("${failedRepoName}", "s3.endpoint", s3_endpoint, "s3.region", region, "AWS_ACCESS_KEY", ak, "AWS_SECRET_KEY", sk, "true", "s3://ck/" + System.currentTimeMillis())
        }
        //region is empty
        shouldFail {
            createRepository("${failedRepoName}", "s3.endpoint", "", "s3.region", "", "s3.access_key", ak, "s3.secret_key", sk, "", "s3://${bucket}/test_" + System.currentTimeMillis())
        }
    }
    /*-------------AWS S3--------------------------------*/
    String ak = context.config.otherConfigs.get("AWSAK")
    String sk = context.config.otherConfigs.get("AWSSK")
    String s3_endpoint = "s3.ap-northeast-1.amazonaws.com"
    String region = "ap-northeast-1"
    String bucket = "selectdb-qa-datalake-test"
    String objPrefix="s3"
    test_backup_restore(ak,sk,s3_endpoint,region,bucket,objPrefix)
    //todo When the new fs is fully enabled, we need to open this startup
    String enabledOtherObjectStorageTest = context.config.otherConfigs.get("enabledOtherObjectStorageTest")
    if (enabledOtherObjectStorageTest == null || enabledOtherObjectStorageTest.equalsIgnoreCase("false")) {
        return
    }
    /*-----------------Tencent COS----------------*/
    ak = context.config.otherConfigs.get("txYunAk")
    sk = context.config.otherConfigs.get("txYunSk")
    s3_endpoint = "cos.ap-beijing.myqcloud.com"
    region = "ap-beijing"
    bucket = "doris-build-1308700295";
    
    objPrefix="cos"
    test_backup_restore(ak,sk,s3_endpoint,region,bucket,objPrefix)
    /*  cos_url  */
    def cos_repoName1 = "${objPrefix}_repo_cos_prefix_1"
    // url is : cos://bucket/prefix/
    createRepository("${cos_repoName1}", "cos.endpoint", s3_endpoint, "cos.region", region, "cos.access_key", ak, "cos.secret_key", sk, "true", "cos://${bucket}/test_" + System.currentTimeMillis())

    def cosDbName1 = currentDBName + "${objPrefix}_cos_1"
    createDBAndTbl("${cosDbName1}")
    backupAndRestore("${cos_repoName1}", cosDbName1, s3table, "backup_${cos_repoName1}_test")
    def cos_repoName2 = "${objPrefix}_repo_cos_prefix_2"
    // url is : cos://bucket/prefix/
    createRepository("${cos_repoName2}", "cos.endpoint", s3_endpoint, "cos.region", region, "cos.access_key", ak, "cos.secret_key", sk, "false", "https://${bucket}.${s3_endpoint}/test_" + System.currentTimeMillis())

    def cosDbName2 = currentDBName + "${objPrefix}_cos_2"
    createDBAndTbl("${cosDbName2}")
    backupAndRestore("${cos_repoName2}", cosDbName2, s3table, "backup_${cos_repoName1}_test")
    


    /*-----------------Huawei OBS----------------*/
    ak = context.config.otherConfigs.get("hwYunAk")
    sk = context.config.otherConfigs.get("hwYunSk")
    s3_endpoint = "obs.cn-north-4.myhuaweicloud.com"
    region = "cn-north-4"
    bucket = "doris-build";
    objPrefix="obs"
    test_backup_restore(ak,sk,s3_endpoint,region,bucket,objPrefix)
    def obs_repoName1 = "${objPrefix}_repo_obs_prefix_1"
    // url is : cos://bucket/prefix/
    createRepository("${obs_repoName1}", "obs.endpoint", s3_endpoint, "obs.region", region, "obs.access_key", ak, "obs.secret_key", sk, "true", "obs://${bucket}/test_" + System.currentTimeMillis())

    def obsDbName1 = currentDBName + "${objPrefix}_obs_1"
    createDBAndTbl("${obsDbName1}")
    backupAndRestore("${obs_repoName1}", obsDbName1, s3table, "backup_${obs_repoName1}_test")
    def obs_repoName2 = "${objPrefix}_repo_obs_prefix_2"
    // url is : cos://bucket/prefix/
    createRepository("${obs_repoName2}", "obs.endpoint", s3_endpoint, "obs.region", region, "obs.access_key", ak, "obs.secret_key", sk, "false", "https://${bucket}.${s3_endpoint}/test_" + System.currentTimeMillis())

    def obsDbName2 = currentDBName + "${objPrefix}_obs_2"
    createDBAndTbl("${obsDbName2}")
    backupAndRestore("${obs_repoName2}", obsDbName2, s3table, "backup_${obs_repoName1}_test")


    /*-----------------Aliyun OSS----------------*/
    ak = context.config.otherConfigs.get("aliYunAk")
    sk = context.config.otherConfigs.get("aliYunSk")
    s3_endpoint = "oss-cn-hongkong.aliyuncs.com"
    region = "oss-cn-hongkong"
    bucket = "doris-regression-hk";
    objPrefix="oss"
    // oss has some problem, so we comment it.
    //test_backup_restore(ak,sk,s3_endpoint,region,bucket,objPrefix)
    def oss_repoName1 = "${objPrefix}_repo_oss_prefix_1"
    // url is : cos://bucket/prefix/
    createRepository("${oss_repoName1}", "oss.endpoint", s3_endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "false", "oss://${bucket}/test_" + System.currentTimeMillis())

    def ossDbName1 = currentDBName + "${objPrefix}_oss_1"
    createDBAndTbl("${ossDbName1}")
    backupAndRestore("${oss_repoName1}", ossDbName1, s3table, "backup_${oss_repoName1}_test")
    def oss_repoName2 = "${objPrefix}_repo_oss_prefix_2"
    // url is : cos://bucket/prefix/
    createRepository("${oss_repoName2}", "oss.endpoint", s3_endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "false", "https://${bucket}.${s3_endpoint}/test_" + System.currentTimeMillis())

    def ossDbName2 = currentDBName + "${objPrefix}_oss_2"
    createDBAndTbl("${ossDbName2}")
    backupAndRestore("${oss_repoName2}", ossDbName2, s3table, "backup_${oss_repoName1}_test")


}
