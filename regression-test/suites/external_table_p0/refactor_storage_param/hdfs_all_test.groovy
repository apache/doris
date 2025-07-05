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

suite("refactor_params_hdfs_all_test", "p0,external,kerberos,external_docker,external_docker_kerberos") {
    String enabled = context.config.otherConfigs.get("refactor_params_hdfs_kerberos_test")
    if (enabled == null || enabled.equalsIgnoreCase("false")) {
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def keytab_root_dir = "/keytabs"
    def keytab_dir = "${keytab_root_dir}/hive-presto-master.keytab"
    def table = "hdfs_all_test";

    def databaseQueryResult = sql """
       select database();
    """
    println databaseQueryResult
    def currentDBName = 'refactor_params_hdfs_all_test'
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

    def hdfsNonXmlParams = "\"fs.defaultFS\" = \"hdfs://${externalEnvIp}:8520\",\n" +
            "\"hadoop.kerberos.min.seconds.before.relogin\" = \"5\",\n" +
            "\"hadoop.security.authentication\" = \"kerberos\",\n" +
            "\"hadoop.kerberos.principal\"=\"hive/presto-master.docker.cluster@LABS.TERADATA.COM\",\n" +
            "\"hadoop.kerberos.keytab\" = \"${keytab_dir}\",\n" +
            "\"hive.metastore.sasl.enabled \" = \"true\",\n" +
            "\"hadoop.security.auth_to_local\" = \"RULE:[2:\\\$1@\\\$0](.*@LABS.TERADATA.COM)s/@.*//\n" +
            "                                   RULE:[2:\\\$1@\\\$0](.*@OTHERLABS.TERADATA.COM)s/@.*//\n" +
            "                                   RULE:[2:\\\$1@\\\$0](.*@OTHERREALM.COM)s/@.*//\n" +
            "                                   DEFAULT\""

    def createRepository = { String repoName, String location, String hdfsParams ->
        try {
            sql """
                drop repository  ${repoName};
            """
        } catch (Exception e) {
            // ignore exception, repo may not exist
        }

        sql """
            CREATE REPOSITORY  ${repoName}
            WITH HDFS
            ON LOCATION "${location}"
            PROPERTIES (
                ${hdfsParams}
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
                        println "tbl not found" + e.getMessage()
                        return false
                    }
                })
    }
    def hdfs_tvf = { filePath, hdfsParam ->

        def hdfs_tvf_sql = sql """
        select * from hdfs

        (
          'uri'='${filePath}',
          "format" = "csv",
           ${hdfsParam}
        );
       """
    }
    def export_hdfs = { defaultFs, hdfsParams ->
        def exportPath = defaultFs + "/test/_export/" + System.currentTimeMillis()
        def exportLabel = "export_" + System.currentTimeMillis();
        sql """
                EXPORT TABLE ${table}
                TO "${exportPath}"
                PROPERTIES
                (
                "label"="${exportLabel}",
                "line_delimiter" = ","
                )
                with HDFS
                (
              
                 ${hdfsParams}
                );
              """

        databaseQueryResult = sql """
            select database();
         """
        currentDBName = databaseQueryResult.get(0).get(0)
        Awaitility.await().atMost(60, SECONDS).pollInterval(5, SECONDS).until({
            def exportResult = sql """
                 SHOW EXPORT FROM ${currentDBName} WHERE LABEL = "${exportLabel}";
                
                """

            println exportResult

            if (null == exportResult || exportResult.isEmpty() || null == exportResult.get(0) || exportResult.get(0).size() < 3) {
                return false;
            }
            if (exportResult.get(0).get(2) == 'CANCELLED' || exportResult.get(0).get(2) == 'FAILED') {
                throw new RuntimeException("load failed")
            }

            return exportResult.get(0).get(2) == 'FINISHED'
        })

    }
    def outfile_to_hdfs = { defaultFs, hdfsParams ->
        def outFilePath = "${defaultFs}/outfile_different_hdfs/exp_"
        // select ... into outfile ...
        def res = sql """
            SELECT * FROM ${table}  ORDER BY user_id
            INTO OUTFILE "${outFilePath}"
            FORMAT AS CSV
            PROPERTIES (
                ${hdfsParams}
            );
        """
        return res[0][3]
    }
    def hdfsLoad = { filePath, hdfsParams ->
        databaseQueryResult = sql """
       select database();
    """
        println databaseQueryResult
        def dataCountResult = sql """
            SELECT count(*) FROM ${table}
        """
        def dataCount = dataCountResult[0][0]
        def label = "hdfs_load_label_" + System.currentTimeMillis()
        def load = sql """
            LOAD LABEL `${label}` (
           data infile ("${filePath}")
           into table ${table}
            COLUMNS TERMINATED BY "\\\t"
            FORMAT AS "CSV"
             (
                user_id,
                name,
                age
             ))
             with hdfs
             (
              ${hdfsParams}
             )
             PROPERTIES
            (
                "timeout" = "3600"
            );
        """
        Awaitility.await().atMost(60, SECONDS).pollInterval(5, SECONDS).until({
            def loadResult = sql """
           show load where label = '${label}';
           """
            println 'test'
            println loadResult

            if (null == loadResult || loadResult.isEmpty() || null == loadResult.get(0) || loadResult.get(0).size() < 3) {
                return false;
            }
            if (loadResult.get(0).get(2) == 'CANCELLED' || loadResult.get(0).get(2) == 'FAILED') {
                throw new RuntimeException("load failed")
            }

            return loadResult.get(0).get(2) == 'FINISHED'
        })


        def expectedCount = dataCount + 1
        Awaitility.await().atMost(60, SECONDS).pollInterval(5, SECONDS).until({
            def loadResult = sql """
            select count(*) from ${table}
        """
            println "loadResult: ${loadResult} "
            return loadResult.get(0).get(0) == expectedCount
        })

    }
    def repoName = 'hdfs_repo';
    // create repo
    createRepository(repoName,"hdfs://${externalEnvIp}:8520/test_repo",hdfsNonXmlParams);
    def dbName1 = currentDBName + "${repoName}_1"
    createDBAndTbl(dbName1)
    def backupLabel=repoName+System.currentTimeMillis()
    //backup and restore
    backupAndRestore(repoName,dbName1,table,backupLabel)
    def failedRepoName='failedRepo'
    shouldFail {
        createRepository(failedRepoName,"s3://172.20.32.136:8520",hdfsNonXmlParams);
    }
    shouldFail {
        createRepository(failedRepoName," ",hdfsNonXmlParams);
    }

    //outfile 
    dbName1 = currentDBName + 'outfile_test_1'
    createDBAndTbl(dbName1)
    def outfile = outfile_to_hdfs("hdfs://${externalEnvIp}:8520", hdfsNonXmlParams);
    println outfile
    //hdfs tvf
    def hdfsTvfResult = hdfs_tvf(outfile, hdfsNonXmlParams)
    println hdfsTvfResult

    //hdfsLoad(outfile,hdfsNonXmlParams)

    //export 
    export_hdfs("hdfs://${externalEnvIp}:8520", hdfsNonXmlParams)


}