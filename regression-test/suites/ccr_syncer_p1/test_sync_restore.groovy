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

suite("test_sync_restore") {

    def syncer = getSyncer()
    if (!syncer.checkEnableFeatureBinlog()) {
        logger.info("fe enable_feature_binlog is false, skip case test_sync_restore")
        return
    }
    String user = 'test_sync_restore_user'
    String pwd = 'C123_567p'
    def dbName = 'test_sync_restore_db'
    def tableName = "tbl_sync_restore_tb"
    def tableNametmp = "tbl_sync_restore_tmp"
    def fes = sql_return_maparray "show frontends"
    logger.info("frontends: ${fes}")
    def fe = fes[0]

    def ccr_task = {
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X POST http://${fe.Host}:${fe.HttpPort}/create_ccr -H \"Content-Type: application/json\" ")
        .append("-d \'{\"name\": \"test\",")
        .append("\"src\":{\"host\":\"${fe.Host}\",\"port\":\"${fe.QueryPort}\",\"thrift_port\":\"${fe.RpcPort}\",")
        .append("\"user\":\"root\",\"password\":\"\",\"database\":\"${dbName}\",\"table\": \"{tableName}\"},")
        .append("\"dest\": {\"host\": \"${fe.Host}\", \"port\": \"${fe.QueryPort}\", \"thrift_port\": \"${fe.RpcPort}\",")
        .append("\"user\": \"root\", \"password\": \"\",\"database\":\"${dbName}\",\"table\": \"${tableNametmp}\" }}\'")
        .append("http://${fe.Host}:9190/create_ccr");
        String command = sb.toString()
        logger.info(command)
        def process = command.execute()
    }

    def waitingBackupTaskFinished = { def curDbName ->
        Thread.sleep(2000)
        String showTasks = "SHOW BACKUP FROM ${curDbName};"
        String status = "NULL"
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 5 * 60 * 1000 // 5 min
        do {
            result = sql(showTasks)
            logger.info("result: " + result.toString())
            assertTrue(result.size() == 1)
            if (!result.isEmpty()) {
                status = result.last().get(3)
            }
            logger.info("The state of ${showTasks} is ${status}")
            Thread.sleep(1000);
        } while (timeoutTimestamp > System.currentTimeMillis() && (status != 'FINISHED'))
        if (status != "FINISHED") {
            logger.info("status is not success")
        }
        assertTrue(status == "FINISHED")
        return result[0][1]
    }

    def test_num = 1
    def insert_num = 5
    String ak = getS3AK()
    String sk = getS3SK()
    String endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");
    String repositoryName = 'test_sync_restore_rps'
    String snapshotName = "test_sync_restore_snapshot"
    try_sql("DROP USER ${user}")
    try_sql """DROP DATABASE IF EXISTS ${dbName}"""
    try_sql("""DROP REPOSITORY `${repositoryName}`;""")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""

    sql "CREATE DATABASE ${dbName}"

    sql "USE ${dbName}"

    sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE if NOT EXISTS ${tableName} 
        (
            `test` INT,
            `id` INT
        )
        ENGINE=OLAP
        UNIQUE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1 
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
        )
    """

    for (int i = 0; i < insert_num; ++i) {
        sql """
               INSERT INTO ${tableName} VALUES (${test_num}, ${i})
            """ 
    }

    def res = sql "SELECT * FROM ${tableName}"
    assertEquals(res.size(), insert_num)

    sql """
        CREATE REPOSITORY `${repositoryName}`
        WITH S3
        ON LOCATION "s3://${bucket}/${repositoryName}"
        PROPERTIES
        (
            "s3.endpoint" = "http://${endpoint}",
            "s3.region" = "${region}",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}"
        )
    """


    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repositoryName}`
        ON (${tableName})
     """
    
    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repositoryName, snapshotName)
    assertTrue(snapshot != null)

    def real_label = waitingBackupTaskFinished(dbName)
    def backup_timestamp = sql """SHOW SNAPSHOT ON ${repositoryName};"""
    logger.info("backup_timestamp: " + backup_timestamp)
    def real_timestamp
    for (int i = 0; i < backup_timestamp.size(); i++) {
        if (backup_timestamp[i][0] == real_label) {
            real_timestamp = backup_timestamp[i][1]
            break
        }
    }

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repositoryName}`
        ON (${tableName})
        PROPERTIES
        (
            "backup_timestamp"="${real_timestamp}",
            "replication_num" = "1"
        );
    """

    syncer.waitAllRestoreFinish(dbName)
    sql "sync"

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        ccr_task()
    }

    res = target_sql "SELECT * FROM ${tableName}"
    assertEquals(res.size(), insert_num)
}

