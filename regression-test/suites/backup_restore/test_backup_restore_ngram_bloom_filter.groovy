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

suite("test_backup_restore_ngram_bloom_filter", "backup_restore") {
    String suiteName = "test_backup_restore_ngram_bloom_filter"
    String repoName = "${suiteName}_repo"
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"


    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        CREATE TABLE if NOT EXISTS ${dbName}.${tableName}
        (
            `test` INT,
            `id` INT,
            `username` varchar(32) NULL DEFAULT "",
            `only4test` varchar(32) NULL DEFAULT "",
            INDEX idx_ngrambf (`username`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256")
        )
        ENGINE=OLAP
        DUPLICATE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "bloom_filter_columns" = "id"
        )
    """
    for (int index = 0; index < 10; index++) {
        sql """
            INSERT INTO ${dbName}.${tableName} VALUES (${index}, ${index}, "test_${index}", "${index}_test")
            """
    }

    def checkNgramBf = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if (row[2] == "idx_ngrambf" && row[10] == "NGRAM_BF") {
                return true
            }
        }
        return false
    }
    def checkBloomFilter = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if ((row[1] as String).contains("\"bloom_filter_columns\" = \"id\"")) {
                return true
            }
        }
        return false
    }
    List<List<Object>> res = sql "SHOW INDEXES FROM ${dbName}.${tableName}"
    assertTrue(checkNgramBf(res));
    res = sql "SHOW CREATE TABLE ${dbName}.${tableName}"
    assertTrue(checkBloomFilter(res));

    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
        PROPERTIES ("type" = "full")
        """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)

    assertTrue(snapshot != null)

    sql "TRUNCATE TABLE ${dbName}.${tableName}"
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
        """

    syncer.waitAllRestoreFinish(dbName)

    res = sql "SHOW INDEXES FROM ${dbName}.${tableName}"
    assertTrue(checkNgramBf(res));
    res = sql "SHOW CREATE TABLE ${dbName}.${tableName}"
    assertTrue(checkBloomFilter(res));

    sql """
        ALTER TABLE ${dbName}.${tableName}
        ADD INDEX idx_only4test(`only4test`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256")
        """
    def checkNgramBf1 = { inputRes -> Boolean
        for (List<Object> row : inputRes) {
            if (row[2] == "idx_only4test" && row[10] == "NGRAM_BF") {
                return true
            }
        }
        return false
    }

    int count = 0;
    while (true) {
        res = sql "SHOW INDEXES FROM ${dbName}.${tableName}"
        if (checkNgramBf1(res)) {
            break
        }
        count += 1;
        if (count >= 30) {
            throw new IllegalStateException("alter table add index timeouted")
        }
        Thread.sleep(1000);
    }

    snapshotName = "${snapshotName}_1"
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
        PROPERTIES ("type" = "full")
        """

    syncer.waitSnapshotFinish(dbName)

    snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)

    assertTrue(snapshot != null)

    sql "TRUNCATE TABLE ${dbName}.${tableName}"
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
        """

    syncer.waitAllRestoreFinish(dbName)

    res = sql "SHOW INDEXES FROM ${dbName}.${tableName}"
    assertTrue(checkNgramBf1(res));

    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"

}
