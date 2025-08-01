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

suite("test_backup_restore_colocate", "backup_restore") {
    String suiteName = "test_backup_restore_colocate"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String newDbName = "${suiteName}_db_new"
    String tableName1 = "${suiteName}_table1"
    String tableName2 = "${suiteName}_table2"
    String tableName3 = "${suiteName}_table3"
    String snapshotName = "${suiteName}_snapshot"
    String groupName = "${suiteName}_group"

    def showTabletHealth = { name ->
        def ret = sql_return_maparray """SHOW PROC '/cluster_health/tablet_health' """
        ret.find {
            def matcher = it.DbName =~ /.*${name}$/
            matcher.matches()
        }
    }

    def checkColocateTabletHealth = { db_name ->
        def result = showTabletHealth.call(db_name)
        log.info(result as String)
        assertNotNull(result)
        assertTrue(result.ColocateMismatchNum as int == 0)
    }

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "DROP DATABASE IF EXISTS ${newDbName}"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "CREATE DATABASE IF NOT EXISTS ${newDbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName1}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName2}"
    sql """
           CREATE TABLE if NOT EXISTS ${dbName}.${tableName1}
           (
               `test` INT,
               `id` INT
           )
           ENGINE=OLAP
           UNIQUE KEY(`test`, `id`)
           DISTRIBUTED BY HASH(id) BUCKETS 2
           PROPERTIES (
               "replication_allocation" = "tag.location.default: 1",
               "colocate_with" = "${groupName}"
        )
    """
    sql """
           CREATE TABLE if NOT EXISTS ${dbName}.${tableName2}
           (
               `test` INT,
               `id` INT
           )
           ENGINE=OLAP
           UNIQUE KEY(`test`, `id`)
           DISTRIBUTED BY HASH(id) BUCKETS 2
           PROPERTIES (
               "replication_allocation" = "tag.location.default: 1",
               "colocate_with" = "${groupName}"
        )
        """
    def insert_num = 5
    for (int i = 0; i < insert_num; ++i) {
        sql """
               INSERT INTO ${dbName}.${tableName1} VALUES (${i}, ${i})
            """
        sql """
               INSERT INTO ${dbName}.${tableName2} VALUES (${i}, ${i})
            """
    }

    def query = "select * from ${dbName}.${tableName1} as t1, ${dbName}.${tableName2} as t2 where t1.id=t2.id;"

    def res = sql "SELECT * FROM ${dbName}.${tableName1}"
    assertEquals(res.size(), insert_num)
    res = sql "SELECT * FROM ${dbName}.${tableName2}"
    assertEquals(res.size(), insert_num)

    explain {
        sql("${query}")
        contains("COLOCATE")
    }

    res = sql "${query}"
    assertEquals(res.size(), insert_num)
    checkColocateTabletHealth(dbName)

    sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName}
            TO `${repoName}`
            ON (${tableName1}, ${tableName2})
            PROPERTIES ("type" = "full")
        """

    syncer.waitSnapshotFinish(dbName)
    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)


    logger.info("============== test 1: without reserve_colocate =============")

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName1}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName2}"

    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """
    syncer.waitAllRestoreFinish(dbName)

    res = sql "SELECT * FROM ${dbName}.${tableName1}"
    assertEquals(res.size(), insert_num)
    res = sql "SELECT * FROM ${dbName}.${tableName2}"
    assertEquals(res.size(), insert_num)


    explain {
        sql("${query}")
        notContains("COLOCATE")
    }
    res = sql "${query}"
    assertEquals(res.size(), insert_num)
    checkColocateTabletHealth(dbName)

    logger.info("============== test 2: reserve_colocate = false =============")

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName1}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName2}"

    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_colocate" = "false"
        )
    """
    syncer.waitAllRestoreFinish(dbName)

    res = sql "SELECT * FROM ${dbName}.${tableName1}"
    assertEquals(res.size(), insert_num)
    res = sql "SELECT * FROM ${dbName}.${tableName2}"
    assertEquals(res.size(), insert_num)


    explain {
        sql("${query}")
        notContains("COLOCATE")
    }
    res = sql "${query}"
    assertEquals(res.size(), insert_num)
    checkColocateTabletHealth(dbName)


    logger.info("============== test 3: reserve_colocate = true =============")

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName1}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName2}"

    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_colocate" = "true"
        )
    """
    syncer.waitAllRestoreFinish(dbName)

    res = sql "SELECT * FROM ${dbName}.${tableName1}"
    assertEquals(res.size(), insert_num)
    res = sql "SELECT * FROM ${dbName}.${tableName2}"
    assertEquals(res.size(), insert_num)


    explain {
        sql("${query}")
        contains("COLOCATE")
    }
    res = sql "${query}"
    assertEquals(res.size(), insert_num)
    checkColocateTabletHealth(dbName)

    logger.info("============== test 4: Not support to restore to local table with colocate group =============")

    res = sql "SELECT * FROM ${dbName}.${tableName1}"
    assertEquals(res.size(), insert_num)
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_colocate" = "true"
        )
    """
    syncer.waitAllRestoreFinish(dbName)
    // Not support to restore to local table with colocate group
    def records = sql_return_maparray "SHOW restore FROM ${dbName}"
    def row = records[records.size() - 1]
    assertTrue(row.Status.contains("with colocate group"))


    logger.info("============== test 5: local table with colocate group =============")

    res = sql "SELECT * FROM ${dbName}.${tableName1}"
    assertEquals(res.size(), insert_num)
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_colocate" = "false"
        )
    """
    syncer.waitAllRestoreFinish(dbName)
    records = sql_return_maparray "SHOW restore FROM ${dbName}"
    row = records[records.size() - 1]
    assertTrue(row.Status.contains("with colocate group"))


    logger.info("============== test 6: local table without colocate group =============")
    
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName1}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName2}"
    // without colocate group
    sql """
           CREATE TABLE if NOT EXISTS ${dbName}.${tableName1}
           (
               `test` INT,
               `id` INT
           )
           ENGINE=OLAP
           UNIQUE KEY(`test`, `id`)
           DISTRIBUTED BY HASH(id) BUCKETS 2
           PROPERTIES (
               "replication_allocation" = "tag.location.default: 1"
        )
    """
    sql """
           CREATE TABLE if NOT EXISTS ${dbName}.${tableName2}
           (
               `test` INT,
               `id` INT
           )
           ENGINE=OLAP
           UNIQUE KEY(`test`, `id`)
           DISTRIBUTED BY HASH(id) BUCKETS 2
           PROPERTIES (
               "replication_allocation" = "tag.location.default: 1"
        )
    """

    assertEquals(res.size(), insert_num)
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_colocate" = "false"
        )
    """
    syncer.waitAllRestoreFinish(dbName)
    records = sql_return_maparray "SHOW restore FROM ${dbName}"
    row = records[records.size() - 1]
    assertTrue(row.Status.contains("OK"))
    checkColocateTabletHealth(dbName)


    logger.info("============== test 7: local colocate mismatch error =============")

    sql "DROP TABLE IF EXISTS ${newDbName}.${tableName1}"
    sql "DROP TABLE IF EXISTS ${newDbName}.${tableName2}"
    sql "DROP TABLE IF EXISTS ${newDbName}.${tableName3}"
    // create with different colocat
    sql """
           CREATE TABLE if NOT EXISTS ${newDbName}.${tableName3}
           (
               `test` INT,
               `id` INT
           )
           ENGINE=OLAP
           UNIQUE KEY(`test`, `id`)
           DISTRIBUTED BY HASH(id) BUCKETS 3
           PROPERTIES (
               "replication_allocation" = "tag.location.default: 1",
               "colocate_with" = "${groupName}"
        )
    """

    sql """
        RESTORE SNAPSHOT ${newDbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_colocate" = "true"
        )
    """
    syncer.waitAllRestoreFinish(newDbName)

    records = sql_return_maparray "SHOW restore FROM ${newDbName}"
    row = records[records.size() - 1]
    assertTrue(row.Status.contains("Colocate tables must have same bucket num"))
  
    //cleanup
    sql "DROP TABLE IF EXISTS ${newDbName}.${tableName1}"
    sql "DROP TABLE IF EXISTS ${newDbName}.${tableName2}"
    sql "DROP TABLE IF EXISTS ${newDbName}.${tableName3}"
    sql "DROP DATABASE ${newDbName} FORCE"

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName1}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName2}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName3}"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

suite("test_backup_restore_colocate_with_partition", "backup_restore") {
    String suiteName = "test_backup_restore_colocate_with_partition"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String newDbName = "${suiteName}_db_new"
    String tableName1 = "${suiteName}_table1"
    String tableName2 = "${suiteName}_table2"
    String tableName3 = "${suiteName}_table3"
    String snapshotName = "${suiteName}_snapshot"
    String groupName = "${suiteName}_group"

    def showTabletHealth = { name ->
        def ret = sql_return_maparray """SHOW PROC '/cluster_health/tablet_health' """
        ret.find {
            def matcher = it.DbName =~ /.*${name}$/
            matcher.matches()
        }
    }

    def checkColocateTabletHealth = { db_name ->
        def result = showTabletHealth.call(db_name)
        log.info(result as String)
        assertNotNull(result)
        assertTrue(result.ColocateMismatchNum as int == 0)
    }

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "DROP DATABASE IF EXISTS ${newDbName}"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "CREATE DATABASE IF NOT EXISTS ${newDbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName1}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName2}"
    sql """
           CREATE TABLE if NOT EXISTS ${dbName}.${tableName1}
           (
               `test` INT,
               `id` INT,
                a INT
           )
           ENGINE=OLAP
           UNIQUE KEY(`test`, `id`)
           partition by range(test)
           (
              PARTITION `p1` VALUES LESS THAN (4),
              PARTITION `P2` VALUES LESS THAN (100)
           )
           DISTRIBUTED BY HASH(id) BUCKETS 2
           PROPERTIES (
               "replication_allocation" = "tag.location.default: 1",
               "colocate_with" = "${groupName}"
        )
    """
    createMV( """
       create materialized view mv_t1 as select test as a1, id as a2 from ${dbName}.${tableName1};
    """)
    sql """
           CREATE TABLE if NOT EXISTS ${dbName}.${tableName2}
           (
               `test` INT,
               `id` INT
           )
           ENGINE=OLAP
           UNIQUE KEY(`test`, `id`)
           partition by range(test)
           (
              PARTITION `p1` VALUES LESS THAN (4),
              PARTITION `P2` VALUES LESS THAN (100)
           )
           DISTRIBUTED BY HASH(id) BUCKETS 2
           PROPERTIES (
               "replication_allocation" = "tag.location.default: 1",
               "colocate_with" = "${groupName}"
        )
        """
    def insert_num = 5
    for (int i = 0; i < insert_num; ++i) {
        sql """
               INSERT INTO ${dbName}.${tableName1} VALUES (${i}, ${i}, ${i})
            """
        sql """
               INSERT INTO ${dbName}.${tableName2} VALUES (${i}, ${i})
            """
    }

    checkColocateTabletHealth(dbName)

    def query = "select * from ${dbName}.${tableName1} as t1, ${dbName}.${tableName2} as t2 where t1.id=t2.id;"

    def res = sql "SELECT * FROM ${dbName}.${tableName1}"
    assertEquals(res.size(), insert_num)
    res = sql "SELECT * FROM ${dbName}.${tableName2}"
    assertEquals(res.size(), insert_num)

    explain {
        sql("${query}")
        contains("COLOCATE")
    }

    res = sql "${query}"
    assertEquals(res.size(), insert_num)

    sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName}
            TO `${repoName}`
            ON (${tableName1}, ${tableName2})
            PROPERTIES ("type" = "full")
        """

    syncer.waitSnapshotFinish(dbName)
    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)


    logger.info("============== test 1: without reserve_colocate =============")

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName1}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName2}"

    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """
    syncer.waitAllRestoreFinish(dbName)

    res = sql "SELECT * FROM ${dbName}.${tableName1}"
    assertEquals(res.size(), insert_num)
    res = sql "SELECT * FROM ${dbName}.${tableName2}"
    assertEquals(res.size(), insert_num)


    explain {
        sql("${query}")
        notContains("COLOCATE")
    }
    res = sql "${query}"
    assertEquals(res.size(), insert_num)
    checkColocateTabletHealth(dbName)

    logger.info("============== test 2: reserve_colocate = false =============")

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName1}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName2}"

    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_colocate" = "false"
        )
    """
    syncer.waitAllRestoreFinish(dbName)

    res = sql "SELECT * FROM ${dbName}.${tableName1}"
    assertEquals(res.size(), insert_num)
    res = sql "SELECT * FROM ${dbName}.${tableName2}"
    assertEquals(res.size(), insert_num)


    explain {
        sql("${query}")
        notContains("COLOCATE")
    }
    res = sql "${query}"
    assertEquals(res.size(), insert_num)
    checkColocateTabletHealth(dbName)

    logger.info("============== test 3: reserve_colocate = true =============")

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName1}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName2}"

    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_colocate" = "true"
        )
    """
    syncer.waitAllRestoreFinish(dbName)

    res = sql "SELECT * FROM ${dbName}.${tableName1}"
    assertEquals(res.size(), insert_num)
    res = sql "SELECT * FROM ${dbName}.${tableName2}"
    assertEquals(res.size(), insert_num)


    explain {
        sql("${query}")
        contains("COLOCATE")
    }
    res = sql "${query}"
    assertEquals(res.size(), insert_num)
    checkColocateTabletHealth(dbName)

    logger.info("============== test 4: Not support to restore to local table with colocate group =============")

    res = sql "SELECT * FROM ${dbName}.${tableName1}"
    assertEquals(res.size(), insert_num)
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_colocate" = "true"
        )
    """
    syncer.waitAllRestoreFinish(dbName)
    // Not support to restore to local table with colocate group
    def records = sql_return_maparray "SHOW restore FROM ${dbName}"
    def row = records[records.size() - 1]
    assertTrue(row.Status.contains("with colocate group"))


    logger.info("============== test 5: local table with colocate group =============")

    res = sql "SELECT * FROM ${dbName}.${tableName1}"
    assertEquals(res.size(), insert_num)
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_colocate" = "false"
        )
    """
    syncer.waitAllRestoreFinish(dbName)
    records = sql_return_maparray "SHOW restore FROM ${dbName}"
    row = records[records.size() - 1]
    assertTrue(row.Status.contains("with colocate group"))

    logger.info("============== test 6: restore to a new db =============")

    sql "DROP TABLE IF EXISTS ${newDbName}.${tableName1}"
    sql "DROP TABLE IF EXISTS ${newDbName}.${tableName2}"
    sql "DROP TABLE IF EXISTS ${newDbName}.${tableName3}"
   

    sql """
        RESTORE SNAPSHOT ${newDbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_colocate" = "true"
        )
    """
    syncer.waitAllRestoreFinish(newDbName)

    res = sql "SELECT * FROM ${newDbName}.${tableName1}"
    assertEquals(res.size(), insert_num)
    res = sql "SELECT * FROM ${newDbName}.${tableName2}"
    assertEquals(res.size(), insert_num)

    query = "select * from ${newDbName}.${tableName1} as t1, ${newDbName}.${tableName2} as t2 where t1.id=t2.id;"

    explain {
        sql("${query}")
        contains("COLOCATE")
    }
    res = sql "${query}"
    assertEquals(res.size(), insert_num)
    checkColocateTabletHealth(newDbName)

  
    //cleanup
    sql "DROP TABLE IF EXISTS ${newDbName}.${tableName1}"
    sql "DROP TABLE IF EXISTS ${newDbName}.${tableName2}"
    sql "DROP TABLE IF EXISTS ${newDbName}.${tableName3}"
    sql "DROP DATABASE ${newDbName} FORCE"

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName1}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName2}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName3}"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}
