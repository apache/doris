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

import org.junit.jupiter.api.Assertions;

suite("docs/admin-manual/data-admin/restore.md", "p0,nonConcurrent") {
    try {
        multi_sql """
            CREATE DATABASE IF NOT EXISTS example_db1;
            USE example_db1;
            DROP TABLE IF EXISTS backup_tbl;
            CREATE TABLE IF NOT EXISTS example_db1.backup_tbl(
                a INT
            ) PARTITION BY RANGE(a) (
                PARTITION p1 VALUES LESS THAN (1),
                PARTITION p2 VALUES LESS THAN (2)
            ) DISTRIBUTED BY HASH(a) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
            INSERT INTO backup_tbl VALUES (1);
            DROP TABLE IF EXISTS backup_tbl2;
            CREATE TABLE backup_tbl2 LIKE backup_tbl;
            INSERT INTO backup_tbl2 SELECT * FROM backup_tbl;
        """

        def uuid = UUID.randomUUID().hashCode().abs()
        def syncer = getSyncer()
        syncer.createS3Repository("example_repo")
        sql """
            BACKUP SNAPSHOT example_db1.snapshot_1${uuid}
            TO example_repo
            ON (backup_tbl)
            PROPERTIES ("type" = "full");
        """
        syncer.waitSnapshotFinish("example_db1")
        sql """
            BACKUP SNAPSHOT example_db1.snapshot_2${uuid}
            TO example_repo
            ON (backup_tbl, backup_tbl2)
            PROPERTIES ("type" = "full");
        """
        syncer.waitSnapshotFinish("example_db1")

        multi_sql """
            truncate table backup_tbl;
            truncate table backup_tbl2;
        """

        var timestamp = syncer.getSnapshotTimestamp("example_repo", "snapshot_1${uuid}")
        sql """
            RESTORE SNAPSHOT example_db1.`snapshot_1${uuid}`
            FROM `example_repo`
            ON ( `backup_tbl` )
            PROPERTIES
            (
                "backup_timestamp"="${timestamp}",
                "replication_num" = "1"
            );
        """
        syncer.waitAllRestoreFinish("example_db1")

        var timestamp2 = syncer.getSnapshotTimestamp("example_repo", "snapshot_2${uuid}")
        sql """
            RESTORE SNAPSHOT example_db1.`snapshot_2${uuid}`
            FROM `example_repo`
            ON
            (
                `backup_tbl` PARTITION (`p1`, `p2`),
                `backup_tbl2` AS `new_tbl`
            )
            PROPERTIES
            (
                "backup_timestamp"="${timestamp2}",
                "replication_num" = "1"
            );
        """
        syncer.waitAllRestoreFinish("example_db1")

        sql """SHOW RESTORE"""

    } catch (Throwable t) {
        Assertions.fail("examples in docs/admin-manual/data-admin/restore.md failed to exec, please fix it", t)
    }
}
