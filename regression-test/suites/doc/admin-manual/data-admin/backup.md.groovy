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

suite("docs/admin-manual/data-admin/backup.md", "p0,nonConcurrent") {
    try {
        multi_sql """
            CREATE DATABASE IF NOT EXISTS example_db;
            USE example_db;
            DROP TABLE IF EXISTS example_tbl;
            CREATE TABLE IF NOT EXISTS example_db.example_tbl(
                a INT
            ) PARTITION BY RANGE(a) (
                PARTITION p1 VALUES LESS THAN (1),
                PARTITION p2 VALUES LESS THAN (2)
            ) DISTRIBUTED BY HASH(a) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
            INSERT INTO example_tbl VALUES (1);
            DROP TABLE IF EXISTS example_tbl2;
            CREATE TABLE example_tbl2 LIKE example_tbl;
            INSERT INTO example_tbl2 SELECT * FROM example_tbl;
        """

        def uuid = UUID.randomUUID().hashCode().abs()
        def syncer = getSyncer()
        /*
         CREATE REPOSITORY `example_repo`
         WITH HDFS
         ON LOCATION "hdfs://hadoop-name-node:54310/path/to/repo/"
         PROPERTIES
         (
         "fs.defaultFS"="hdfs://hdfs_host:port",
         "hadoop.username" = "hadoop"
         );
         */
        syncer.createHdfsRepository("example_repo")

        /*
        CREATE REPOSITORY `s3_repo`
        WITH S3
        ON LOCATION "s3://bucket_name/test"
        PROPERTIES
        (
            "AWS_ENDPOINT" = "http://xxxx.xxxx.com",
            "AWS_ACCESS_KEY" = "xxxx",
            "AWS_SECRET_KEY"="xxx",
            "AWS_REGION" = "xxx"
        );
         */
        syncer.createS3Repository("s3_repo")
        sql """
            BACKUP SNAPSHOT example_db.snapshot_label1${uuid}
            TO example_repo
            ON (example_tbl)
            PROPERTIES ("type" = "full");
        """
        syncer.waitSnapshotFinish("example_db")

        sql """
            BACKUP SNAPSHOT example_db.snapshot_label2${uuid}
            TO example_repo
            ON
            (
               example_tbl PARTITION (p1,p2),
               example_tbl2
            );
        """
        syncer.waitSnapshotFinish("example_db")

        sql """show BACKUP"""
        sql """SHOW SNAPSHOT ON example_repo WHERE SNAPSHOT = "snapshot_label1${uuid}";"""

    } catch (Throwable t) {
        Assertions.fail("examples in docs/admin-manual/data-admin/backup.md failed to exec, please fix it", t)
    }
}
