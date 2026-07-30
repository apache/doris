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

suite("test_iceberg_write_merge_duplicate_source_negative",
        "p0,external,iceberg,external_docker,external_docker_iceberg,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_merge_duplicate_source_negative"
    String dbName = "iceberg_write_merge_duplicate_source_negative_db"
    String dockerCommand = context.config.otherConfigs.get("externalDockerCommand") ?: "docker"
    String mcImage = "minio/mc:RELEASE.2025-01-17T23-25-50Z"

    def countDataObjects = { String objectPath ->
        def stdout = new StringBuilder()
        def stderr = new StringBuilder()
        def process = new ProcessBuilder("/bin/bash", "-c",
                "${dockerCommand} run --rm --entrypoint /bin/sh ${mcImage} -c "
                        + "'/usr/bin/mc alias set minio http://${externalEnvIp}:${minioPort} "
                        + "admin password >/dev/null "
                        + "&& /usr/bin/mc find ${objectPath} --type f | wc -l'").start()
        process.consumeProcessOutput(stdout, stderr)
        process.waitForOrKill(30000)
        assertEquals(0, process.exitValue(), "Failed to list Iceberg data objects: ${stderr}")
        return stdout.toString().trim() as long
    }

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${restPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1"
        )
    """
    sql """switch ${catalogName}"""
    sql """drop database if exists ${dbName} force"""
    sql """create database ${dbName}"""
    sql """use ${dbName}"""

    sql """drop table if exists duplicate_source_target"""
    sql """
        create table duplicate_source_target (
            id int,
            region string,
            payload string
        )
        partition by list (region) ()
        properties (
            "format-version" = "2",
            "write.delete.mode" = "merge-on-read",
            "write.update.mode" = "merge-on-read",
            "write.merge.mode" = "merge-on-read"
        )
    """
    sql """insert into duplicate_source_target values (1, 'A', 'committed')"""

    String committedFile = (sql """
        select file_path from duplicate_source_target\$files order by file_path limit 1
    """)[0][0].toString()
    int dataDirectoryEnd = committedFile.indexOf('/data/') + '/data'.length()
    assertTrue(dataDirectoryEnd >= '/data'.length(), "Unexpected Iceberg data path: ${committedFile}")
    String dataObjectPath = committedFile.substring(0, dataDirectoryEnd)
            .replaceFirst('^s3a?://warehouse', 'minio/warehouse')
    long objectsBefore = countDataObjects(dataObjectPath)

    long snapshotsBefore =
            (sql """select count(*) from duplicate_source_target\$snapshots""")[0][0] as long
    long filesBefore =
            (sql """select count(*) from duplicate_source_target\$files""")[0][0] as long

    // Negative scenario: Iceberg MERGE cardinality permits only one source row
    // to update a target row. Tiny blocks and files force a valid row to roll before
    // the late duplicate, and the entire statement must still leave no data object behind.
    sql """set batch_size = 1"""
    // A single pipeline instance preserves the ordered source sequence needed to prove late cleanup.
    sql """set parallel_pipeline_task_num = 1"""
    sql """set iceberg_write_target_file_size_bytes = 1"""
    test {
        sql """
            merge into duplicate_source_target t
            using (
                select id, region, payload
                from (
                    select 1 as seq, 2 as id, 'B' as region, 'valid-insert' as payload
                    union all
                    select 2, 1, 'B', 'first-update'
                    union all
                    select 3, 1, 'C', 'second-update'
                ) ordered_source
                order by seq
            ) s
            on t.id = s.id
            when matched then update set
                region = s.region,
                payload = s.payload
            when not matched then insert (id, region, payload)
                values (s.id, s.region, s.payload)
        """
        exception "multiple source rows matched the same target row"
    }
    assertEquals(snapshotsBefore,
            (sql """select count(*) from duplicate_source_target\$snapshots""")[0][0] as long)
    assertEquals(filesBefore,
            (sql """select count(*) from duplicate_source_target\$files""")[0][0] as long)
    assertEquals(objectsBefore, countDataObjects(dataObjectPath))
    order_qt_duplicate_source_atomic_state """
        select id, region, payload
        from duplicate_source_target
        order by id
    """

    // A sibling delete close can fail only after the data side has closed successfully. The outer
    // MERGE still owns and must remove those unpublished data objects.
    long siblingFailureObjectsBefore = countDataObjects(dataObjectPath)
    try {
        GetDebugPoint().enableDebugPointForAllBEs("VIcebergDeleteSink.close.inject_failure")
        test {
            sql """
                merge into duplicate_source_target t
                using (
                    select 1 as id, 'B' as region, 'updated' as payload
                    union all
                    select 3, 'C', 'inserted'
                ) s
                on t.id = s.id
                when matched then update set
                    region = s.region,
                    payload = s.payload
                when not matched then insert (id, region, payload)
                    values (s.id, s.region, s.payload)
            """
            exception "injected Iceberg delete close failure"
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("VIcebergDeleteSink.close.inject_failure")
    }
    assertEquals(snapshotsBefore,
            (sql """select count(*) from duplicate_source_target\$snapshots""")[0][0] as long)
    assertEquals(filesBefore,
            (sql """select count(*) from duplicate_source_target\$files""")[0][0] as long)
    assertEquals(siblingFailureObjectsBefore, countDataObjects(dataObjectPath))
    order_qt_sibling_close_atomic_state """
        select id, region, payload
        from duplicate_source_target
        order by id
    """
}
