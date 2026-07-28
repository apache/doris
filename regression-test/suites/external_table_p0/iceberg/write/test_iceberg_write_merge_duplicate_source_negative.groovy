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
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }
    String knownBugEnabled = context.config.otherConfigs.get("enableIcebergKnownBugTest")
    if (knownBugEnabled == null || !knownBugEnabled.equalsIgnoreCase("true")) {
        logger.info("skip isolated Iceberg known-bug test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_merge_duplicate_source_negative"
    String dbName = "iceberg_write_merge_duplicate_source_negative_db"

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

    long snapshotsBefore =
            (sql """select count(*) from duplicate_source_target\$snapshots""")[0][0] as long
    long filesBefore =
            (sql """select count(*) from duplicate_source_target\$files""")[0][0] as long

    // Negative scenario: Iceberg MERGE cardinality permits only one source row
    // to update a target row. The entire statement must fail before publishing.
    test {
        sql """
            merge into duplicate_source_target t
            using (
                select 1 as id, 'B' as region, 'first-update' as payload
                union all
                select 1, 'C', 'second-update'
            ) s
            on t.id = s.id
            when matched then update set
                region = s.region,
                payload = s.payload
        """
        exception "more than one"
    }
    assertEquals(snapshotsBefore,
            (sql """select count(*) from duplicate_source_target\$snapshots""")[0][0] as long)
    assertEquals(filesBefore,
            (sql """select count(*) from duplicate_source_target\$files""")[0][0] as long)
    order_qt_duplicate_source_atomic_state """
        select id, region, payload
        from duplicate_source_target
        order by id
    """
}
