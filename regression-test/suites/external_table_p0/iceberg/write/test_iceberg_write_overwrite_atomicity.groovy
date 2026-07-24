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

suite("test_iceberg_write_overwrite_atomicity",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_overwrite_atomicity"
    String dbName = "iceberg_write_overwrite_atomicity_db"

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

    sql """drop table if exists overwrite_atomicity"""
    sql """
        create table overwrite_atomicity (
            id int not null,
            region string,
            payload string
        )
        partition by list (region) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "parquet"
        )
    """
    sql """
        insert into overwrite_atomicity values
            (1, 'A', 'committed-a'),
            (2, 'B', 'committed-b')
    """
    sql """alter table overwrite_atomicity create branch retry_branch"""
    sql """set enable_strict_cast = true"""

    // WO03-S01: A distributed expression failure must not publish a partial
    // overwrite, remove an existing partition, or create a new snapshot.
    long snapshotsBeforeFailure =
            (sql """select count(*) from overwrite_atomicity\$snapshots""")[0][0] as long
    long filesBeforeFailure =
            (sql """select count(*) from overwrite_atomicity\$files""")[0][0] as long
    test {
        sql """
            insert overwrite table overwrite_atomicity
            select cast(if(number = 2, 'invalid-id', cast(number + 10 as string)) as int),
                   if(number % 2 = 0, 'A', 'C'),
                   concat('candidate-', number)
            from numbers('number' = '8')
        """
        exception "can't cast to INT in strict mode"
    }
    assertEquals(snapshotsBeforeFailure,
            (sql """select count(*) from overwrite_atomicity\$snapshots""")[0][0] as long)
    assertEquals(filesBeforeFailure,
            (sql """select count(*) from overwrite_atomicity\$files""")[0][0] as long)
    order_qt_overwrite_failure_state """
        select id, region, payload
        from overwrite_atomicity
        order by id
    """

    // WO03-S02: The same invariant applies to a branch-qualified overwrite.
    test {
        sql """
            insert overwrite table overwrite_atomicity@branch(retry_branch)
            select cast(if(number = 3, 'invalid-id', cast(number + 20 as string)) as int),
                   'A',
                   concat('branch-candidate-', number)
            from numbers('number' = '8')
        """
        exception "can't cast to INT in strict mode"
    }
    order_qt_branch_overwrite_failure_state """
        select id, region, payload
        from overwrite_atomicity@branch(retry_branch)
        order by id
    """
    order_qt_main_after_branch_overwrite_failure """
        select id, region, payload
        from overwrite_atomicity
        order by id
    """

    // WO03-S03: Retry the corrected logical operation. Each replacement row
    // becomes visible exactly once and only one new main snapshot is committed.
    sql """
        insert overwrite table overwrite_atomicity
        select number + 10,
               if(number % 2 = 0, 'A', 'C'),
               concat('candidate-', number)
        from numbers('number' = '8')
    """
    assertEquals(snapshotsBeforeFailure + 1,
            (sql """select count(*) from overwrite_atomicity\$snapshots""")[0][0] as long)
    order_qt_overwrite_retry """
        select id, region, payload, count(*)
        from overwrite_atomicity
        group by id, region, payload
        order by id
    """
}
