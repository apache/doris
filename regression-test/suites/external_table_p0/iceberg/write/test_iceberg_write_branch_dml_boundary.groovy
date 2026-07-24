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

suite("test_iceberg_write_branch_dml_boundary",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_branch_dml_boundary"
    String dbName = "iceberg_write_branch_dml_boundary_db"

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

    sql """drop table if exists branch_dml_boundary"""
    sql """
        create table branch_dml_boundary (
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
    sql """insert into branch_dml_boundary values (1, 'A', 'main')"""
    sql """alter table branch_dml_boundary create branch audit_branch"""
    sql """alter table branch_dml_boundary create tag protected_tag"""

    // WB01-S01: Doris supports INSERT and INSERT OVERWRITE to an Iceberg branch.
    sql """insert into branch_dml_boundary@branch(audit_branch) values (2, 'B', 'branch-insert')"""
    sql """
        insert overwrite table branch_dml_boundary@branch(audit_branch)
        values (3, 'C', 'branch-overwrite')
    """
    order_qt_branch_write """
        select id, region, payload
        from branch_dml_boundary@branch(audit_branch)
        order by id
    """
    order_qt_main_after_branch_write """
        select id, region, payload
        from branch_dml_boundary
        order by id
    """

    long mainSnapshots = (sql """select count(*) from branch_dml_boundary\$snapshots""")[0][0] as long

    // WB01-S02: The current Doris SQL surface does not accept branch-qualified
    // targets for row-level DML. Keep the capability boundary explicit and atomic.
    test {
        sql """delete from branch_dml_boundary@branch(audit_branch) where id = 3"""
        exception "@"
    }
    test {
        sql """
            update branch_dml_boundary@branch(audit_branch)
            set payload = 'updated'
            where id = 3
        """
        exception "@"
    }
    test {
        sql """
            merge into branch_dml_boundary@branch(audit_branch) t
            using (select 3 as id, 'merged' as payload) s
            on t.id = s.id
            when matched then update set payload = s.payload
        """
        exception "@"
    }
    assertEquals(mainSnapshots,
            (sql """select count(*) from branch_dml_boundary\$snapshots""")[0][0] as long)
    order_qt_branch_after_rejected_dml """
        select id, region, payload
        from branch_dml_boundary@branch(audit_branch)
        order by id
    """

    // WB01-S03: Tags are immutable write targets.
    test {
        sql """insert into branch_dml_boundary@branch(protected_tag) values (9, 'T', 'tag-write')"""
        exception "tag"
        exception "not a branch"
    }
}
