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

import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

suite("test_iceberg_write_concurrent_merge_invariants",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_iceberg_write_concurrent_merge_invariants"
    String dbName = "iceberg_write_concurrent_merge_invariants_db"

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${restPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1",
            "meta.cache.iceberg.table.ttl-second" = "0",
            "meta.cache.iceberg.schema.ttl-second" = "0"
        )
    """
    sql """switch ${catalogName}"""
    sql """drop database if exists ${dbName} force"""
    sql """create database ${dbName}"""
    sql """use ${dbName}"""

    sql """drop table if exists concurrent_merge"""
    sql """
        create table concurrent_merge (
            id int not null,
            region string,
            payload string
        )
        partition by list (region) ()
        properties (
            "format-version" = "2",
            "write.format.default" = "parquet",
            "write.delete.mode" = "merge-on-read",
            "write.update.mode" = "merge-on-read",
            "write.merge.mode" = "merge-on-read",
            "write.merge.isolation-level" = "serializable"
        )
    """
    sql """insert into concurrent_merge values (1, 'A', 'base')"""
    long snapshotsBefore = (sql """select count(*) from concurrent_merge\$snapshots""")[0][0] as long

    def isExpectedIcebergCommitConflict = { Exception exception ->
        String message = exception.toString()
        return message.contains("org.apache.iceberg.exceptions.ValidationException")
                || message.contains("org.apache.iceberg.exceptions.CommitFailedException")
                || message.contains("Found conflicting files")
    }

    // WC02-S01: A readiness barrier prevents a fast worker from completing
    // before the second session is even eligible for concurrent dispatch.
    // The exact winner is intentionally unspecified; cardinality, snapshot
    // accounting and cross-engine visibility are deterministic invariants.
    CountDownLatch ready = new CountDownLatch(2)
    CountDownLatch start = new CountDownLatch(1)
    List<String> successes = Collections.synchronizedList(new ArrayList<String>())
    List<String> failures = Collections.synchronizedList(new ArrayList<String>())

    def first = thread("iceberg-merge-one") {
        ready.countDown()
        start.await()
        try {
            sql """
                merge into ${catalogName}.${dbName}.concurrent_merge t
                using (select 1 as id, 'B' as region, 'winner-one' as payload) s
                on t.id = s.id
                when matched then update set region = s.region, payload = s.payload
            """
            successes.add("one")
        } catch (Exception e) {
            // Only an Iceberg optimistic-validation conflict is an admissible
            // loser; planner, RPC, catalog and BE failures must fail the suite.
            if (!isExpectedIcebergCommitConflict(e)) {
                throw e
            }
            failures.add(e.getMessage())
        }
    }
    def second = thread("iceberg-merge-two") {
        ready.countDown()
        start.await()
        try {
            sql """
                merge into ${catalogName}.${dbName}.concurrent_merge t
                using (select 1 as id, 'C' as region, 'winner-two' as payload) s
                on t.id = s.id
                when matched then update set region = s.region, payload = s.payload
            """
            successes.add("two")
        } catch (Exception e) {
            if (!isExpectedIcebergCommitConflict(e)) {
                throw e
            }
            failures.add(e.getMessage())
        }
    }
    assertTrue(ready.await(30, TimeUnit.SECONDS),
            "Both MERGE workers must reach the dispatch barrier")
    start.countDown()
    first.get()
    second.get()

    assertTrue(successes.size() >= 1)
    assertEquals(2, successes.size() + failures.size())
    assertEquals(1L, (sql """select count(*) from concurrent_merge where id = 1""")[0][0] as long)
    assertEquals(snapshotsBefore + successes.size(),
            (sql """select count(*) from concurrent_merge\$snapshots""")[0][0] as long)
    def visible = sql """
        select payload
        from concurrent_merge
        where id = 1
    """
    assertTrue(["winner-one", "winner-two"].contains(visible[0][0].toString()))

    spark_iceberg """refresh table demo.${dbName}.concurrent_merge"""
    def sparkRows = spark_iceberg """
        select id, region, payload
        from demo.${dbName}.concurrent_merge
        order by id
    """
    def dorisRows = sql """
        select id, region, payload
        from concurrent_merge
        order by id
    """
    assertSparkDorisResultEquals(sparkRows, dorisRows)

    // WC02-S02: Use the same readiness invariant for non-conflicting appends;
    // otherwise sequential execution can falsely satisfy the row-count oracle.
    CountDownLatch appendReady = new CountDownLatch(2)
    CountDownLatch appendStart = new CountDownLatch(1)
    def appendOne = thread("iceberg-append-one") {
        appendReady.countDown()
        appendStart.await()
        sql """
            insert into ${catalogName}.${dbName}.concurrent_merge
            select number + 10, 'append-one', concat('one-', number)
            from numbers('number' = '128')
        """
    }
    def appendTwo = thread("iceberg-append-two") {
        appendReady.countDown()
        appendStart.await()
        sql """
            insert into ${catalogName}.${dbName}.concurrent_merge
            select number + 1000, 'append-two', concat('two-', number)
            from numbers('number' = '128')
        """
    }
    assertTrue(appendReady.await(30, TimeUnit.SECONDS),
            "Both append workers must reach the dispatch barrier")
    appendStart.countDown()
    appendOne.get()
    appendTwo.get()
    order_qt_concurrent_append_counts """
        select region, count(*), count(distinct id)
        from concurrent_merge
        where region in ('append-one', 'append-two')
        group by region
        order by region
    """

    // Refresh after both appends so the cross-engine oracle covers the
    // concurrent commits rather than only the earlier MERGE result.
    spark_iceberg """refresh table demo.${dbName}.concurrent_merge"""
    sparkRows = spark_iceberg """
        select id, region, payload
        from demo.${dbName}.concurrent_merge
        order by id
    """
    dorisRows = sql """
        select id, region, payload
        from concurrent_merge
        order by id
    """
    assertSparkDorisResultEquals(sparkRows, dorisRows)
}
