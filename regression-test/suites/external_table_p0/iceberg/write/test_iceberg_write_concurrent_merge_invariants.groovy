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

    // WC02-S01: Start two conflicting MERGE statements at the same barrier.
    // The exact winner is intentionally unspecified; cardinality, snapshot
    // accounting and cross-engine visibility are deterministic invariants.
    CountDownLatch start = new CountDownLatch(1)
    List<String> successes = Collections.synchronizedList(new ArrayList<String>())
    List<String> failures = Collections.synchronizedList(new ArrayList<String>())

    def first = thread {
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
            failures.add(e.getMessage())
        }
    }
    def second = thread {
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
            failures.add(e.getMessage())
        }
    }
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

    // WC02-S02: Concurrent non-conflicting appends must both commit without
    // duplicate ids or lost rows.
    CountDownLatch appendStart = new CountDownLatch(1)
    def appendOne = thread {
        appendStart.await()
        sql """
            insert into ${catalogName}.${dbName}.concurrent_merge
            select number + 10, 'append-one', concat('one-', number)
            from numbers('number' = '128')
        """
    }
    def appendTwo = thread {
        appendStart.await()
        sql """
            insert into ${catalogName}.${dbName}.concurrent_merge
            select number + 1000, 'append-two', concat('two-', number)
            from numbers('number' = '128')
        """
    }
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
}
