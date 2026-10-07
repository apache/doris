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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

suite('test_concurrent_alter_colocate') {
    if (isCloudMode()) {
        return
    }
    def dbName = (sql 'SELECT DATABASE()')[0][0].toString()
    // Recycled tables may retain old groups; use fresh groups to exercise concurrent first admission.
    def suffix = UUID.randomUUID().toString().replace('-', '')
    def compatibleName = "concurrent_compatible_${suffix}"
    def incompatibleName = "concurrent_incompatible_${suffix}"
    def forceReplicaNum = getFeConfig('force_olap_table_replication_num') as int
    def replicaNum = forceReplicaNum > 0 ? forceReplicaNum : 1
    sql 'DROP TABLE IF EXISTS concurrent_colocate_left'
    sql 'DROP TABLE IF EXISTS concurrent_colocate_right'
    sql 'DROP TABLE IF EXISTS concurrent_colocate_int'
    sql 'DROP TABLE IF EXISTS concurrent_colocate_bigint'
    sql """CREATE TABLE concurrent_colocate_left (id INT NOT NULL, v INT)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 16
           PROPERTIES ("replication_num" = "${replicaNum}")"""
    sql """CREATE TABLE concurrent_colocate_right (id INT NOT NULL, v INT)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 16
           PROPERTIES ("replication_num" = "${replicaNum}")"""
    sql """CREATE TABLE concurrent_colocate_int (id INT NOT NULL, v INT)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 16
           PROPERTIES ("replication_num" = "${replicaNum}")"""
    sql """CREATE TABLE concurrent_colocate_bigint (id BIGINT NOT NULL, v INT)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 16
           PROPERTIES ("replication_num" = "${replicaNum}")"""
    sql '''INSERT INTO concurrent_colocate_left
           SELECT number, number + 100 FROM numbers("number" = "16")'''
    sql '''INSERT INTO concurrent_colocate_right
           SELECT number, number + 200 FROM numbers("number" = "16")'''
    sql '''INSERT INTO concurrent_colocate_int
           SELECT number, number + 300 FROM numbers("number" = "16")'''
    sql '''INSERT INTO concurrent_colocate_bigint
           SELECT number, number + 400 FROM numbers("number" = "16")'''

    def typeError = 'Colocate tables distribution columns must have the same data type'
    def concurrentAlter = { List<String> tables, String groupName, boolean allowTypeError ->
        def ready = new CountDownLatch(2)
        def start = new CountDownLatch(1)
        def successes = Collections.synchronizedList(new ArrayList<String>())
        def failures = Collections.synchronizedList(new ArrayList<String>())
        // DSL threads use separate connections; release both ALTERs after warmup and propagate unexpected errors.
        def workers = tables.collect { tableName ->
            thread("alter-colocate-${tableName}") {
                sql "USE `${dbName}`"
                sql 'SELECT 1'
                ready.countDown()
                if (!start.await(60, TimeUnit.SECONDS)) {
                    throw new IllegalStateException('Timed out waiting to start concurrent ALTER')
                }
                try {
                    sql "ALTER TABLE ${tableName} SET (\"colocate_with\" = \"${groupName}\")"
                    successes.add(tableName)
                } catch (Exception e) {
                    if (!allowTypeError || !e.toString().contains(typeError)) {
                        throw e
                    }
                    failures.add(tableName)
                }
            }
        }
        try {
            if (!ready.await(60, TimeUnit.SECONDS)) {
                throw new IllegalStateException('Both ALTER connections must reach the start barrier')
            }
        } finally {
            start.countDown()
        }
        workers.each { it.get(120, TimeUnit.SECONDS) }
        return [successes: successes, failures: failures]
    }
    def compatible = concurrentAlter(
            ['concurrent_colocate_left', 'concurrent_colocate_right'], compatibleName, false)
    qt_compatible_outcomes "SELECT ${compatible.successes.size()}, ${compatible.failures.size()}"
    def incompatible = concurrentAlter(
            ['concurrent_colocate_int', 'concurrent_colocate_bigint'], incompatibleName, true)
    qt_incompatible_outcomes "SELECT ${incompatible.successes.size()}, ${incompatible.failures.size()}"
    // Scheduling determines the winner; output only invariants, excluding random group names and table IDs.
    def winningType = incompatible.successes.get(0) == 'concurrent_colocate_int' ? 'INT' : 'BIGINT'
    test {
        sql "ALTER TABLE ${incompatible.failures.get(0)} SET (\"colocate_with\" = \"${incompatibleName}\")"
        exception typeError
    }
    def groupNames = ["${dbName}.${compatibleName}".toString(), "${dbName}.${incompatibleName}".toString()]
    def groups
    awaitUntil(180) {
        groups = sql_return_maparray("SHOW PROC '/colocation_group'").findAll {
            groupNames.contains(it.GroupName.toString())
        }.collectEntries { [(it.GroupName.toString()): it] }
        groups.size() == 2 && groups.values().every { it.IsStable.toString().toBoolean() }
    }
    def compatibleGroup = groups[groupNames[0]]
    def incompatibleGroup = groups[groupNames[1]]
    def sequences = groups.values().collect { group ->
        sql("SHOW PROC '/colocation_group/${group.GroupId}'").collect { row ->
            [bucket: row[0].toString().toInteger(), backends: row.drop(1).collectMany { it.toString().tokenize(', ') }]
        }.sort { it.bucket }
    }
    qt_metadata """SELECT ${groups.size()}, ${compatibleGroup.TableIds.toString().tokenize(', ').size()},
            ${incompatibleGroup.TableIds.toString().tokenize(', ').size()},
            ${compatibleGroup.BucketsNum}, ${incompatibleGroup.BucketsNum},
            ${sequences[0].size()}, ${sequences[1].size()},
            ${sequences.every { it.collect { bucket -> bucket.bucket } == (0..<16).toList() }},
            ${sequences.every { it.every { bucket -> bucket.backends.size() == replicaNum } }},
            ${compatibleGroup.DistCols.toString().equalsIgnoreCase('INT')},
            ${incompatibleGroup.DistCols.toString().equalsIgnoreCase(winningType)}"""
    order_qt_join '''SELECT l.id, l.v, r.v
                    FROM concurrent_colocate_left l JOIN concurrent_colocate_right r ON l.id = r.id'''
    qt_incompatible_data '''SELECT
            (SELECT COUNT(*) FROM concurrent_colocate_int), (SELECT SUM(v) FROM concurrent_colocate_int),
            (SELECT COUNT(*) FROM concurrent_colocate_bigint), (SELECT SUM(v) FROM concurrent_colocate_bigint)'''
}
