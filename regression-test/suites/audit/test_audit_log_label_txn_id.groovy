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

suite("test_audit_log_label_txn_id","nonConcurrent") {
    def originalAuditEnabled = true
    try {
        def varRes = sql "show global variables like 'enable_audit_plugin'"
        if (varRes.size() > 0) {
            originalAuditEnabled = Boolean.parseBoolean(varRes[0][1].toString())
        }
    } catch (Exception e) {
        // ignore
    }

    try {
        sql "set global enable_audit_plugin = true"
    } catch (Exception e) {
        log.warn("skip this case, because " + e.getMessage())
        assertTrue(e.getMessage().toUpperCase().contains("ADMIN"))
        return
    }

    try {

    sql "drop table if exists audit_label_txn_test"
    sql """
        CREATE TABLE `audit_label_txn_test` (
          `id` bigint,
          `name` varchar(32)
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        )
        """

    def auditSchema = sql """desc internal.__internal_schema.audit_log"""
    def columnNames = auditSchema.collect { it[0].toString().toLowerCase() }
    assertTrue(columnNames.contains("load_label"), "audit_log should contain load_label column")
    assertTrue(columnNames.contains("txn_id"), "audit_log should contain txn_id column")

    def createTableRes = sql """show create table __internal_schema.audit_log"""
    assertTrue(createTableRes[0][1].toLowerCase().contains("load_label"), "show create table should contain load_label column")
    assertTrue(createTableRes[0][1].toLowerCase().contains("txn_id"), "show create table should contain txn_id column")

    sql "truncate table __internal_schema.audit_log"

    def uniqueLabel = "audit_test_" + UUID.randomUUID().toString().replace("-", "_")

    // INSERT with label
    sql "insert into audit_label_txn_test with label ${uniqueLabel} values (1, 'alice')"

    // INSERT without explicit label (auto-generated)
    sql "insert into audit_label_txn_test values (2, 'bob')"

    // UPDATE
    sql "update audit_label_txn_test set name = 'charlie' where id = 2"

    // DELETE
    sql "delete from audit_label_txn_test where id = 1"

    // SELECT (should have empty load_label and -1 txn_id)
    sql "select * from audit_label_txn_test"

    Thread.sleep(6000)
    sql """call flush_audit_log()"""

    // Check INSERT with label has non-empty load_label and valid txn_id
    def retry = 180
    def insertWithLabelRes = sql "select load_label, txn_id from __internal_schema.audit_log where stmt_type = 'INSERT' and load_label = '${uniqueLabel}' order by time desc limit 1"
    while (insertWithLabelRes.isEmpty()) {
        if (retry-- < 0) {
            throw new RuntimeException("It has retried a few but still failed to find INSERT with label")
        }
        sleep(3000)
        insertWithLabelRes = sql "select load_label, txn_id from __internal_schema.audit_log where stmt_type = 'INSERT' and load_label = '${uniqueLabel}' order by time desc limit 1"
    }
    assertTrue(insertWithLabelRes[0][0].toString().length() > 0, "INSERT with label should have non-empty load_label")
    assertTrue(Long.parseLong(insertWithLabelRes[0][1].toString()) > 0, "INSERT should have valid txn_id")

    // Check INSERT without explicit label still has auto-generated load_label and valid txn_id
    retry = 180
    def insertNoLabelRes = sql "select load_label, txn_id from __internal_schema.audit_log where stmt_type = 'INSERT' and stmt like '%audit_label_txn_test%' and load_label != '${uniqueLabel}' order by time desc limit 1"
    while (insertNoLabelRes.isEmpty()) {
        if (retry-- < 0) {
            throw new RuntimeException("It has retried a few but still failed to find INSERT without label")
        }
        sleep(3000)
        insertNoLabelRes = sql "select load_label, txn_id from __internal_schema.audit_log where stmt_type = 'INSERT' and stmt like '%audit_label_txn_test%' and load_label != '${uniqueLabel}' order by time desc limit 1"
    }
    assertTrue(insertNoLabelRes[0][0].toString().length() > 0, "INSERT without explicit label should have auto-generated load_label")
    assertTrue(Long.parseLong(insertNoLabelRes[0][1].toString()) > 0, "INSERT should have valid txn_id")

    // Check UPDATE has load_label and txn_id
    retry = 180
    def updateRes = sql "select load_label, txn_id from __internal_schema.audit_log where stmt_type = 'UPDATE' and stmt like '%audit_label_txn_test%' order by time desc limit 1"
    while (updateRes.isEmpty()) {
        if (retry-- < 0) {
            throw new RuntimeException("It has retried a few but still failed to find UPDATE")
        }
        sleep(3000)
        updateRes = sql "select load_label, txn_id from __internal_schema.audit_log where stmt_type = 'UPDATE' and stmt like '%audit_label_txn_test%' order by time desc limit 1"
    }
    assertTrue(updateRes[0][0].toString().length() > 0, "UPDATE should have load_label")
    assertTrue(Long.parseLong(updateRes[0][1].toString()) > 0, "UPDATE should have valid txn_id")

    // Check DELETE has load_label and txn_id
    retry = 180
    def deleteRes = sql "select load_label, txn_id from __internal_schema.audit_log where stmt_type = 'DELETE' and stmt like '%audit_label_txn_test%' order by time desc limit 1"
    while (deleteRes.isEmpty()) {
        if (retry-- < 0) {
            throw new RuntimeException("It has retried a few but still failed to find DELETE")
        }
        sleep(3000)
        deleteRes = sql "select load_label, txn_id from __internal_schema.audit_log where stmt_type = 'DELETE' and stmt like '%audit_label_txn_test%' order by time desc limit 1"
    }
    assertTrue(deleteRes[0][0].toString().length() > 0, "DELETE should have load_label")
    assertTrue(Long.parseLong(deleteRes[0][1].toString()) > 0, "DELETE should have valid txn_id")

    // Check SELECT has load_label and txn_id columns (load_label should be empty, txn_id should be -1 for non-txn statements)
    retry = 180
    def selectRes = sql "select load_label, txn_id from __internal_schema.audit_log where stmt_type = 'SELECT' and stmt = 'select * from audit_label_txn_test' order by time desc limit 1"
    while (selectRes.isEmpty()) {
        if (retry-- < 0) {
            throw new RuntimeException("It has retried a few but still failed to find SELECT")
        }
        sleep(3000)
        selectRes = sql "select load_label, txn_id from __internal_schema.audit_log where stmt_type = 'SELECT' and stmt = 'select * from audit_label_txn_test' order by time desc limit 1"
    }
    assertTrue(selectRes[0][0] != null, "SELECT should have load_label column (can be empty string)")
    assertTrue(Long.parseLong(selectRes[0][1].toString()) == -1, "SELECT should have txn_id = -1")

    } finally {
        try {
            if (originalAuditEnabled) {
                sql "set global enable_audit_plugin = true"
            } else {
                sql "set global enable_audit_plugin = false"
            }
        } catch (Exception e) {
            log.warn("failed to restore enable_audit_plugin: " + e.getMessage())
        }
    }
}
