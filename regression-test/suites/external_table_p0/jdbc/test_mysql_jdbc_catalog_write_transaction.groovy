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

suite("test_mysql_jdbc_catalog_write_transaction", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    // String driver_url = "mysql-connector-j-8.4.0.jar"
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return;
    }

    String catalog_name = "mysql_jdbc_write_transaction_test";
    String ex_db_name = "doris_test";
    String mysql_port = context.config.otherConfigs.get("mysql_57_port");
    String test_table = "test_insert";

    sql """drop catalog if exists ${catalog_name} """

    sql """create catalog if not exists ${catalog_name} properties(
        "type"="jdbc",
        "user"="root",
        "password"="123456",
        "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
        "driver_url" = "${driver_url}",
        "driver_class" = "com.mysql.cj.jdbc.Driver"
    );"""

    sql """switch ${catalog_name}"""
    sql """use ${ex_db_name}"""

    // The test_insert table already exists in the MySQL test environment.
    // It has columns: id varchar, name varchar, age int.
    // We use UUID to ensure our test data doesn't conflict with other tests.

    // ========== Test 1: INSERT without transaction (default behavior) ==========
    // By default, enable_odbc_transcation is false, so autoCommit stays true
    // and each statement auto-commits. This should work regardless of Bug 1.
    String uuid1 = UUID.randomUUID().toString();
    sql """ set enable_odbc_transcation = false """
    sql """ insert into ${test_table} values ('${uuid1}', 'no_txn_test', 100) """

    // Read back — should be visible (auto-committed)
    order_qt_write_no_txn """ select name, age from ${test_table} where id = '${uuid1}' order by age """

    // ========== Test 2: INSERT with transaction enabled ==========
    // This is the critical test for Bug 1.
    // enable_odbc_transcation = true => JdbcJniWriter sets autoCommit=false
    // and the writer must explicitly call conn.commit() before close().
    // Without the Bug 1 fix, the JDBC driver will rollback on close() and
    // the data will be silently lost.
    String uuid2 = UUID.randomUUID().toString();
    sql """ set enable_odbc_transcation = true """
    sql """ insert into ${test_table} values ('${uuid2}', 'txn_test_1', 200) """

    // Read back — data should be visible if commit() was called
    order_qt_write_txn_1 """ select name, age from ${test_table} where id = '${uuid2}' order by age """

    // ========== Test 3: Multi-row INSERT with transaction ==========
    // All rows should be committed atomically
    String uuid3 = UUID.randomUUID().toString();
    sql """ set enable_odbc_transcation = true """
    sql """ insert into ${test_table} values
        ('${uuid3}', 'txn_batch_1', 301),
        ('${uuid3}', 'txn_batch_2', 302),
        ('${uuid3}', 'txn_batch_3', 303) """

    // All three rows should be visible (not rolled back)
    order_qt_write_txn_2 """ select name, age from ${test_table} where id = '${uuid3}' order by age """

    // ========== Test 4: INSERT INTO SELECT with transaction ==========
    String uuid4 = UUID.randomUUID().toString();
    sql """ set enable_odbc_transcation = true """
    sql """ insert into ${test_table} values ('${uuid4}', 'txn_src', 401) """
    sql """ insert into ${test_table} select '${uuid4}', concat(name, '_copy'), age + 1 from ${test_table} where id = '${uuid4}' """

    // Should have 2 rows: original + copy
    order_qt_write_txn_3 """ select name, age from ${test_table} where id = '${uuid4}' order by age """

    // ========== Test 5: Verify row count with assertTrue ==========
    def result = sql """ select count(*) from ${test_table} where id = '${uuid3}' """
    assertTrue(result[0][0] == 3, "Expected 3 rows for uuid3 but got ${result[0][0]} — transaction may not have been committed")

    // clean
    sql """ set enable_odbc_transcation = false """
    sql """switch internal"""
    sql """drop catalog if exists ${catalog_name} """
}
