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

suite("test_show_transaction", "p0") {
    // define a sql table
    def testTable = "test_show_transaction"

    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              `k1` INT NULL COMMENT "",
              `k2` STRING NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            );
            """

    try {
      // not exist with id
      sql "show transaction where id = -1"
      assertTrue(true)
    } catch (Exception e) {
      assertTrue(false)
    }

    try {
      // not exist with label
      sql """show transaction where label = "label_not_exist"
                """
      assertTrue(true)
    } catch (Exception e) {
      assertTrue(false)
    }

    try {
      // throw exception with message "status should be prepare/precommitted/committed/visible/aborted"
      sql """show transaction where status="nothing"
            """
      assertTrue(false)
    } catch (Exception e) {
      assertTrue(true)
    }

    def uuid = UUID.randomUUID().toString().replaceAll("-", "");
    sql """ INSERT INTO ${testTable} WITH LABEL label_test_show_transaction_${uuid} VALUES(100, 'doris')  """
    def res = sql_return_maparray """ show transaction where label = 'label_test_show_transaction_${uuid}'  """
    print("show transaction result : " + res)
    def reslike = sql_return_maparray """ show transaction where label like 'label_test_show_transaction_${uuid}%'  """
    assertTrue(res.equals(reslike))
}
