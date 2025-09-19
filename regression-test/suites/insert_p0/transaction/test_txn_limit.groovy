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

suite("test_txn_limit", 'nonConcurrent') {
    if (isCloudMode()) {
        return
    }

    def configResult = sql "SHOW FRONTEND CONFIG LIKE 'max_running_txn_num_per_db'"
    logger.info("configResult: ${configResult}")
    assert configResult.size() == 1


    def originTxnNum = configResult[0][1] as long
    logger.info("max_running_txn_num_per_db: $originTxnNum")

    if (originTxnNum == 0) {
        originTxnNum = 1000 // default value is 1000, if it is set to 0, we will use 1000
    }

    sql "ADMIN SET FRONTEND CONFIG ('max_running_txn_num_per_db' = '0')"
    configResult = sql "SHOW FRONTEND CONFIG LIKE 'max_running_txn_num_per_db'"
    logger.info("configResult: ${configResult}")

    def create_db_and_table = { dbName, tableName ->
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql "DROP DATABASE IF EXISTS ${dbName}"
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `user_id` bigint default 999,
                `group_id` bigint,
                `id` bigint,
                `vv` variant,
                INDEX idx_col1 (user_id) USING INVERTED
                ) ENGINE=OLAP
            UNIQUE KEY(user_id, group_id)
            DISTRIBUTED BY HASH (user_id) BUCKETS 1
            PROPERTIES(
                    "store_row_column" = "true",
                    "replication_num" = "1"
                    );
        """
    }

    def dbName = "test_txn_limit_db"
    def tableName = "${dbName}.test_txn_limit"

    create_db_and_table("${dbName}", "${tableName}")

    test {
        sql """insert into ${tableName} values(1,1,5,'{"b":"b"}'),(1,1,4,'{"b":"b"}'),(1,1,3,'{"b":"b"}')"""
        exception "current running txns on db"
    }

    sql "ADMIN SET FRONTEND CONFIG ('max_running_txn_num_per_db' = '${originTxnNum}')"
    logger.info("reset max_running_txn_num_per_db to ${originTxnNum}")

    // test max_publishing_txn_num_per_table
    // this config is used to limit the number of publishing txns on a table
    dbName = "test_txn_limit_db1"
    tableName = "${dbName}.test_txn_limit"
    create_db_and_table("${dbName}", "${tableName}")

    def maxPublishingTxnConfig = sql "SHOW FRONTEND CONFIG LIKE 'max_publishing_txn_num_per_table'"
    assert maxPublishingTxnConfig.size() == 1
    def maxPublishingTxnNum = maxPublishingTxnConfig[0][1] as long
    logger.info("max_publishing_txn_num_per_table: ${maxPublishingTxnConfig[0][1]}")

    sql "ADMIN SET FRONTEND CONFIG ('max_publishing_txn_num_per_table' = '0')"
        test {
        sql """insert into ${tableName} values(1,1,5,'{"b":"b"}'),(1,1,4,'{"b":"b"}'),(1,1,3,'{"b":"b"}')"""
        exception "current committed txns on table"
    }
    sql "ADMIN SET FRONTEND CONFIG ('max_publishing_txn_num_per_table' = '${maxPublishingTxnNum}')"
}
