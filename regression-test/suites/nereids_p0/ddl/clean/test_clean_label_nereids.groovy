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

suite("test_clean_label_nereids") {
    String dbName = "nereids_clean_label"
    String tableName1 = "test_table1"
    String tableName2 = "test_table2"
    String insertLabel = "label_" + System.currentTimeMillis()

    sql """DROP DATABASE IF EXISTS ${dbName}"""
    sql """create database ${dbName}"""
    // Clean up the label in advance
    sql """clean label from ${dbName}"""
    sql """use ${dbName}"""
    sql """
            CREATE TABLE ${dbName}.${tableName1}(
                user_id            BIGINT       NOT NULL ,
                name               VARCHAR(20)           ,
                age                INT
            )
            DUPLICATE KEY(user_id)
            DISTRIBUTED BY HASH(user_id) BUCKETS 10
            properties
            (
            	"replication_num" = "1"
            )
            ;
        """
    sql """
            CREATE TABLE ${dbName}.${tableName2}(
                user_id            BIGINT       NOT NULL COMMENT "user id",
                name               VARCHAR(20)           COMMENT "name",
                age                INT                   COMMENT "age"
            )
            DUPLICATE KEY(user_id)
            DISTRIBUTED BY HASH(user_id) BUCKETS 10
            properties
            (
                "replication_num" = "1"
            )
            ;
        """

    sql """
        INSERT INTO ${dbName}.${tableName1} (user_id, name, age)
        VALUES (1, "Emily", 25),
               (2, "Benjamin", 35),
               (3, "Olivia", 28),
               (4, "Alexander", 60),
               (5, "Ava", 17);
        """

    sql """
        insert into ${dbName}.${tableName2} with LABEL ${insertLabel} select * from ${dbName}.${tableName1};

        """
    sql """
        INSERT INTO ${dbName}.${tableName2} (user_id, name, age)
                VALUES (6, "Kevin", 30);
        INSERT INTO ${dbName}.${tableName2} (user_id, name, age)
                VALUES (7, "Simba", 31);
        """
    //
    def totalLabel = sql """show load from ${dbName}"""
    println totalLabel
    assert totalLabel.size() == isGroupCommitMode() ? 1 : 4
    // clean label
    checkNereidsExecute("clean label ${insertLabel} from ${dbName};")
    checkNereidsExecute("clean label  from ${dbName};")
    def labelResult = sql """show load from ${dbName}"""
    println labelResult
    assert labelResult.size() == 0
    sql """drop database ${dbName}"""
}