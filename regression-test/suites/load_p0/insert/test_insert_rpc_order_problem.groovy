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

import org.apache.doris.regression.suite.ClusterOptions

suite('test_insert_rpc_order_problem', 'docker') {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.beNum = 3


    docker(options) {
        def sourceTable = "test_src_table"
        def tableName = "test_dst_table"

        sql "DROP TABLE IF EXISTS ${sourceTable}"
        sql "DROP TABLE IF EXISTS ${tableName}"


        sql """
            CREATE TABLE ${sourceTable} (
                `date` DATE NOT NULL,
                `id` INT,
                `value` VARCHAR(100)
            )
            DISTRIBUTED BY HASH(`date`) BUCKETS 2
            PROPERTIES (
                "replication_num" = "3"
            );
        """

        sql """
            CREATE TABLE ${tableName} (
                `date` DATE NOT NULL,
                `id` INT
            )
            DISTRIBUTED BY HASH(id) BUCKETS 10
            PROPERTIES (
                "replication_num" = "3"
            );
        """
        
        sql """ INSERT INTO ${sourceTable} VALUES ("2025-11-04", 1, "test1"); """
        sql """ INSERT INTO ${sourceTable} SELECT "2025-11-04", number, "test" FROM numbers("number" = "20000"); """
        
        // 期望收到 unknown load_id的错误
        sql """ INSERT INTO ${tableName} SELECT * FROM ${sourceTable}; """




    }

}