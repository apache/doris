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

suite("test_sequence_col_default_value") {

    def tableName = "test_sequence_col_default_value"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE ${tableName} 
        (
            project_id BIGINT NOT NULL ,
            consume_date DATETIMEV2 NOT NULL,
            order_id BIGINT NOT NULL ,
            combo_extend JSONB DEFAULT NULL,
            age INT,
            name varchar(20),
            write_time DATETIME DEFAULT CURRENT_TIMESTAMP
        ) UNIQUE KEY(project_id, consume_date,order_id)
        DISTRIBUTED BY HASH(project_id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true",
            "function_column.sequence_col" = 'write_time'
        );
    """

    // test insert into.
    sql " insert into ${tableName} (order_id,project_id,consume_date,age,name) values (1231234356,370040365,'2023-01-12',NULL,'a'); "
    sql " insert into ${tableName} (order_id,project_id,consume_date,age,name) values (1231234356,370040365,'2023-01-12',NULL,'b'); "

    qt_sql """
        select *
        from ${tableName} where name = 'b'  
    """

    sql " DROP TABLE IF EXISTS ${tableName} "

}
