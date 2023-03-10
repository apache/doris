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

suite("test_unsigned_int_compatibility") {
    def tableName = "test_unsigned_int_compatibility"

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `value1` UNSIGNED INT COMMENT "value2"
        )
        UNIQUE KEY(`user_id`, `city`) 
        DISTRIBUTED BY HASH(`user_id`)
        PROPERTIES ( 
            "replication_num" = "1" , 
            "light_schema_change" = "true"
        );
        """
    qt_desc_tb "DESC ${tableName}"
    
    sql """ 
        INSERT INTO ${tableName} VALUES
                (1, 'Beijing', 21474836478);
        """
    qt_select_tb "SELECT * FROM ${tableName}"

    sql """
        ALTER table ${tableName} ADD COLUMN value2 UNSIGNED INT;
        """
    qt_desc_tb "DESC ${tableName}"
    
    sql """ 
        INSERT INTO ${tableName} VALUES
                (2, 'Beijing', 21474836478, 21474836478);
        """
    qt_select_tb "SELECT * FROM ${tableName} order by user_id"

    sql "DROP TABLE ${tableName}"
}