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

suite('test_create_drop_sync') {
    sql "DROP DATABASE IF EXISTS test_create_drop_sync"
    sql "CREATE DATABASE test_create_drop_sync"
    sql "USE test_create_drop_sync"
    
    sql """
        CREATE TABLE source_table (
            id INT NOT NULL,
            city VARCHAR(32) NOT NULL,
            code VARCHAR(32) NOT NULL
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        CREATE TABLE another_table (
            id INT,
            area VARCHAR(32)
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    // create dictionary
    sql """
        create dictionary dic1 using source_table
        (
            city KEY, 
            id VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """
    waitAllDictionariesReady()

    // check dictionary is created
    def dict_res = sql "SHOW DICTIONARIES"
    assertEquals(dict_res.size(), 1)
    assertEquals(dict_res[0][1], "dic1")
    assertEquals(dict_res[0][4], "NORMAL")

    // drop table and check dictionary is dropped
    sql "DROP TABLE source_table"
    dict_res = sql "SHOW DICTIONARIES"
    assertEquals(dict_res.size(), 0)

    // recreate source table. check could create another dictionary with the same name
    sql """
        CREATE TABLE source_table (
            id INT NOT NULL,
            city VARCHAR(32) NOT NULL,
            code VARCHAR(32) NOT NULL
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        create dictionary dic1 using source_table
        (
            city KEY, 
            id VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """

    // drop and recreate the database. check dic1 is dropped.
    sql "DROP DATABASE test_create_drop_sync"
    sql "CREATE DATABASE test_create_drop_sync"
    sql "USE test_create_drop_sync"
    dict_res = sql "SHOW DICTIONARIES"
    assertEquals(dict_res.size(), 0)
    sql """
        CREATE TABLE source_table (
            id INT NOT NULL,
            city VARCHAR(32) NOT NULL,
            code VARCHAR(32) NOT NULL
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        create dictionary dic1 using source_table
        (
            city KEY, 
            id VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """
}