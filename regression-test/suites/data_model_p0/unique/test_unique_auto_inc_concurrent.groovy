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

suite("test_unique_table_auto_inc_concurrent") {
    
    def table1 = "test_unique_table_auto_inc_concurrent"
    sql "drop table if exists ${table1}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table1}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT,
          `value` int(11) NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    
    def threads = []
    def thread_num = 30
    def rows = 10000
    def iters = 10

    def load_task = {
        (1..iters).each { id -> 
            sql """insert into ${table1}(value) select number from numbers("number" = "${rows}");"""
        }
    }

    (1..thread_num).each { id -> 
        threads.add(Thread.start {
            load_task()
        })
    }

    threads.each { thread -> thread.join() }

    qt_sql "select count(id), count(distinct id) from ${table1};"

    sql "drop table if exists ${table1};"
}

