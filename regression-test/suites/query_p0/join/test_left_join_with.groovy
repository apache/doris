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

suite("test_left_join_with", "query") {

    def tbName = "test_insert"

    sql """
           CREATE TABLE IF NOT EXISTS ${tbName} (
              `id` varchar(11) NULL COMMENT '唯一标识',
               `name` varchar(10) NULL COMMENT '采集时间',
               `age` int(11) NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            COMMENT 'test'
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false"
            );
         """

    sql """insert into ${tbName} values (1,'a',10),(2,'b',20),(3,'c',30);"""

    qt_select """
                with t1 as (select 1 id)
                select a.id,a.name,a.age
                from ${tbName} a
                join (select id from ${tbName} where id = (select * from t1)) b on a.id = b.id
                ; 
              """

}
