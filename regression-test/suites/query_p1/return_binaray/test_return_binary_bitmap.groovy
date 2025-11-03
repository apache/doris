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

suite("test_return_binary_bitmap") {
    def tableName="test_return_binary_bitmap"
    sql "drop table if exists ${tableName};"

    sql """
    CREATE TABLE `${tableName}` (
        `dt` int(11) NULL,
        `page` varchar(10) NULL,
        `user_id` bitmap BITMAP_UNION 
        ) ENGINE=OLAP
        AGGREGATE KEY(`dt`, `page`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`dt`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """
    sql """
        insert into ${tableName} values(1,1,to_bitmap(1)),(1,1,to_bitmap(2)),(1,1,to_bitmap(3)),(1,1,to_bitmap(23332));
    """
    sql "set return_object_data_as_binary=false;"
    def result1 = sql "select * from ${tableName}"
    assertTrue(result1[0][2]==null);

    sql "set return_object_data_as_binary=true;"
    def result2 = sql "select * from ${tableName}"
    assertTrue(result2[0][2]!=null);
}