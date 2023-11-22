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

suite("test_string_to_decimal") {
    sql """
        drop table if exists string_decimal_table;
    """
    
    sql """
        CREATE TABLE `string_decimal_table` (
        `bskrf` decimal(16, 9) NULL 
        ) ENGINE=OLAP
        UNIQUE KEY(`bskrf`)
        DISTRIBUTED BY HASH(`bskrf`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        ); 
    """

    sql """
        insert into string_decimal_table values('0.000000000000000E+00');
    """
}