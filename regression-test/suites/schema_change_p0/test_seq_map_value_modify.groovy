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

suite('test_seq_map_value_modify', 'p0') {
    def tbName = 'test_seq_map_value_modify'
    sql """ DROP TABLE IF EXISTS ${tbName} """
    sql """
        CREATE TABLE IF NOT EXISTS `$tbName` (
                `a` bigint(20) NULL COMMENT "",
                `b` int(11) NULL COMMENT "",
                `c` int(11) NULL COMMENT "",
                `d` int(11) NULL COMMENT "",
                `s1` int(11) NULL COMMENT "",
                ) ENGINE=OLAP
                UNIQUE KEY(`a`, `b`)
                COMMENT "OLAP"
                DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
                PROPERTIES (
                "enable_unique_key_merge_on_write" = "false",
                "light_schema_change"="true",
                "replication_num" = "1",
                "sequence_mapping.s1" = "c,d"
                );
    """

    sql "insert into ${tbName} values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3);"
    sql "ALTER TABLE ${tbName} MODIFY COLUMN c bigint(20) NULL COMMENT ''"
    sql "insert into ${tbName} values(4,4,4,4,4);"
}
