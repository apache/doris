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

suite("test_delete"){
    // prepare test table

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def indexTblName = "test_delete"

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
	CREATE TABLE `${indexTblName}` (
      `a` int NULL COMMENT '',
      `b` varchar(60) NOT NULL COMMENT '',
      `c` char(10) NULL COMMENT '',
      INDEX index_b(b) USING INVERTED  COMMENT '',
      INDEX index_c(c) USING INVERTED  COMMENT ''
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`a`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
    );
    """

    sql """ INSERT INTO `${indexTblName}`(`a`, `b`, `c`) VALUES ('1', '6afef581285b6608bf80d5a4e46cf839', 'aaa'), ('2', '48a33ec3453a28bce84b8f96fe161956', 'bbb'),
                                                                ('3', '021603e7dcfe65d44af0efd0e5aee154', 'ccc'), ('4', 'ee27ee1da291e46403c408e220bed6e1', 'ddd'),
                                                                ('5', 'a648a447b8f71522f11632eba4b4adde', 'eee'), ('6', 'a9fb5c985c90bf05f3bee5ca3ae95260', 'fff'),
                                                                ('7', '0974e7a82e30d1af83205e474fadd0a2', 'ggg'); """


    sql """ DELETE FROM ${indexTblName} WHERE c IN ('aaa','ccc'); """

    qt_sql """ SELECT count(1) as cnt FROM ${indexTblName} WHERE a BETWEEN 1 AND 6 AND b IN ('48a33ec3453a28bce84b8f96fe161956', 'a9fb5c985c90bf05f3bee5ca3ae95260'); """
}