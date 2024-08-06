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


suite("test_mor_table_with_inverted_index", "p0"){
    def morTable = "test_mor_table_with_inverted_index"
    sql "DROP TABLE IF EXISTS ${morTable}"

    sql """
        CREATE TABLE ${morTable} (
            `foo` varchar(500) NULL,
            `fee` varchar(500) NULL,
            `voo` int DEFAULT '0',
            `vo2` int DEFAULT '0',
            INDEX idx_foo(foo) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true")
        ) ENGINE=OLAP 
        UNIQUE KEY(`foo`, `fee`) DISTRIBUTED BY HASH(`foo`, `fee`) BUCKETS 1  
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "compression" = "ZSTD", "enable_unique_key_merge_on_write" = "false");
    """
    
    sql """ INSERT INTO ${morTable} values ('bar', 'aar', 20, 20); """
    sql """ INSERT INTO ${morTable} values ('bae', 'aae', 20, 20); """
    sql """ INSERT INTO ${morTable} values ('bae', 'aae', 20, 20); """
    sql """ set enable_match_without_inverted_index = false; """
    sql """ set enable_common_expr_pushdown = true; """
    sql "sync"

    qt_sql """ select count() from ${morTable} where foo MATCH_REGEXP 'b*'; """
    qt_sql """ select count() from ${morTable} where foo MATCH_REGEXP 'b*' and fee > 'a'; """
    qt_sql """ select * from ${morTable} where foo MATCH_REGEXP 'b*' order by foo, fee; """
}