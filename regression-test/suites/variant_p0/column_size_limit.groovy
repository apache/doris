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
import groovy.json.JsonBuilder

suite("regression_test_variant_column_limit"){
    def table_name = "var_column_limit"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "false");
    """
    def jsonBuilder = new JsonBuilder()
    def root = jsonBuilder {
        // Generate 4097 fields
        (1..4097).each { fieldNumber ->
            "field$fieldNumber" fieldNumber
        }
    }

    String jsonString = jsonBuilder.toPrettyString()
    test {
        sql """insert into ${table_name} values (1, '$jsonString')"""
        exception("Reached max column size limit")
    }
    sql """insert into ${table_name} values (1, '{"a" : 1, "b" : 2, "c" : 3}')"""

}