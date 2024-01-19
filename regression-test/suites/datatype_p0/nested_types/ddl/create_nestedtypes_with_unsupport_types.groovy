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

suite("create_nestedtypes_with_unsupport_types") {
    // this cases make sure that now we not support some types in array|map|struct
    def not_support_types = ["HLL", "JSONB", "BITMAP", "QUANTILE_STATE"] //"VARIANT"]
    
    // array
    def create_array_table = {testTablex, columnType ->
        test {
            sql """
            CREATE TABLE IF NOT EXISTS ${testTablex} (
              `k1` INT(11) NULL,
              `k2` array<${columnType}> NULL
            )
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            )
            """
            exception("ARRAY unsupported sub-type")
        }
    }

    for (String typ : not_support_types) {
        create_array_table.call("test_array_"+typ, typ)
    }

    // map
    def create_map_table = {testTablex, columnType, typeIsKey ->
        test {
            if (typeIsKey) {
                sql """
            CREATE TABLE IF NOT EXISTS ${testTablex} (
              `k1` INT(11) NULL,
              `k2` map<${columnType}, String> NULL
            )
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            )
            """
            } else {
                sql """
            CREATE TABLE IF NOT EXISTS ${testTablex} (
              `k1` INT(11) NULL,
              `k2` map<String, ${columnType}> NULL
            )
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            )
            """
            }
            exception("MAP unsupported sub-type")
        }
    }

    for (String typ : not_support_types) {
        create_map_table.call("test_map_k_"+typ, typ, true)
        create_map_table.call("test_map_v_"+typ, typ, false)
    }

    // struct
    def create_struct_table = {testTablex, columnType ->
        test {
            sql """
            CREATE TABLE IF NOT EXISTS ${testTablex} (
              `k1` INT(11) NULL,
              `k2` STRUCT<f1:${columnType}> NULL
            )
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            )
            """
            exception("STRUCT unsupported sub-type")
        }
    }
    for (String typ : not_support_types) {
        create_struct_table.call("test_struct_"+typ, typ)
    }
}
