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

suite("regression_test_variant_complexjson", "variant_type_complex_json") {
    def create_table = { table_name ->
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY RANDOM BUCKETS 5 
            properties("replication_num" = "1", "disable_auto_compaction" = "true");
        """
    }
    table_name = "complexjson"
    create_table table_name
    sql """insert into ${table_name} values (1, '{
    "id": 1,
    "key_0":[
        {
            "key_1":[
                {
                    "key_3":[
                        {"key_7":1025,"key_6":25.5,"key_4":1048576,"key_5":0.0001048576},
                        {"key_7":2,"key_6":"","key_4":null}
                    ]
                }
            ]
        },
        {
            "key_1":[
                {
                    "key_3":[
                        {"key_7":-922337203685477580.8,"key_6":"aqbjfiruu","key_5":-1},
                        {"key_7":65537,"key_6":"","key_4":""}
                    ]
                },
                {
                    "key_3":[
                        {"key_7":21474836.48,"key_4":"ghdqyeiom","key_5":1048575}
                    ]
                }
            ]
        }
    ]
    }')"""
    // qt_sql """SELECT v:key_0.key_1.key_3.key_4, v:key_0.key_1.key_3.key_5, \
    // v:key_0.key_1.key_3.key_6, v:key_0.key_1.key_3.key_7 FROM ${table_name} ORDER BY v:id"""
    qt_sql """SELECT * from ${table_name} order by cast(v:id as int)"""

    table_name = "complexjson2"
    create_table table_name
    sql """insert into ${table_name} values (1, '{
    "id": 1,
    "key_1":[
        {
            "key_2":[
                {
                    "key_3":[
                        {"key_8":65537},
                        {
                            "key_4":[
                                {"key_5":-0.02},
                                {"key_7":1023},
                                {"key_7":1,"key_6":9223372036854775807}
                            ]
                        },
                        {
                            "key_4":[{"key_7":65537,"key_6":null}]
                        }
                    ]
                }
            ]
        }
    ]
    }')""" 
    // qt_sql """SELECT \
    // v:key_1.key_2.key_3.key_8, \
    // v:key_1.key_2.key_3.key_4.key_5, \
    // v:key_1.key_2.key_3.key_4.key_6, \
    // v:key_1.key_2.key_3.key_4.key_7 \
    // FROM  ${table_name} ORDER BY v:id"""
    qt_sql """SELECT * from ${table_name} order by cast(v:id as int)"""

    table_name = "complexjson3"
    create_table table_name
    sql """INSERT INTO ${table_name} VALUES (1, '{"key_10":65536,"key_11":"anve","key_0":{"key_1":{"key_2":1025,"key_3":1},"key_4":1,"key_5":256}}')"""
    sql """INSERT INTO ${table_name} VALUES (2, '{"key_0":[{"key_12":"buwvq","key_11":0.0000000255}]}')"""
    // qt_sql """SELECT k, v:key_10, v:key_11, v:key_0.key_1.key_2, v:key_0.key_1.key_3, v:key_0.key_4, v:key_0.key_5, v:key_0.key_12, v:key_0.key_11 FROM complexjson3 ORDER BY k;""" 
    qt_sql """SELECT * from ${table_name} order by k"""

    table_name = "complexjson4"
    create_table table_name
    sql """INSERT INTO ${table_name} VALUES (1, '{
        "id": 1,
        "key_0":[
            {"key_1":{"key_2":[1, 2, 3],"key_8":"sffjx"},"key_10":65535,"key_0":-1},
            {"key_10":10.23,"key_0":922337203.685}
        ]
    }')"""
    // qt_sql """SELECT \
    // v:key_0.key_1.key_2, \
    // v:key_0.key_1.key_8, \
    // v:key_0.key_10, \
    // v:key_0.key_0 \
    // FROM ${table_name} ORDER BY v:id"""
    qt_sql """SELECT * from ${table_name} order by cast(v:id as int)"""

    table_name = "complexjson5"
    create_table table_name
    sql """INSERT INTO ${table_name} VALUES (1, '{
    "id": 1,
    "key_0":[
        {
        "key_1":[
            {
            "key_2":
                {
                    "key_3":[
                        {"key_4":255},
                        {"key_4":65535},
                        {"key_7":255,"key_6":3}
                    ],
                    "key_5":[
                        {"key_7":"nnpqx","key_6":1},
                        {"key_7":255,"key_6":3}
                    ]
                }
            }
        ]
        }
    ]
    }')""" 
    // qt_sql """SELECT \
    // v:key_0.key_1.key_2.key_3.key_4,
    // v:key_0.key_1.key_2.key_3.key_6,
    // v:key_0.key_1.key_2.key_3.key_7,
    // v:key_0.key_1.key_2.key_5.key_6, \
    // v:key_0.key_1.key_2.key_5.key_7
    // FROM ${table_name} ORDER BY v:id"""
    qt_sql """SELECT * from ${table_name} order by cast(v:id as int)"""
}