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

suite("test_if_json") {
    def tblName = "test_if_json"
    sql """DROP TABLE IF EXISTS ${tblName};"""
    sql """
        CREATE TABLE ${tblName} (
            `id` INT NULL,
            `v_key` CHAR NULL,
            `v_json` JSON NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `v_key`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "replication_num" = "1"
        )
    """

    sql """ insert into ${tblName} values (1, 'a', '{"k":"k1","v":"v1"}')"""
    sql """ insert into ${tblName} values (2, 'a', '{"k":"k2","v":"v3"}')"""
    sql """ insert into ${tblName} values (3, 'b', '{"k":"k3","v":"v3"}')"""
    sql """ insert into ${tblName} values (4, 'a', '{"k":"k4","v":"v4"}')"""
    sql """ insert into ${tblName} values (5, 'c', '{"k":"k5","v":"v5"}')"""

    qt_if_json_parse """
    SELECT id,
        if(v_key = 'a', JSON_PARSE(v_json), NULL) AS a,
        if(v_key = 'b', NULL, JSON_PARSE(v_json)) AS b
    FROM ${tblName}
    ORDER BY id
    """

    qt_case_json_parse """
    SELECT id,
        CASE v_key WHEN 'a' THEN JSON_PARSE(v_json) ELSE NULL END AS a,
        CASE v_key WHEN 'b' THEN NULL ELSE JSON_PARSE(v_json) END AS b
    FROM ${tblName}
    ORDER BY id
    """

    qt_case_json_parse_error_to_null """ 
    SELECT id,
        CASE v_key WHEN 'a' THEN json_parse_error_to_null(v_json) ELSE NULL END AS a,
        CASE v_key WHEN 'b' THEN NULL ELSE json_parse_error_to_null(v_json) END AS b
    FROM ${tblName}
    ORDER BY id
    """

    qt_case_json_parse_error_to_value """ 
    SELECT id,
        CASE v_key WHEN 'a' THEN json_parse_error_to_value(v_json,json_object()) ELSE NULL END AS a,
        CASE v_key WHEN 'b' THEN NULL ELSE json_parse_error_to_value(v_json,json_object()) END AS b
    FROM ${tblName}
    ORDER BY id
    """

    sql """DROP TABLE IF EXISTS ${tblName};"""
}
