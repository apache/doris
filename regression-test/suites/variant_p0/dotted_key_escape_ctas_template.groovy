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

suite("dotted_key_escape_ctas_template", "p0") {
    sql "DROP TABLE IF EXISTS dotted_ctas_template_legacy"
    sql """CREATE TABLE dotted_ctas_template_legacy(k INT,
        v VARIANT<MATCH_NAME '50%':STRING, MATCH_NAME 'a%2Eb':STRING,
            PROPERTIES("variant_enable_nested_group"="false")>)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO dotted_ctas_template_legacy VALUES
        (1, parse_to_variant('{"50%":"percent","a%2Eb":"literal_percent","a":{"b":"nested"}}'))"""
    // Even an INT-only escaped source makes the inferred destination escaped.
    sql "DROP TABLE IF EXISTS dotted_ctas_template_int"
    sql """CREATE TABLE dotted_ctas_template_int(k INT) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1","variant_escape_dots_in_keys"="true")"""
    sql "INSERT INTO dotted_ctas_template_int VALUES (1)"
    for (def policy : ["inherited", "true", "false"]) {
        def explicit = policy == "inherited" ? "" : ", 'variant_escape_dots_in_keys'='${policy}'"
        sql "DROP TABLE IF EXISTS dotted_ctas_template_${policy}"
        sql """CREATE TABLE dotted_ctas_template_${policy} DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES("replication_num"="1" ${explicit}) AS SELECT l.k,l.v
            FROM dotted_ctas_template_legacy l JOIN dotted_ctas_template_int e ON l.k=e.k"""
        // Exercise a later write as well as the initial CTAS write.
        sql """INSERT INTO dotted_ctas_template_${policy}
            SELECT k+1,v FROM dotted_ctas_template_legacy"""
        quickTest(policy, """SELECT k,v['50%'],v['a%2Eb'],v['a']['b']
            FROM dotted_ctas_template_${policy} ORDER BY k""")
    }
    sql "DROP TABLE IF EXISTS dotted_ctas_template_escaped"
    sql """CREATE TABLE dotted_ctas_template_escaped(k INT,
        v VARIANT<MATCH_NAME '50%25':STRING, MATCH_NAME 'a%2Eb':STRING,
            PROPERTIES("variant_enable_nested_group"="false")>)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1","variant_escape_dots_in_keys"="true")"""
    sql """INSERT INTO dotted_ctas_template_escaped VALUES
        (1, parse_to_variant('{"50%":"percent","a.b":"literal_dot","a%2Eb":"literal_percent"}'))"""
    sql "DROP TABLE IF EXISTS dotted_ctas_template_to_legacy"
    sql """CREATE TABLE dotted_ctas_template_to_legacy DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1","variant_escape_dots_in_keys"="false")
        AS SELECT k,v FROM dotted_ctas_template_escaped"""
    qt_to_legacy """SELECT k,v['50%'],v['a%2Eb'],v['a']['b']
        FROM dotted_ctas_template_to_legacy ORDER BY k"""
    qt_source """SELECT k,v['50%'],v['a.b'],v['a%2Eb']
        FROM dotted_ctas_template_escaped ORDER BY k"""
}
