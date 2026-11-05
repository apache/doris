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

suite("dotted_key_escape_score", "p0") {
    sql "set group_commit='off_mode'"
    sql "DROP TABLE IF EXISTS dotted_key_escape_score"
    sql """CREATE TABLE dotted_key_escape_score (
        id INT, v VARIANT<PROPERTIES("variant_escape_dots_in_keys"="true",
            "variant_enable_nested_group"="false")>,
        INDEX idx_v(v) USING INVERTED PROPERTIES("parser"="english","support_phrase"="true"))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true")"""
    sql """INSERT INTO dotted_key_escape_score VALUES
        (1,CAST(CAST('{"a.b":"apple pie","a":{"b":"pear jam"},"a%2Eb":"pear jam","50%":"pear jam"}' AS JSON) AS VARIANT)),
        (2,CAST(CAST('{"a.b":"pear jam","a":{"b":"apple pie"},"a%2Eb":"pear jam","50%":"pear jam"}' AS JSON) AS VARIANT)),
        (3,CAST(CAST('{"a.b":"pear jam","a":{"b":"pear jam"},"a%2Eb":"apple pie","50%":"pear jam"}' AS JSON) AS VARIANT)),
        (4,CAST(CAST('{"a.b":"pear jam","a":{"b":"pear jam"},"a%2Eb":"pear jam","50%":"apple pie"}' AS JSON) AS VARIANT))"""
    // Different matching ids detect using a display-name alias for another physical path.
    qt_literal """SELECT id,CAST(score()>0 AS INT) FROM dotted_key_escape_score
        WHERE v['a.b'] MATCH_ANY 'apple' ORDER BY id"""
    qt_nested """SELECT id,CAST(score()>0 AS INT) FROM dotted_key_escape_score
        WHERE v['a']['b'] MATCH_ANY 'apple' ORDER BY id"""
    qt_percent_escape """SELECT id,CAST(score()>0 AS INT) FROM dotted_key_escape_score
        WHERE v['a%2Eb'] MATCH_ANY 'apple' ORDER BY id"""
    qt_percent """SELECT id,CAST(score()>0 AS INT) FROM dotted_key_escape_score
        WHERE v['50%'] MATCH_ANY 'apple' ORDER BY id"""
}
