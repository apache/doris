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

// The JSON text of a VARIANT document renders every double in its shortest round-trip form: the same text the
// element's own cast produces, and a number parsed back from the document text is the stored double, not its
// neighbour (0.30000000000000004 used to render as 0.3, 1997.6000000000001 as 1997.6).
suite("test_variant_double_text_roundtrip", "p0") {
    def tbl = "test_variant_double_text_roundtrip"
    sql "DROP TABLE IF EXISTS ${tbl}"
    sql """
        CREATE TABLE ${tbl} (id INT, v VARIANT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO ${tbl} VALUES
            (1, '{"s":0.30000000000000004,"q":1997.6000000000001,"r":1997.6,"t":0.1,"w":123456789012345680.0,"x":-2.5e-7}')
    """
    def rows = sql """SELECT CAST(v AS STRING), CAST(v['s'] AS STRING), CAST(v['q'] AS STRING) FROM ${tbl}"""
    def doc = rows[0][0] as String
    assertTrue(doc.contains('"s":0.30000000000000004'), doc)
    assertTrue(doc.contains('"q":1997.6000000000001'), doc)
    assertTrue(doc.contains('"w":1.2345678901234568e+17'), doc)
    assertTrue(doc.contains('"x":-2.5e-07'), doc)
    assertEquals("0.30000000000000004", rows[0][1] as String)
    assertEquals("1997.6000000000001", rows[0][2] as String)
    // parsed back from the document text, each number is the stored double
    def back = sql """
        SELECT json_extract_double(CAST(v AS STRING), '\$.s'), CAST(v['s'] AS DOUBLE),
               json_extract_double(CAST(v AS STRING), '\$.q'), CAST(v['q'] AS DOUBLE),
               json_extract_double(CAST(v AS STRING), '\$.r'), CAST(v['r'] AS DOUBLE)
        FROM ${tbl}
    """
    assertEquals(back[0][1] as Double, back[0][0] as Double, 0.0d)
    assertEquals(back[0][3] as Double, back[0][2] as Double, 0.0d)
    assertEquals(back[0][5] as Double, back[0][4] as Double, 0.0d)
    assertTrue((back[0][1] as Double) != (back[0][5] as Double))
    sql "DROP TABLE IF EXISTS ${tbl}"
}
