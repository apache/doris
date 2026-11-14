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
// neighbour. A fixed 16 significant digits went both ways: 0.30000000000000004 rendered as 0.3 and
// 1997.6000000000001 as 1997.6 (too short), 8.12 as 8.119999999999999 (too long).
suite("test_variant_double_text_roundtrip", "p0") {
    sql "DROP TABLE IF EXISTS test_variant_double_text_roundtrip"
    sql """
        CREATE TABLE test_variant_double_text_roundtrip (id INT, v VARIANT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_variant_double_text_roundtrip VALUES
            (1, '{"s":0.30000000000000004,"q":1997.6000000000001,"r":1997.6,"t":0.1,"u":8.12,"w":123456789012345680.0,"x":-2.5e-7}')
    """
    // the document text is deterministic (keys byte-sorted), so the whole text is pinned: a longer or a
    // shorter rendering of any value fails here
    def rows = sql """
        SELECT CAST(v AS STRING), CAST(v['s'] AS STRING), CAST(v['q'] AS STRING), CAST(v['u'] AS STRING)
        FROM test_variant_double_text_roundtrip
    """
    assertEquals('{"q":1997.6000000000001,"r":1997.6,"s":0.30000000000000004,"t":0.1,"u":8.12,"w":1.2345678901234568e+17,"x":-2.5e-07}',
            rows[0][0] as String)
    assertEquals("0.30000000000000004", rows[0][1] as String)
    assertEquals("1997.6000000000001", rows[0][2] as String)
    assertEquals("8.12", rows[0][3] as String)
    // parsed back from the document text, each number is the stored double (delta 0: the framework's
    // tolerance for DOUBLE cells cannot tell 1997.6 from 1997.6000000000001)
    def back = sql """
        SELECT json_extract_double(CAST(v AS STRING), '\$.s'), CAST(v['s'] AS DOUBLE),
               json_extract_double(CAST(v AS STRING), '\$.q'), CAST(v['q'] AS DOUBLE),
               json_extract_double(CAST(v AS STRING), '\$.r'), CAST(v['r'] AS DOUBLE)
        FROM test_variant_double_text_roundtrip
    """
    assertEquals(back[0][1] as Double, back[0][0] as Double, 0.0d)
    assertEquals(back[0][3] as Double, back[0][2] as Double, 0.0d)
    assertEquals(back[0][5] as Double, back[0][4] as Double, 0.0d)
    // q and r, two different doubles, stay distinct through the text
    assertTrue((back[0][2] as Double) != (back[0][4] as Double))
}
