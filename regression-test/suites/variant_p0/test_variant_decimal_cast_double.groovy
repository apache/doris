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

// A DECIMAL path of a VARIANT schema template makes the element decimal-backed; CAST(element AS DOUBLE) goes
// through the generic decimal -> double cast and must equal the correctly rounded value of the decimal's digits,
// i.e. the text route CAST(CAST(element AS STRING) AS DOUBLE), for unscaled values beyond 2^53.
suite("test_variant_decimal_cast_double", "p0") {
    def tbl = "test_variant_decimal_cast_double"
    sql "DROP TABLE IF EXISTS ${tbl}"
    sql """
        CREATE TABLE ${tbl} (
            id INT,
            v VARIANT<MATCH_NAME 'p': DECIMAL(38,12)>
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // the string form is the exact route onto a DECIMAL template path
    sql """
        INSERT INTO ${tbl} VALUES
            (1, '{"p":"29201.024999999998"}'),
            (2, '{"p":"-29201.024999999998"}'),
            (3, '{"p":"21051.524999999998"}'),
            (4, '{"p":"3763.955000000000"}'),
            (5, '{"p":"0.000000000001"}')
    """
    def rows = sql """
        SELECT id, CAST(v['p'] AS DOUBLE), CAST(CAST(v['p'] AS STRING) AS DOUBLE), CAST(v['p'] AS STRING)
        FROM ${tbl} ORDER BY id
    """
    assertEquals(5, rows.size())
    for (row in rows) {
        def direct = row[1] as Double
        def viaText = row[2] as Double
        def text = row[3] as String
        assertEquals(viaText, direct, 0.0d)
        assertEquals(Double.parseDouble(text), direct, 0.0d)
    }
    sql "DROP TABLE IF EXISTS ${tbl}"
}
