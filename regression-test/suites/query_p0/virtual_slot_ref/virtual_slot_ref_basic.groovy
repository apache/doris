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

suite("virtual_slot_ref_basic") {
    sql "drop table if exists virtual_slot_ref_basic;"
    sql """
    CREATE TABLE `virtual_slot_ref_basic` (
        `id` int NULL,
        `val` smallint NULL
        ) ENGINE=OLAP
    DUPLICATE KEY(`id`, `val`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 10
    PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    sql """
        INSERT INTO virtual_slot_ref_basic VALUES
        (1, 10),
        (2, 20),
        (3, 30),
        (4, 40),
        (5, 50);
    """

    def sql0 = "SELECT id + val FROM virtual_slot_ref_basic WHERE id + val > 30 ORDER BY id + val;"
    def result0 = sql """explain verbose ${sql0}"""
    result0 = result0.join("\n")
    /*
    [Tuples:]
    [TupleDescriptor{id=0, tbl=virtual_slot_ref_basic}]
    [  SlotDescriptor{id=0, col=id, colUniqueId=0, type=int, nullable=true, isAutoIncrement=false, subColPath=null, virtualColumn=null}]
    [  SlotDescriptor{id=1, col=val, colUniqueId=1, type=smallint, nullable=true, isAutoIncrement=false, subColPath=null, virtualColumn=null}]
    [  SlotDescriptor{id=2, col=__DORIS_VIRTUAL_COL__1, colUniqueId=2147483646, type=bigint, nullable=true, isAutoIncrement=false, subColPath=null, virtualColumn=(id[#0] + val[#1])}]
    */
    assertTrue(result0.contains("(cast(id as BIGINT) + cast(val as BIGINT))[#2]"));
    assertTrue(result0.contains("__DORIS_VIRTUAL_COL__"));
    assertTrue(result0.contains("virtualColumn=(CAST(id[#0] AS bigint) + CAST(val[#1] AS bigint)"));
    qt_0 """${sql0}"""

    // Make sure topn global lazy materialization works
    qt_1 """SELECT * FROM virtual_slot_ref_basic WHERE id > 0 ORDER BY id LIMIT 10;"""


    // One conjunct has more than one virtual column
    def sql2 = "SELECT id + val FROM virtual_slot_ref_basic WHERE (id + val) > 10 OR (id + val) < 30 ORDER BY (id + val);"
    def result2 = sql """explain verbose ${sql2}"""
    result2 = result2.join("\n")
    /*
    [Tuples:]
    [TupleDescriptor{id=0, tbl=virtual_slot_ref_basic}]
    [  SlotDescriptor{id=0, col=id, colUniqueId=0, type=int, nullable=true, isAutoIncrement=false, subColPath=null, virtualColumn=null}]
    [  SlotDescriptor{id=1, col=val, colUniqueId=1, type=smallint, nullable=true, isAutoIncrement=false, subColPath=null, virtualColumn=null}]
    [  SlotDescriptor{id=2, col=__DORIS_VIRTUAL_COL__1, colUniqueId=2147483646, type=bigint, nullable=true, isAutoIncrement=false, subColPath=null, virtualColumn=(id[#0] + val[#1])}]
    */
    assertTrue(result2.contains("(cast(id as BIGINT) + cast(val as BIGINT))[#2]"));
    assertTrue(result2.contains("__DORIS_VIRTUAL_COL__"));
    assertTrue(result2.contains("virtualColumn=(CAST(id[#0] AS bigint) + CAST(val[#1] AS bigint)"));
    qt_2 """SELECT id + val FROM virtual_slot_ref_basic WHERE (id + val) > 10 OR (id + val) < 30 ORDER BY (id + val);"""

    // Virtual column exists in different conditions
    def sql3 = "SELECT id + val FROM virtual_slot_ref_basic WHERE (id + val) > 10 AND (id + val) < 30 ORDER BY (id + val);"
    def result3 = sql """explain verbose ${sql3}"""
    result3 = result3.join("\n")
    /*
    [Tuples:]
    [TupleDescriptor{id=0, tbl=virtual_slot_ref_basic}]
    [  SlotDescriptor{id=0, col=id, colUniqueId=0, type=int, nullable=true, isAutoIncrement=false, subColPath=null, virtualColumn=null}]
    [  SlotDescriptor{id=1, col=val, colUniqueId=1, type=smallint, nullable=true, isAutoIncrement=false, subColPath=null, virtualColumn=null}]
    [  SlotDescriptor{id=2, col=__DORIS_VIRTUAL_COL__1, colUniqueId=2147483646, type=bigint, nullable=true, isAutoIncrement=false, subColPath=null, virtualColumn=(id[#0] + val[#1])}]
    */
    assertTrue(result3.contains("(cast(id as BIGINT) + cast(val as BIGINT))[#2]"));
    assertTrue(result3.contains("__DORIS_VIRTUAL_COL__"));
    assertTrue(result3.contains("virtualColumn=(CAST(id[#0] AS bigint) + CAST(val[#1] AS bigint)"));
    qt_3 """SELECT id + val FROM virtual_slot_ref_basic WHERE (id + val) > 10 AND (id + val) < 30 ORDER BY (id + val);"""

    // Nested virtual column
    def sql4 = "SELECT id + val FROM virtual_slot_ref_basic WHERE (id + val) > 10 AND (id + val) < 30 ORDER BY (id + val);"
    def result4 = sql """explain verbose ${sql4}"""
    result4 = result4.join("\n")
    /*
    [Tuples:]
    [TupleDescriptor{id=0, tbl=virtual_slot_ref_basic}]
    [  SlotDescriptor{id=0, col=id, colUniqueId=0, type=int, nullable=true, isAutoIncrement=false, subColPath=null, virtualColumn=null}]
    [  SlotDescriptor{id=1, col=val, colUniqueId=1, type=smallint, nullable=true, isAutoIncrement=false, subColPath=null, virtualColumn=null}]
    [  SlotDescriptor{id=2, col=__DORIS_VIRTUAL_COL__1, colUniqueId=2147483646, type=bigint, nullable=true, isAutoIncrement=false, subColPath=null, virtualColumn=(id[#0] + val[#1])}]
    */
    assertTrue(result4.contains("(cast(id as BIGINT) + cast(val as BIGINT))[#2]"));
    assertTrue(result4.contains("__DORIS_VIRTUAL_COL__"));
    assertTrue(result4.contains("virtualColumn=(CAST(id[#0] AS bigint) + CAST(val[#1] AS bigint)"));
    qt_4 """SELECT id + val FROM virtual_slot_ref_basic WHERE round((id + val), 0) > 30 ORDER BY (id + val)"""

    // Test case 5: Multiple different virtual columns in WHERE clause
    def sql5 = "SELECT id + val, id * val FROM virtual_slot_ref_basic WHERE (id + val) > 20 AND (id * val) < 100 ORDER BY (id + val);"
    def result5 = sql """explain verbose ${sql5}"""
    result5 = result5.join("\n")
    // Should have two virtual columns: (id + val) and (id * val)
    assertTrue(result5.contains("__DORIS_VIRTUAL_COL__"));
    assertTrue(result5.contains("virtualColumn=(CAST(id[#0] AS bigint) + CAST(val[#1] AS bigint)"));
    assertTrue(result5.contains("virtualColumn=(CAST(id[#0] AS bigint) * CAST(val[#1] AS bigint)"));
    qt_5 """SELECT id + val, id * val FROM virtual_slot_ref_basic WHERE (id + val) > 20 AND (id * val) < 100 ORDER BY (id + val);"""

    // Test case 6: Virtual column in both WHERE and SELECT with repeated expression
    def sql6 = "SELECT id + val, id - val FROM virtual_slot_ref_basic WHERE (id + val) > 10 AND (id - val) > 0 ORDER BY (id + val), (id - val);"
    def result6 = sql """explain verbose ${sql6}"""
    result6 = result6.join("\n")
    // Should have virtual columns for both expressions
    assertTrue(result6.contains("__DORIS_VIRTUAL_COL__"));
    assertTrue(result6.contains("virtualColumn=(CAST(id[#0] AS bigint) + CAST(val[#1] AS bigint)"));
    assertTrue(result6.contains("virtualColumn=(CAST(id[#0] AS bigint) - CAST(val[#1] AS bigint)"));
    qt_6 """SELECT id + val, id - val FROM virtual_slot_ref_basic WHERE (id + val) > 10 AND (id - val) > 0 ORDER BY (id + val), (id - val);"""

    // Test case 7: Complex expression with function calls
    def sql7 = "SELECT abs(id - val) FROM virtual_slot_ref_basic WHERE abs(id - val) > 5 ORDER BY abs(id - val);"
    def result7 = sql """explain verbose ${sql7}"""
    result7 = result7.join("\n")
    // Should have virtual column for abs(id - val)
    assertTrue(result7.contains("__DORIS_VIRTUAL_COL__"));
    assertTrue(result7.contains("(abs((cast(id as BIGINT) - cast(val as BIGINT)))[#2] > 5)"));
    qt_7 """SELECT abs(id - val) FROM virtual_slot_ref_basic WHERE abs(id - val) > 5 ORDER BY abs(id - val);"""

    // Test case 8: Virtual column with nested expressions
    def sql8 = "SELECT (id + val) * 2 FROM virtual_slot_ref_basic WHERE (id + val) * 2 > 60 ORDER BY (id + val) * 2;"
    def result8 = sql """explain verbose ${sql8}"""
    result8 = result8.join("\n")
    // Should have virtual column for (id + val) and potentially ((id + val) * 2)
    assertTrue(result8.contains("__DORIS_VIRTUAL_COL__"));
    qt_8 """SELECT (id + val) * 2 FROM virtual_slot_ref_basic WHERE (id + val) * 2 > 60 ORDER BY (id + val) * 2;"""

    // Test case 9: Multiple occurrences of same expression in different parts of query
    def sql9 = "SELECT id + val, (id + val) * 2 FROM virtual_slot_ref_basic WHERE (id + val) > 15 AND (id + val) < 50 ORDER BY (id + val);"
    def result9 = sql """explain verbose ${sql9}"""
    result9 = result9.join("\n")
    // Should have virtual column for (id + val) which appears multiple times
    assertTrue(result9.contains("__DORIS_VIRTUAL_COL__"));
    assertTrue(result9.contains("virtualColumn=(CAST(id[#0] AS bigint) + CAST(val[#1] AS bigint)"));
    qt_9 """SELECT id + val, (id + val) * 2 FROM virtual_slot_ref_basic WHERE (id + val) > 15 AND (id + val) < 50 ORDER BY (id + val);"""

    // Test case 10: Virtual column with CASE WHEN expression
    def sql10 = "SELECT CASE WHEN id + val > 30 THEN 'HIGH' ELSE 'LOW' END FROM virtual_slot_ref_basic WHERE id + val > 0 ORDER BY id + val;"
    def result10 = sql """explain verbose ${sql10}"""
    result10 = result10.join("\n")
    // Should have virtual column for (id + val)
    assertTrue(result10.contains("__DORIS_VIRTUAL_COL__"));
    qt_10 """SELECT CASE WHEN id + val > 30 THEN 'HIGH' ELSE 'LOW' END FROM virtual_slot_ref_basic WHERE id + val > 0 ORDER BY id + val;"""

    // Test case 11: Virtual column with IN clause
    def sql11 = "SELECT id + val FROM virtual_slot_ref_basic WHERE (id + val) IN (11, 22, 33, 44) ORDER BY (id + val);"
    def result11 = sql """explain verbose ${sql11}"""
    result11 = result11.join("\n")
    // Should have virtual column for (id + val)
    assertTrue(result11.contains("__DORIS_VIRTUAL_COL__"));
    assertTrue(result11.contains("virtualColumn=(CAST(id[#0] AS bigint) + CAST(val[#1] AS bigint)"));
    qt_11 """SELECT id + val FROM virtual_slot_ref_basic WHERE (id + val) IN (11, 22, 33, 44) ORDER BY (id + val);"""

    // Test case 12: Virtual column with BETWEEN clause
    def sql12 = "SELECT id + val FROM virtual_slot_ref_basic WHERE (id + val) BETWEEN 20 AND 40 ORDER BY (id + val);"
    def result12 = sql """explain verbose ${sql12}"""
    result12 = result12.join("\n")
    // Should have virtual column for (id + val)
    assertTrue(result12.contains("__DORIS_VIRTUAL_COL__"));
    assertTrue(result12.contains("virtualColumn=(CAST(id[#0] AS bigint) + CAST(val[#1] AS bigint)"));
    qt_12 """SELECT id + val FROM virtual_slot_ref_basic WHERE (id + val) BETWEEN 20 AND 40 ORDER BY (id + val);"""

    // Test case 13: Virtual column with GROUP BY
    def sql13 = "SELECT id + val, COUNT(*) FROM virtual_slot_ref_basic WHERE (id + val) > 10 GROUP BY (id + val) ORDER BY (id + val);"
    def result13 = sql """explain verbose ${sql13}"""
    result13 = result13.join("\n")
    // Should have virtual column for (id + val)
    assertTrue(result13.contains("__DORIS_VIRTUAL_COL__"));
    assertTrue(result13.contains("virtualColumn=(CAST(id[#0] AS bigint) + CAST(val[#1] AS bigint)"));
    qt_13 """SELECT id + val, COUNT(*) FROM virtual_slot_ref_basic WHERE (id + val) > 10 GROUP BY (id + val) ORDER BY (id + val);"""

    // Test case 14: Virtual column with HAVING clause
    def sql14 = "SELECT id + val, COUNT(*) FROM virtual_slot_ref_basic GROUP BY (id + val) HAVING (id + val) > 20 ORDER BY (id + val);"
    def result14 = sql """explain verbose ${sql14}"""
    result14 = result14.join("\n")
    // Should have virtual column for (id + val)
    assertTrue(result14.contains("__DORIS_VIRTUAL_COL__"));
    qt_14 """SELECT id + val, COUNT(*) FROM virtual_slot_ref_basic GROUP BY (id + val) HAVING (id + val) > 20 ORDER BY (id + val);"""

    // Test case 15: Multiple virtual columns with different complexity
    def sql15 = "SELECT id + val, id * val, id / val FROM virtual_slot_ref_basic WHERE (id + val) > 20 AND (id * val) < 200 AND (id / val) > 1 ORDER BY (id + val);"
    def result15 = sql """explain verbose ${sql15}"""
    result15 = result15.join("\n")
    // Should have virtual columns for all three expressions
    assertTrue(result15.contains("__DORIS_VIRTUAL_COL__"));
    qt_15 """SELECT id + val, id * val, id / val FROM virtual_slot_ref_basic WHERE (id + val) > 20 AND (id * val) < 200 AND (id / val) > 1 ORDER BY (id + val);"""

    // Test case 16: Virtual column with string functions (if supported)
    def sql16 = "SELECT CONCAT(CAST(id AS STRING), '_', CAST(val AS STRING)) FROM virtual_slot_ref_basic WHERE LENGTH(CONCAT(CAST(id AS STRING), '_', CAST(val AS STRING))) > 3 ORDER BY id;"
    def result16 = sql """explain verbose ${sql16}"""
    result16 = result16.join("\n")
    // Should have virtual column for the CONCAT expression
    assertTrue(result16.contains("__DORIS_VIRTUAL_COL__"));
    qt_16 """SELECT CONCAT(CAST(id AS STRING), '_', CAST(val AS STRING)) FROM virtual_slot_ref_basic WHERE LENGTH(CONCAT(CAST(id AS STRING), '_', CAST(val AS STRING))) > 3 ORDER BY id;"""

    // Test case 17: Virtual column with arithmetic and comparison in subquery
    def sql17 = "SELECT * FROM (SELECT id + val as sum_val FROM virtual_slot_ref_basic WHERE (id + val) > 15) t WHERE sum_val < 45 ORDER BY sum_val;"
    def result17 = sql """explain verbose ${sql17}"""
    result17 = result17.join("\n")
    qt_17 """SELECT * FROM (SELECT id + val as sum_val FROM virtual_slot_ref_basic WHERE (id + val) > 15) t WHERE sum_val < 45 ORDER BY sum_val;"""

    // Test case 18: Virtual column with NOT operator
    def sql18 = "SELECT id + val FROM virtual_slot_ref_basic WHERE NOT ((id + val) <= 20) ORDER BY (id + val);"
    def result18 = sql """explain verbose ${sql18}"""
    result18 = result18.join("\n")
    // Should have virtual column for (id + val)
    assertTrue(result18.contains("__DORIS_VIRTUAL_COL__"));
    assertTrue(result18.contains("virtualColumn=(CAST(id[#0] AS bigint) + CAST(val[#1] AS bigint)"));
    qt_18 """SELECT id + val FROM virtual_slot_ref_basic WHERE NOT ((id + val) <= 20) ORDER BY (id + val);"""

    // Test case 19: Virtual column with IS NULL check
    def sql19 = "SELECT id + val FROM virtual_slot_ref_basic WHERE (id + val) IS NOT NULL ORDER BY (id + val);"
    def result19 = sql """explain verbose ${sql19}"""
    result19 = result19.join("\n")
    // Should have virtual column for (id + val)
    assertTrue(result19.contains("__DORIS_VIRTUAL_COL__"));
    assertTrue(result19.contains("virtualColumn=(CAST(id[#0] AS bigint) + CAST(val[#1] AS bigint)"));
    qt_19 """SELECT id + val FROM virtual_slot_ref_basic WHERE (id + val) IS NOT NULL ORDER BY (id + val);"""

    // Test case 20: Virtual column with LIMIT
    def sql20 = "SELECT id + val FROM virtual_slot_ref_basic WHERE (id + val) > 10 ORDER BY (id + val) LIMIT 3;"
    def result20 = sql """explain verbose ${sql20}"""
    result20 = result20.join("\n")
    // Should have virtual column for (id + val)
    assertTrue(result20.contains("__DORIS_VIRTUAL_COL__"));
    assertTrue(result20.contains("virtualColumn=(CAST(id[#0] AS bigint) + CAST(val[#1] AS bigint)"));
    qt_20 """SELECT id + val FROM virtual_slot_ref_basic WHERE (id + val) > 10 ORDER BY (id + val) LIMIT 3;"""

}