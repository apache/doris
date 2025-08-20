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

suite("test_string_function_regexp_position") {
    // Drop the existing table
    sql "DROP TABLE IF EXISTS test_regexp_position"

    // Create the test table
    sql """
        CREATE TABLE test_regexp_position (
            str varchar(100),
            pattern varchar(100),
            position int,
            num int          -- this column used to filter
        )
        DISTRIBUTED BY HASH(str)
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    // Insert test data
    sql """
        INSERT INTO test_regexp_position VALUES
        ('I have 23 apples, 5 pears and 13 oranges', '\\\\d', 0, 1),
        ('I have 23 apples, 5 pears and 13 oranges', '\\\\d', 15, 2),
        ('I have 23 apples, 5 pears and 13 oranges', ' ', 0, 3),
        ('I have 23 apples, 5 pears and 13 oranges', ' ', 3, 4),
        ('I have 23 apples, 5 pears and 13 oranges', '\\\\b\\\\d+\\\\b', 0, 5),
        ('I have 23 apples, 5 pears and 13 oranges', '\\\\b\\\\d+\\\\b', 10, 6),
        ('I have 23 apples, 5 pears and 13 oranges', '5', 0, 7),
        ('I have 23 apples, 5 pears and 13 oranges', '5', 25, 8),
        ('我爱北京天安门', '爱', 0, 9),
        ('我爱北京天安门', '爱', 3, 10),
        ('123AbCdExCx', '([[:lower:]]+)C([[:lower:]]+)', 1, 11),
        ('123AbCdExCx', '([[:lower:]]+)C([[:lower:]]+)', 5, 12),
        (null, null, null, 13),
        (null, 'a', null, 14),
        ('abc', 'a', null, 15),
        ('abc', null, 1, 16),
        ('abc', null, null, 17),
        ('这是一段中文 This is a passage in English 1234567', '(\\\\p{Han}+)(.+)', 2, 18),
        ('😀😊😎', '😀|😊|😎', 0, 19);
    """

    // literal, three params
    def res1 = sql """select regexp_position('I have 23 apples, 5 pears and 13 oranges', '\\\\d', 0)"""
    assertEquals(1, res1.size())
    assertEquals(-1, res1.get(0).get(0))
    def res2 = sql """select regexp_position('I have 23 apples, 5 pears and 13 oranges', '\\\\d', 9)"""
    assertEquals(1, res2.size())
    assertEquals(9, res2.get(0).get(0))
    def res3 = sql """select regexp_position('I have 23 apples, 5 pears and 13 oranges', '\\\\d', 100)"""
    assertEquals(1, res3.size())
    assertEquals(-1, res3.get(0).get(0))
    def res4 = sql """select regexp_position('I have 23 apples, 5 pears and 13 oranges', ' ', 0)"""
    assertEquals(1, res4.size())
    assertEquals(-1, res4.get(0).get(0))
    def res5 = sql """select regexp_position('I have 23 apples, 5 pears and 13 oranges', ' ', 9)"""
    assertEquals(1, res5.size())
    assertEquals(10, res5.get(0).get(0))
    def res6 = sql """select regexp_position('I have 23 apples, 5 pears and 13 oranges', ' ', 100)"""
    assertEquals(1, res6.size())
    assertEquals(-1, res6.get(0).get(0))
    def res7 = sql """select regexp_position('I have 23 apples, 5 pears and 13 oranges', '\\\\b\\\\d+\\\\b', 0)"""
    assertEquals(1, res7.size())
    assertEquals(-1, res7.get(0).get(0))
    def res8 = sql """select regexp_position('I have 23 apples, 5 pears and 13 oranges', '\\\\b\\\\d+\\\\b', 9)"""
    assertEquals(1, res8.size())
    assertEquals(9, res8.get(0).get(0))
    def res9 = sql """select regexp_position('I have 23 apples, 5 pears and 13 oranges', '\\\\b\\\\d+\\\\b', 100)"""
    assertEquals(1, res9.size())
    assertEquals(-1, res9.get(0).get(0))
    def res10 = sql """select regexp_position('I have 23 apples, 5 pears and 13 oranges', '5', 0)"""
    assertEquals(1, res10.size())
    assertEquals(-1, res10.get(0).get(0))
    def res11 = sql """select regexp_position('I have 23 apples, 5 pears and 13 oranges', '5', 20)"""
    assertEquals(1, res11.size())
    assertEquals(-1, res11.get(0).get(0))
    def res12 = sql """select regexp_position('我爱北京天安门', '爱', 0)"""
    assertEquals(1, res12.size())
    assertEquals(-1, res12.get(0).get(0))
    def res12_1 = sql """select regexp_position('我爱北京天安门', '爱', 1)"""
    assertEquals(1, res12_1.size())
    assertEquals(4, res12_1.get(0).get(0))
    def res13 = sql """select regexp_position('我爱北京天安门', '爱', 5)"""
    assertEquals(1, res13.size())
    assertEquals(-1, res13.get(0).get(0))
    def res14 = sql """select regexp_position('123AbCdExCx', '([[:lower:]]+)C([[:lower:]]+)', 1)"""
    assertEquals(1, res14.size())
    assertEquals(5, res14.get(0).get(0))
    def res15 = sql """select regexp_position('123AbCdExCx', '([[:lower:]]+)C([[:lower:]]+)', 6)"""
    assertEquals(1, res15.size())
    assertEquals(9, res15.get(0).get(0))
    def res16 = sql """select regexp_position(null, null, null)"""
    assertEquals(1, res16.size())
    assertEquals(null, res16.get(0).get(0))
    def res17 = sql """select regexp_position("null", null, null)"""
    assertEquals(1, res17.size())
    assertEquals(null, res17.get(0).get(0))
    def res18 = sql """select regexp_position(null, "null", null)"""
    assertEquals(1, res18.size())
    assertEquals(null, res18.get(0).get(0))
    def res19 = sql """select regexp_position(null, null, 0)"""
    assertEquals(1, res19.size())
    assertEquals(null, res19.get(0).get(0))
    def res20 = sql """select regexp_position('这是一段中文 This is a passage in English 1234567', '(\\\\p{Han}+)(.+)', 2)"""
    assertEquals(1, res20.size())
    assertEquals(4, res20.get(0).get(0))
    def res21 = sql """select regexp_position('这是一段中文 This is a passage in English 1234567', '(\\\\p{Han}+)(.+)', 4)"""
    assertEquals(1, res21.size())
    assertEquals(4, res21.get(0).get(0))
    def res22 = sql """select regexp_position('😀😊😎', '😀|😊|😎', 0)"""
    assertEquals(1, res22.size())
    assertEquals(-1, res22.get(0).get(0))
    def res23 = sql """select regexp_position('😀😊😎', '😀|😊|😎', 1)"""
    assertEquals(1, res23.size())
    assertEquals(1, res23.get(0).get(0))

    // literal, two params
    def res24 = sql """select regexp_position('I have 23 apples, 5 pears and 13 oranges', '\\\\d')"""
    assertEquals(1, res24.size())
    assertEquals(8, res24.get(0).get(0))
    def res25 = sql """select regexp_position('I have 23 apples, 5 pears and 13 oranges', ' ')"""
    assertEquals(1, res25.size())
    assertEquals(2, res25.get(0).get(0))
    def res26 = sql """select regexp_position('I have 23 apples, 5 pears and 13 oranges', '\\\\b\\\\d+\\\\b')"""
    assertEquals(1, res26.size())
    assertEquals(8, res26.get(0).get(0))
    def res27 = sql """select regexp_position('I have 23 apples, 5 pears and 13 oranges', '5')"""
    assertEquals(1, res27.size())
    assertEquals(19, res27.get(0).get(0))
    def res28 = sql """select regexp_position('我爱北京天安门', '爱')"""
    assertEquals(1, res28.size())
    assertEquals(4, res28.get(0).get(0))
    def res29 = sql """select regexp_position('123AbCdExCx', '([[:lower:]]+)C([[:lower:]]+)')"""
    assertEquals(1, res29.size())
    assertEquals(5, res29.get(0).get(0))
    def res30 = sql """select regexp_position(null, null)"""
    assertEquals(1, res30.size())
    assertEquals(null, res30.get(0).get(0))
    def res31 = sql """select regexp_position("null", null)"""
    assertEquals(1, res31.size())
    assertEquals(null, res31.get(0).get(0))
    def res32 = sql """select regexp_position(null, "null")"""
    assertEquals(1, res32.size())
    assertEquals(null, res32.get(0).get(0))
    def res33 = sql """select regexp_position('这是一段中文 This is a passage in English 1234567', '(\\\\p{Han}+)(.+)')"""
    assertEquals(1, res33.size())
    assertEquals(1, res33.get(0).get(0))
    def res34 = sql """select regexp_position('😀😊😎', '😀|😊|😎')"""
    assertEquals(1, res34.size())
    assertEquals(1, res34.get(0).get(0))

    // table, two params
    def t1 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 1"""
    assertEquals(1, t1.size())
    assertEquals(8, t1.get(0).get(0))
    def t2 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 2"""
    assertEquals(1, t2.size())
    assertEquals(8, t2.get(0).get(0))
    def t3 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 3"""
    assertEquals(1, t3.size())
    assertEquals(2, t3.get(0).get(0))
    def t4 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 4"""
    assertEquals(1, t4.size())
    assertEquals(2, t4.get(0).get(0))
    def t5 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 5"""
    assertEquals(1, t5.size())
    assertEquals(8, t5.get(0).get(0))
    def t6 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 6"""
    assertEquals(1, t6.size())
    assertEquals(8, t6.get(0).get(0))
    def t7 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 7"""
    assertEquals(1, t7.size())
    assertEquals(19, t7.get(0).get(0))
    def t8 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 8"""
    assertEquals(1, t8.size())
    assertEquals(19, t8.get(0).get(0))
    def t9 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 9"""
    assertEquals(1, t9.size())
    assertEquals(4, t9.get(0).get(0))
    def t10 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 10"""
    assertEquals(1, t10.size())
    assertEquals(4, t10.get(0).get(0))
    def t11 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 11"""
    assertEquals(1, t11.size())
    assertEquals(5, t11.get(0).get(0))
    def t12 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 12"""
    assertEquals(1, t12.size())
    assertEquals(5, t12.get(0).get(0))
    def t13 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 13"""
    assertEquals(1, t13.size())
    assertEquals(null, t13.get(0).get(0))
    def t14 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 14"""
    assertEquals(1, t14.size())
    assertEquals(null, t14.get(0).get(0))
    def t15 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 15"""
    assertEquals(1, t15.size())
    assertEquals(1, t15.get(0).get(0))
    def t16 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 16"""
    assertEquals(1, t16.size())
    assertEquals(null, t16.get(0).get(0))
    def t17 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 17"""
    assertEquals(1, t17.size())
    assertEquals(null, t17.get(0).get(0))
    def t18 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 18"""
    assertEquals(1, t18.size())
    assertEquals(1, t18.get(0).get(0))
    def t19 = sql """select regexp_position(str, pattern) from test_regexp_position where num = 19"""
    assertEquals(1, t19.size())
    assertEquals(1, t19.get(0).get(0))

    // table, three params
    def r1 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 1"""
    assertEquals(1, r1.size())
    assertEquals(-1, r1.get(0).get(0))
    def r2 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 2"""
    assertEquals(1, r2.size())
    assertEquals(19, r2.get(0).get(0))
    def r3 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 3"""
    assertEquals(1, r3.size())
    assertEquals(-1, r3.get(0).get(0))
    def r4 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 4"""
    assertEquals(1, r4.size())
    assertEquals(7, r4.get(0).get(0))
    def r5 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 5"""
    assertEquals(1, r5.size())
    assertEquals(-1, r5.get(0).get(0))
    def r6 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 6"""
    assertEquals(1, r6.size())
    assertEquals(19, r6.get(0).get(0))
    def r7 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 7"""
    assertEquals(1, r7.size())
    assertEquals(-1, r7.get(0).get(0))
    def r8 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 8"""
    assertEquals(1, r8.size())
    assertEquals(-1, r8.get(0).get(0))
    def r9 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 9"""
    assertEquals(1, r9.size())
    assertEquals(-1, r9.get(0).get(0))
    def r10 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 10"""
    assertEquals(1, r10.size())
    assertEquals(4, r10.get(0).get(0))
    def r11 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 11"""
    assertEquals(1, r11.size())
    assertEquals(5, r11.get(0).get(0))
    def r12 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 12"""
    assertEquals(1, r12.size())
    assertEquals(5, r12.get(0).get(0))
    def r13 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 13"""
    assertEquals(1, r13.size())
    assertEquals(null, r13.get(0).get(0))
    def r14 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 14"""
    assertEquals(1, r14.size())
    assertEquals(null, r14.get(0).get(0))
    def r15 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 15"""
    assertEquals(1, r15.size())
    assertEquals(null, r15.get(0).get(0))
    def r16 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 16"""
    assertEquals(1, r16.size())
    assertEquals(null, r16.get(0).get(0))
    def r17 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 17"""
    assertEquals(1, r17.size())
    assertEquals(null, r17.get(0).get(0))
    def r18 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 18"""
    assertEquals(1, r18.size())
    assertEquals(4, r18.get(0).get(0))
    def r19 = sql """select regexp_position(str, pattern, position) from test_regexp_position where num = 19"""
    assertEquals(1, r19.size())
    assertEquals(-1, r19.get(0).get(0))
}
