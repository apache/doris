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

// Regression test for https://github.com/apache/doris/issues/59691
// Block comments /* ... */ must NOT nest (MySQL/standard SQL semantics): a `/*`
// is closed by the first following `*/`. Previously the grammar treated them as
// nestable, so the comment swallowed the trailing predicate and returned wrong rows.
suite("test_block_comment_no_nesting") {
    sql "drop table if exists test_block_comment_59691"
    sql """
        create table test_block_comment_59691 (
            id INT,
            name varchar(32)
        )
        UNIQUE KEY (id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    sql """insert into test_block_comment_59691 values (1, 'Alice'), (2, 'Bob'), (3, 'Camille');"""

    // The first `/*` closes at the first `*/` (inside `/* */`), so `and id = 3`
    // stays active and `-- */` is a trailing line comment. Expected: only row 3.
    def r1 = sql """
        select id from test_block_comment_59691
          where 1 = 1
            /* and id = 2
            /* */
            and id = 3
            -- */
        order by id
    """
    assertEquals(1, r1.size())
    assertEquals(3, r1[0][0])

    // Second form from the issue: `/* ... /*/` also closes at the first `*/`.
    def r2 = sql """
        select id from test_block_comment_59691
          where 1 = 1
            /* and id = 2 /*/
            and id = 3
        order by id
    """
    assertEquals(1, r2.size())
    assertEquals(3, r2[0][0])

    // Plain (non-nested) block comment still works as before.
    def r3 = sql """
        select id from test_block_comment_59691
          where id = 2 /* a plain comment */ or id = 3
        order by id
    """
    assertEquals(2, r3.size())

    sql "drop table if exists test_block_comment_59691"
}
