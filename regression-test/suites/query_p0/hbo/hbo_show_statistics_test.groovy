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

suite("hbo_show_statistics_test", "nonConcurrent") {
    // HBO SHOW [PINNED|LEARNED] STATISTICS [LIKE '<pattern>'] lists the hbo entries recorded by
    // this FE. Assertions are used instead of qt_* because the pinned Detail column carries the
    // entry creation time, which is not reproducible across runs; every query below is filtered
    // by an exact fingerprint so the result set is deterministic in size and content.
    def fingerprint = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    def structCanonical = "S{internal.hbo_test.hbo_si_r#0,v2}"
    try {
        sql """ HBO SET STATISTICS '${fingerprint}' = 123456 STRUCT '${structCanonical}'; """

        // pinned scope: exactly the injected entry
        def pinnedRows = sql """ HBO SHOW PINNED STATISTICS LIKE '${fingerprint}'; """
        assertEquals(1, pinnedRows.size())
        assertEquals("pinned", pinnedRows[0][0].toString())
        assertEquals(fingerprint, pinnedRows[0][1].toString())
        assertEquals("123456", pinnedRows[0][3].toString())
        assertEquals(structCanonical, pinnedRows[0][4].toString())

        // default scope covers pinned + learned; the injected fingerprint is never a learned key
        def allRows = sql """ HBO SHOW STATISTICS LIKE '${fingerprint}'; """
        assertEquals(1, allRows.size())
        assertEquals("pinned", allRows[0][0].toString())
        assertEquals(fingerprint, allRows[0][1].toString())

        // learned scope: nothing recorded for an injected-only fingerprint
        def learnedRows = sql """ HBO SHOW LEARNED STATISTICS LIKE '${fingerprint}'; """
        assertTrue(learnedRows.isEmpty(), learnedRows.toString())

        // LIKE prefix form
        def prefixRows = sql """ HBO SHOW PINNED STATISTICS LIKE '${fingerprint.substring(0, 8)}%'; """
        assertEquals(1, prefixRows.size())
        assertEquals(fingerprint, prefixRows[0][1].toString())

        // a non-matching pattern filters everything out
        def noMatchRows = sql """ HBO SHOW PINNED STATISTICS LIKE 'ffffffff%'; """
        assertTrue(noMatchRows.isEmpty(), noMatchRows.toString())
    } finally {
        sql """ HBO DELETE STATISTICS '${fingerprint}'; """
    }

    def afterDeleteRows = sql """ HBO SHOW PINNED STATISTICS LIKE '${fingerprint}'; """
    assertTrue(afterDeleteRows.isEmpty(), afterDeleteRows.toString())

    // an unknown scope is rejected with a clear error
    test {
        sql """ HBO SHOW BOGUS STATISTICS; """
        exception "invalid hbo show scope, expect PINNED or LEARNED: bogus"
    }
}
