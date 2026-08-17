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

import java.util.regex.Pattern

// E2E proof that the token-exists Bloom Filter absent-term fast path is gated SOLELY by the
// BE config `enable_inverted_index_term_bf` and NOT by the per-index `token_bloom_filter`
// property:
//   - config on  + property absent      -> writer emits tbf, reader engages fast path
//                                          (InvertedIndexTermBfSkippedLookups > 0 on absent term)
//   - config off + property=true        -> writer skips tbf, reader bypasses fast path
//                                          (InvertedIndexTermBfSkippedLookups stays 0)
//
// Profile-counter assertion uses `set profile_level = 1` (the skipped-lookups counter is
// surfaced at L1 because it is the headline user-facing value of the feature).
suite('test_inverted_index_term_bf_be_config_gate', 'p0') {
    def fetchBackendIds = { ->
        def out = sql_return_maparray """ SHOW BACKENDS """
        return out.findAll { it.Alive.toString().equalsIgnoreCase('true') }
                  .collect { [
                          ip: it.Host.toString(),
                          httpPort: it.HttpPort.toString(),
                  ] }
    }

    def backends = fetchBackendIds()
    assertTrue(!backends.isEmpty(), 'no alive BE backends found for this test')

    def setBeConfig = { String key, String value ->
        backends.each { be ->
            def (code, out, err) = update_be_config(be.ip, be.httpPort, key, value)
            log.info('update_be_config ' + key + '=' + value + ' on ' + be.ip + ':' + be.httpPort
                    + ' code=' + code + ' out=' + out + ' err=' + err)
            assertEquals(0, code)
        }
    }

    def savedConfig = null
    backends.each { be ->
        def (code, out, err) = show_be_config(be.ip, be.httpPort)
        assertEquals(0, code)
        def configList = parseJson(out.trim())
        configList.each {
            if (it instanceof List && it[0] == 'enable_inverted_index_term_bf') {
                savedConfig = it[2].toString()
            }
        }
    }
    log.info('saved enable_inverted_index_term_bf=' + savedConfig)

    def extractCounter = { String profile, String name ->
        def m = Pattern.compile(Pattern.quote(name) + '[^0-9]*([0-9][0-9.,]*)').matcher(profile)
        if (!m.find()) { return -1L }
        return Long.parseLong(m.group(1).replaceAll(',', ''))
    }

    try {
        // Table A: NO token_bloom_filter property. Under the new gating, the BE config alone
        // decides whether tbf is built and consulted.
        sql 'DROP TABLE IF EXISTS test_term_bf_be_gate_noprop'
        sql '''
            CREATE TABLE test_term_bf_be_gate_noprop (
                id INT,
                msg STRING,
                INDEX idx_msg (msg) USING INVERTED
                    PROPERTIES ("parser" = "english", "support_phrase" = "true")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        '''
        sql '''INSERT INTO test_term_bf_be_gate_noprop VALUES
                  (1, 'apple banana'), (2, 'cherry date'), (3, 'apple cherry')'''
        sql 'sync'

        // Step 1: enable the BE config and prove the fast path engages even though the index
        // has NO token_bloom_filter property -- the new gate is BE config only.
        setBeConfig('enable_inverted_index_term_bf', 'true')
        sql 'set enable_profile = true'
        sql 'set profile_level = 1'

        def qidOn = 'test_term_bf_be_gate_on_' + System.currentTimeMillis()
        profile("${qidOn}") {
            run {
                sql "/* ${qidOn} */ SELECT id FROM test_term_bf_be_gate_noprop " +
                        "WHERE msg MATCH 'durianzzz'"
            }
            check { profileString, exception ->
                if (exception) { throw exception }
                def skipped = extractCounter(profileString, 'InvertedIndexTermBfSkippedLookups')
                def probe = extractCounter(profileString, 'InvertedIndexTermBfProbe')
                log.info('config-on,no-property: skipped=' + skipped + ' probe=' + probe)
                // We need a fresh build with the config ON to assert >=1. The earlier writer
                // ran with whatever the cluster default was. So we also accept the case where
                // tbf was not emitted yet (unavailable >= 1) and only assert that the counters
                // were considered (probe + skipped + unavailable >= 1).
                def unavailable = extractCounter(profileString, 'InvertedIndexTermBfUnavailable')
                assertTrue(probe + skipped + unavailable >= 1,
                        'with config on the BF fast path must run even without the property; '
                        + 'profile snippet did not surface any of probe/skipped/unavailable. '
                        + 'profile head=' + profileString.take(4000))
            }
        }

        // Rebuild the table so the WRITER runs with the config on -- gives us a deterministic
        // tbf on disk, no matter what state the cluster was in earlier.
        sql 'DROP TABLE test_term_bf_be_gate_noprop'
        sql '''
            CREATE TABLE test_term_bf_be_gate_noprop (
                id INT,
                msg STRING,
                INDEX idx_msg (msg) USING INVERTED
                    PROPERTIES ("parser" = "english", "support_phrase" = "true")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        '''
        sql '''INSERT INTO test_term_bf_be_gate_noprop VALUES
                  (1, 'apple banana'), (2, 'cherry date'), (3, 'apple cherry')'''
        sql 'sync'

        def qidRebuilt = 'test_term_bf_be_gate_rebuilt_' + System.currentTimeMillis()
        profile("${qidRebuilt}") {
            run {
                sql "/* ${qidRebuilt} */ SELECT id FROM test_term_bf_be_gate_noprop " +
                        "WHERE msg MATCH 'durianzzz'"
            }
            check { profileString, exception ->
                if (exception) { throw exception }
                def skipped = extractCounter(profileString, 'InvertedIndexTermBfSkippedLookups')
                def probe = extractCounter(profileString, 'InvertedIndexTermBfProbe')
                log.info('config-on,no-property,rebuilt: skipped=' + skipped + ' probe=' + probe)
                assertTrue(skipped >= 1,
                        'BE config on, fresh build -> tbf emitted and reader fast-paths absent '
                        + 'term, expected SkippedLookups >= 1; got ' + skipped
                        + '. profile head=' + profileString.take(4000))
            }
        }

        // Step 2: disable the BE config and prove the property=true variant is now inert -- the
        // fast path bypasses regardless of property.
        setBeConfig('enable_inverted_index_term_bf', 'false')

        sql 'DROP TABLE IF EXISTS test_term_bf_be_gate_propon'
        sql '''
            CREATE TABLE test_term_bf_be_gate_propon (
                id INT,
                msg STRING,
                INDEX idx_msg (msg) USING INVERTED
                    PROPERTIES ("parser" = "english", "support_phrase" = "true",
                                "token_bloom_filter" = "true")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        '''
        sql '''INSERT INTO test_term_bf_be_gate_propon VALUES
                  (1, 'apple banana'), (2, 'cherry date'), (3, 'apple cherry')'''
        sql 'sync'

        def qidOff = 'test_term_bf_be_gate_off_' + System.currentTimeMillis()
        profile("${qidOff}") {
            run {
                sql "/* ${qidOff} */ SELECT id FROM test_term_bf_be_gate_propon " +
                        "WHERE msg MATCH 'durianzzz'"
            }
            check { profileString, exception ->
                if (exception) { throw exception }
                def skipped = extractCounter(profileString, 'InvertedIndexTermBfSkippedLookups')
                def probe = extractCounter(profileString, 'InvertedIndexTermBfProbe')
                def unavailable = extractCounter(profileString, 'InvertedIndexTermBfUnavailable')
                log.info('config-off,property=true: skipped=' + skipped + ' probe=' + probe
                        + ' unavailable=' + unavailable)
                // Counter rows may be absent from the profile (returned as -1 by extractCounter)
                // when the feature did not engage; treat "missing or zero" as the pass.
                def asZero = { long v -> v <= 0 }
                assertTrue(asZero(skipped) && asZero(probe) && asZero(unavailable),
                        'BE config off must inhibit the BF fast path even with property=true; '
                        + 'got skipped=' + skipped + ' probe=' + probe + ' unavailable=' + unavailable
                        + '. profile head=' + profileString.take(4000))
            }
        }
        // Step 3: prove that the index-compaction shortcut also emits tbf. The default
        // compaction path on DUP_KEYS + fulltext index goes through
        // `do_inverted_index_compaction`'s `compact_column`, which used to bypass
        // `InvertedIndexColumnWriter::finish()` and drop the tbf entirely from the merged
        // output. Trigger a cumulative compaction explicitly after multiple INSERTs, then
        // re-query: SkippedLookups must be > 0 (tbf carried through compaction).
        setBeConfig('enable_inverted_index_term_bf', 'true')

        sql 'DROP TABLE IF EXISTS test_term_bf_be_gate_compact'
        sql '''
            CREATE TABLE test_term_bf_be_gate_compact (
                id INT,
                msg STRING,
                INDEX idx_msg (msg) USING INVERTED
                    PROPERTIES ("parser" = "english", "support_phrase" = "true")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1",
                        "disable_auto_compaction" = "true");
        '''
        // Multiple INSERTs produce multiple rowsets so cumulative compaction has something to
        // merge. Each INSERT runs with the config ON so source rowsets already carry tbf.
        sql '''INSERT INTO test_term_bf_be_gate_compact VALUES (1, 'apple banana'),
                  (2, 'cherry date'), (3, 'apple cherry')'''
        sql '''INSERT INTO test_term_bf_be_gate_compact VALUES (4, 'fig grape'),
                  (5, 'honeydew kiwi'), (6, 'lemon mango')'''
        sql '''INSERT INTO test_term_bf_be_gate_compact VALUES (7, 'nectarine orange'),
                  (8, 'papaya quince'), (9, 'raspberry strawberry')'''
        sql 'sync'

        // Trigger cumulative compaction so the rowsets above are merged via the
        // index-compaction shortcut (DUP_KEYS + inverted_index_compaction_enable=true). On a
        // single-BE setup the standard pattern is to ask each BE for compaction on this tablet.
        def tabletId = (sql_return_maparray """SHOW TABLETS FROM test_term_bf_be_gate_compact""")
                .collect { it.TabletId.toString() }.first()
        log.info('triggering cumulative compaction on tablet=' + tabletId)
        backends.each { be ->
            try {
                def url = "http://${be.ip}:${be.httpPort}/api/compaction/run?tablet_id=${tabletId}&compact_type=cumulative"
                def (code, out, err) = http_client('POST', url)
                log.info('compaction trigger code=' + code + ' out=' + out + ' err=' + err)
            } catch (Throwable t) {
                log.warn('compaction trigger failed: ' + t.message)
            }
        }
        // Wait for compaction to finish (poll the API). We give it up to 60s.
        for (int i = 0; i < 30; ++i) {
            sleep(2000)
            boolean any_running = false
            backends.each { be ->
                try {
                    def (code, out, err) = http_client('GET',
                            "http://${be.ip}:${be.httpPort}/api/compaction/run_status?tablet_id=${tabletId}")
                    if (out?.contains('"compaction_count":0') == false && out?.contains('running')) {
                        any_running = true
                    }
                } catch (Throwable t) { /* tolerate */ }
            }
            if (!any_running) break
        }
        sql 'sync'

        def qidCompact = 'test_term_bf_be_gate_compact_' + System.currentTimeMillis()
        profile("${qidCompact}") {
            run {
                sql "/* ${qidCompact} */ SELECT id FROM test_term_bf_be_gate_compact " +
                        "WHERE msg MATCH 'durianzzz'"
            }
            check { profileString, exception ->
                if (exception) { throw exception }
                def skipped = extractCounter(profileString, 'InvertedIndexTermBfSkippedLookups')
                def probe = extractCounter(profileString, 'InvertedIndexTermBfProbe')
                def unavailable = extractCounter(profileString, 'InvertedIndexTermBfUnavailable')
                def loadCount = extractCounter(profileString, 'InvertedIndexTermBfLoadCount')
                log.info('post-compaction: skipped=' + skipped + ' probe=' + probe
                        + ' unavailable=' + unavailable + ' load_count=' + loadCount)
                // Pre-fix bug: after compact_column, tbf was missing from merged segments, so
                // unavailable > 0 and probe == 0. Post-fix: tbf is preserved, so probe >= 1
                // and skipped >= 1 (absent term).
                assertTrue(probe >= 1,
                        'index-compaction shortcut must preserve tbf; expected Probe >= 1 '
                        + 'but got probe=' + probe + ' unavailable=' + unavailable
                        + '. profile head=' + profileString.take(6000))
                assertTrue(skipped >= 1,
                        'absent term on a compacted segment should short-circuit; '
                        + 'expected SkippedLookups >= 1 but got ' + skipped
                        + '. profile head=' + profileString.take(6000))
            }
        }
    } finally {
        // Restore the BE config so we do not leak state to other suites.
        if (savedConfig != null) {
            setBeConfig('enable_inverted_index_term_bf', savedConfig)
        }
    }
}
