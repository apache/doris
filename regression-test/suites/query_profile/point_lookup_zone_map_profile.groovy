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

import org.apache.doris.regression.action.ProfileAction

suite('point_lookup_zone_map_profile', 'nonConcurrent') {
    sql 'DROP TABLE IF EXISTS point_lookup_zone_map_profile'
    sql '''
        CREATE TABLE point_lookup_zone_map_profile (
            k INT NOT NULL,
            v INT NOT NULL
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    '''
    sql '''
        INSERT INTO point_lookup_zone_map_profile
        SELECT number, number FROM numbers("number" = "200000")
    '''

    sql 'set enable_profile=true'
    sql 'set profile_level=2'

    def profileAction = new ProfileAction(context)
    def token = UUID.randomUUID().toString()

    // The key range narrows the scan down to a single row, so the zone map has
    // nothing left to prune and must not report the rest of the segment as
    // filtered. The token only keeps the statement identifiable in the profile.
    qt_point """
        SELECT COUNT(*) FROM point_lookup_zone_map_profile
        WHERE k = 12345 AND v = 12345 AND '${token}' IS NOT NULL
    """

    def profile = profileAction
            .getProfileBySql(token, ['RowsKeyRangeFiltered', 'RowsStatsFiltered',
                                    'ZoneMapIndexPagesEvaluated'])
            .toString()
    logger.info("profile of ${token}: ${profile}")

    def keyRangeFiltered = (profile =~ /RowsKeyRangeFiltered:\s*([^\n]+)/).collect { it[1].trim() }
    def statsFiltered = (profile =~ /RowsStatsFiltered:\s*([^\n]+)/).collect { it[1].trim() }
    assertTrue(!keyRangeFiltered.isEmpty(), "no RowsKeyRangeFiltered in profile: ${profile}")
    assertTrue(!statsFiltered.isEmpty(), "no RowsStatsFiltered in profile: ${profile}")
    assertTrue(keyRangeFiltered.any { it != '0' },
            "the key range should have filtered rows: ${profile}")
    statsFiltered.each {
        assertEquals('0', it, "rows pruned by the key range must not be counted again: ${profile}")
    }

    def evaluatedPages = { String text, String counter ->
        def values = (text =~ /${counter}:\s*(\d+)/).collect { it[1].toLong() }
        assertTrue(!values.isEmpty(), "no ${counter} in profile: ${text}")
        values
    }
    def pointPages = evaluatedPages(profile, 'ZoneMapIndexPagesEvaluated')
    assertTrue(pointPages.any { it > 0 } && pointPages.every { it <= 2 },
            "only the candidate page of each predicate column should be evaluated: ${profile}")

    def fullToken = UUID.randomUUID().toString()
    qt_without_key """
        SELECT COUNT(*) FROM point_lookup_zone_map_profile
        WHERE v = 12345 AND '${fullToken}' IS NOT NULL
    """
    def fullProfile = profileAction.getProfileBySql(fullToken,
            ['ZoneMapIndexPagesEvaluated']).toString()
    assertTrue(evaluatedPages(fullProfile, 'ZoneMapIndexPagesEvaluated').any { it > 2 },
            "the control scan must contain multiple candidate pages: ${fullProfile}")

    qt_disjoint """
        SELECT k, v FROM point_lookup_zone_map_profile
        WHERE k IN (12345, 150000) AND v = 150000 ORDER BY k
    """
    qt_rejected """
        SELECT COUNT(*) FROM point_lookup_zone_map_profile WHERE k = 12345 AND v = 150000
    """
    qt_empty """
        SELECT COUNT(*) FROM point_lookup_zone_map_profile WHERE k = 200000 AND v = 12345
    """

    sql 'set enable_expr_zonemap_filter=true'
    def exprToken = UUID.randomUUID().toString()
    qt_expression """
        SELECT COUNT(*) FROM point_lookup_zone_map_profile
        WHERE k = 12345 AND (v < 12346 OR v > 150000) AND '${exprToken}' IS NOT NULL
    """
    def exprProfile = profileAction.getProfileBySql(exprToken,
            ['ZoneMapIndexPagesEvaluated']).toString()
    def exprPages = evaluatedPages(exprProfile, 'ZoneMapIndexPagesEvaluated')
    assertTrue(exprPages.any { it > 0 } && exprPages.every { it <= 2 },
            "expression ZoneMaps should also evaluate only candidate pages: ${exprProfile}")

    // This expression matches elsewhere in the segment, but not on the key range's page.
    // Require an expression-page rejection so ordinary key pruning cannot satisfy the test alone.
    def exprRejectedToken = UUID.randomUUID().toString()
    qt_expression_rejected """
        SELECT COUNT(*) FROM point_lookup_zone_map_profile
        WHERE k = 12345 AND (v < 0 OR v > 150000) AND '${exprRejectedToken}' IS NOT NULL
    """
    def exprRejectedProfile = profileAction.getProfileBySql(exprRejectedToken,
            ['ZoneMapIndexPagesEvaluated', 'ExprZoneMapFilteredPages']).toString()
    assertTrue(evaluatedPages(exprRejectedProfile, 'ExprZoneMapFilteredPages').any { it > 0 },
            "the candidate page must be rejected by the expression: ${exprRejectedProfile}")
    assertTrue(evaluatedPages(exprRejectedProfile, 'ZoneMapIndexPagesEvaluated').every { it <= 2 },
            "expression pruning must not evaluate unrelated pages: ${exprRejectedProfile}")
    sql 'set enable_expr_zonemap_filter=false'
    qt_expression_disabled """
        SELECT COUNT(*) FROM point_lookup_zone_map_profile
        WHERE k = 12345 AND (v < 12346 OR v > 150000)
    """

    sql 'DROP TABLE IF EXISTS point_lookup_bloom_filter_profile'
    sql '''
        CREATE TABLE point_lookup_bloom_filter_profile (
            k INT NOT NULL,
            v INT NOT NULL
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "bloom_filter_columns" = "v"
        )
    '''
    sql '''
        INSERT INTO point_lookup_bloom_filter_profile
        SELECT number, number FROM numbers("number" = "200000")
    '''
    def bloomToken = UUID.randomUUID().toString()
    qt_bloom_point """
        SELECT COUNT(*) FROM point_lookup_bloom_filter_profile
        WHERE k = 12345 AND v = 12345 AND '${bloomToken}' IS NOT NULL
    """
    def bloomProfile = profileAction.getProfileBySql(bloomToken,
            ['BloomFilterIndexPagesEvaluated']).toString()
    def bloomPages = evaluatedPages(bloomProfile, 'BloomFilterIndexPagesEvaluated')
    assertTrue(bloomPages.any { it > 0 } && bloomPages.every { it <= 1 },
            "only the candidate page's Bloom filter should be evaluated: ${bloomProfile}")
}
