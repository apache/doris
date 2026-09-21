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
    sql """
        SELECT COUNT(*) FROM point_lookup_zone_map_profile
        WHERE k = 12345 AND v = 12345 AND '${token}' IS NOT NULL
    """

    def profile = profileAction
            .getProfileBySql(token, ['RowsKeyRangeFiltered', 'RowsStatsFiltered'])
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
}
