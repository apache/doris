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

package org.apache.doris.regression.util

import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets

import static org.junit.jupiter.api.Assertions.*

class RowsetMetaUtilsTest {
    @Test
    void mergedAndUnsortedRowsetsCoverCommittedVersion() {
        assertTrue(RowsetMetaUtils.coversVersion(
                [rowsets: ['[32-61] 3 DATA', '[0-1] 0 DATA', '[2-31] 2 DATA']], 61))
    }

    @Test
    void maximumVersionCannotHideGapOrMissingInitialRowset() {
        assertFalse(RowsetMetaUtils.coversVersion([rowsets: ['[0-1]', '[3-100]']], 11))
        assertFalse(RowsetMetaUtils.coversVersion([rowsets: ['[2-100]']], 11))
        assertFalse(RowsetMetaUtils.coversVersion([rowsets: ['[0-1]']], 2))
        assertFalse(RowsetMetaUtils.coversVersion([rowsets: []], 2))
    }

    @Test
    void gapAfterTargetDoesNotInvalidateTargetCoverage() {
        assertTrue(RowsetMetaUtils.coversVersion([rowsets: ['[0-1]', '[2-6]', '[8-9]']], 6))
    }

    @Test
    void malformedMetadataFailsInsteadOfTimingOut() {
        assertThrows(IllegalArgumentException) { RowsetMetaUtils.coversVersion([:], 2) }
        assertThrows(IllegalArgumentException) {
            RowsetMetaUtils.coversVersion([rowsets: ['not a rowset']], 2)
        }
        assertThrows(IllegalArgumentException) {
            RowsetMetaUtils.coversVersion([rowsets: ['[5-2]']], 2)
        }
        assertThrows(IllegalArgumentException) { RowsetMetaUtils.coversVersion([rowsets: []], -1) }
    }

    @Test
    void keyBoundsPreserveAllBytesAndDoNotMutateInput() {
        byte[] bytes = [0, 2, 127, -128, -1] as byte[]
        String encoded = Base64.encoder.encodeToString(bytes)
        def input = [segments_key_bounds: [[min_key: encoded, max_key: encoded]],
                     segments_key_bounds_truncated: true]
        def decoded = RowsetMetaUtils.decodeKeyBounds(input)
        assertArrayEquals(bytes, decoded.segments_key_bounds[0].min_key.getBytes(StandardCharsets.ISO_8859_1))
        assertEquals(encoded, input.segments_key_bounds[0].min_key)
        assertTrue(decoded.segments_key_bounds_truncated)
        assertFalse(decoded.segments_key_bounds_aggregated)
        assertFalse(input.containsKey('segments_key_bounds_aggregated'))
    }

    @Test
    void missingOrInvalidKeyBoundsAreRejected() {
        assertThrows(IllegalArgumentException) {
            RowsetMetaUtils.decodeKeyBounds([segments_key_bounds: [[max_key: 'AA==']]])
        }
        assertThrows(IllegalArgumentException) {
            RowsetMetaUtils.decodeKeyBounds([segments_key_bounds: [[min_key: '***', max_key: 'AA==']]])
        }
    }
}
