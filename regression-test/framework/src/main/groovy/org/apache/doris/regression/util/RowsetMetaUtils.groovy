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

import java.nio.charset.StandardCharsets

class RowsetMetaUtils {
    /** Only active rowsets count; a high max version must not hide a gap in the version chain. */
    static boolean coversVersion(Map status, long version) {
        if (version < 0) {
            throw new IllegalArgumentException("negative target version: ${version}")
        }
        if (!(status.rowsets instanceof List)) {
            throw new IllegalArgumentException("compaction response is missing rowsets: ${status}")
        }
        def ranges = status.rowsets.collect { rowset ->
            def match = rowset.toString() =~ /^\[(\d+)-(\d+)\](?:\s.*)?$/
            if (!match.matches()) {
                throw new IllegalArgumentException("invalid compaction rowset: ${rowset}")
            }
            long start = match.group(1).toLong()
            long end = match.group(2).toLong()
            if (start > end) {
                throw new IllegalArgumentException("invalid rowset version range: ${rowset}")
            }
            [start, end]
        }.sort { a, b -> a[0] <=> b[0] }
        long covered = -1
        for (def range : ranges) {
            if (range[0] > covered + 1) {
                return false
            }
            covered = Math.max(covered, range[1] as long)
            if (covered >= version) {
                return true
            }
        }
        return false
    }

    /** MS protobuf JSON encodes bytes as base64. Preserve one character per key byte. */
    static Map decodeKeyBounds(Map meta) {
        Map result = new LinkedHashMap(meta)
        if (meta.segments_key_bounds instanceof List) {
            result.segments_key_bounds = meta.segments_key_bounds.collect { bounds ->
                Map decoded = new LinkedHashMap(bounds)
                ['min_key', 'max_key'].each { key ->
                    if (!(bounds[key] instanceof String)) {
                        throw new IllegalArgumentException("key bounds are missing ${key}")
                    }
                    decoded[key] = new String(Base64.decoder.decode(bounds[key]), StandardCharsets.ISO_8859_1)
                }
                decoded
            }
        }
        // These optional protobuf flags default to false when omitted from MS JSON.
        ['segments_key_bounds_truncated', 'segments_key_bounds_aggregated'].each { key ->
            if (!result.containsKey(key)) {
                result[key] = false
            }
        }
        return result
    }
}
