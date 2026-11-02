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

package org.apache.doris.changelog;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.tso.TSOTimestamp;

public class ChangelogUtils {
    // ATTN(changelog-rename): formerly "binlog<Row>"
    private static final String CHANGELOG_INDEX_NAME = "changelog";

    // Stream change types read from a changelog. Must match BE changelog_block_reader_utils.h
    // (STREAM_CHANGE_*), not the persisted changelog op codes (CHANGELOG_OP_*).
    public static final long CHANGE_TYPE_APPEND = 0;
    public static final long CHANGE_TYPE_DELETE = 1;
    public static final long CHANGE_TYPE_UPDATE_BEFORE = 2;
    public static final long CHANGE_TYPE_UPDATE_AFTER = 3;
    public static final String CHANGELOG_OFFSET_EXPIRED =
            "Changelog offset has expired according to changelog.ttl_seconds";

    public static String wrapChangelogIndexName(String originTableName) {
        return CHANGELOG_INDEX_NAME + "(" + originTableName + ")";
    }

    /**
     * Apply an inclusive changelog expiration cutoff to a scan's inclusive start TSO.
     */
    public static long effectiveStartTso(Long startTso, long cutoffTso, boolean rejectExpiredStart) {
        long firstRetainedTso = TSOTimestamp.nextTso(cutoffTso);
        if (startTso == null) {
            return firstRetainedTso;
        }
        if (rejectExpiredStart && startTso < firstRetainedTso) {
            throw new AnalysisException(CHANGELOG_OFFSET_EXPIRED
                    + ": start_tso=" + startTso + ", cutoff_tso=" + cutoffTso);
        }
        return Math.max(startTso, firstRetainedTso);
    }
}
