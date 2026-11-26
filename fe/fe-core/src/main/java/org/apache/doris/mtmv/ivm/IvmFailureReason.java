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

package org.apache.doris.mtmv.ivm;

/** Reasons IVM analysis or execution can fail. */
public enum IvmFailureReason {
    // ATTN(changelog-rename): formerly "BINLOG_BROKEN"
    /**
     * The changelog a stream has to read from is outside the retention: changelog.ttl_seconds expired its offset.
     * A partition refresh still reads the streams at their offsets, so only COMPLETE, the one refresh that resets
     * the stream baselines, repairs it.
     */
    CHANGELOG_BROKEN,
    // ATTN(changelog-rename): formerly "BINLOG_NOT_ENABLED"
    CHANGELOG_NOT_ENABLED,
    STREAM_UNSUPPORTED,
    SNAPSHOT_ALIGNMENT_UNSUPPORTED,
    PLAN_PATTERN_UNSUPPORTED,
    PLAN_REWRITE_FAILED,
    NON_DETERMINISTIC_ROW_ID,
    OUTER_JOIN_RETRACTION_UNSUPPORTED,
    INCREMENTAL_EXECUTION_FAILED,
    AGG_UNSUPPORTED,
    MIN_MAX_BOUNDARY_HIT,
    BITMAP_AGG_DELETE,
    PLAN_SIGNATURE_MISMATCH,
    /** A previous IVM refresh txn committed but its data is not yet visible. */
    MV_COMMIT_NOT_VISIBLE,
    MV_PARTITION_NOT_FOUND;

    /**
     * Whether COMPLETE is the only refresh that repairs this failure, so that a refresh allowed to fall back goes
     * to it directly instead of trying a partition refresh first.
     */
    public boolean requiresCompleteRefresh() {
        return this == CHANGELOG_BROKEN
                || this == STREAM_UNSUPPORTED
                || this == MIN_MAX_BOUNDARY_HIT
                || this == BITMAP_AGG_DELETE
                || this == PLAN_SIGNATURE_MISMATCH;
    }
}
