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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.stream.StreamReadMode;
import org.apache.doris.mtmv.BaseTableInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Statement-level input for all internal IVM rewrite flows.
 *
 * <p>The context is installed on the statement and selects which IVM path is active:
 * normalize when an IVM materialized view is analyzed, incremental when an incremental
 * refresh plan is generated, and full when a complete refresh plan uses IVM stream scans.
 * It contains rewrite configuration only; per-statement rewrite artifacts are stored in
 * {@link IvmRewriteResult}.
 */
public class IvmRewriteContext {
    public enum Mode {
        /** Normalize the materialized-view query and collect its IVM layout metadata. */
        NORMALIZE,
        /** Rewrite the normalized query into an incremental refresh delta plan. */
        INCREMENTAL,
        /** Rewrite a complete refresh plan with the IVM full-refresh stream semantics. */
        FULL
    }

    private final Mode mode;
    private final MTMV mtmv;
    private final boolean includeExhaustedStreams;
    private final Map<BaseTableInfo, Set<Long>> fullRefreshResetPartitionIds;
    private final Optional<StreamReadMode> fullRefreshNonPctReadMode;

    public IvmRewriteContext(Mode mode, MTMV mtmv, boolean includeExhaustedStreams) {
        this(mode, mtmv, includeExhaustedStreams, Collections.emptyMap(), Optional.empty());
    }

    private IvmRewriteContext(Mode mode, MTMV mtmv, boolean includeExhaustedStreams,
            Map<BaseTableInfo, Set<Long>> fullRefreshResetPartitionIds,
            Optional<StreamReadMode> fullRefreshNonPctReadMode) {
        this.mode = Objects.requireNonNull(mode, "mode can not be null");
        this.mtmv = mode == Mode.NORMALIZE ? mtmv : Objects.requireNonNull(mtmv, "mtmv can not be null");
        this.includeExhaustedStreams = includeExhaustedStreams;
        Map<BaseTableInfo, Set<Long>> resetPartitionIds = new HashMap<>();
        Objects.requireNonNull(fullRefreshResetPartitionIds, "fullRefreshResetPartitionIds can not be null")
                .forEach((baseTableInfo, partitionIds) -> resetPartitionIds.put(baseTableInfo,
                        Collections.unmodifiableSet(new HashSet<>(partitionIds))));
        this.fullRefreshResetPartitionIds = Collections.unmodifiableMap(resetPartitionIds);
        this.fullRefreshNonPctReadMode = Objects.requireNonNull(
                fullRefreshNonPctReadMode, "fullRefreshNonPctReadMode can not be null");
    }

    public static IvmRewriteContext normalize() {
        return new IvmRewriteContext(Mode.NORMALIZE, null, false);
    }

    public static IvmRewriteContext normalize(MTMV mtmv) {
        return new IvmRewriteContext(Mode.NORMALIZE, Objects.requireNonNull(mtmv, "mtmv can not be null"), false);
    }

    public static IvmRewriteContext incremental(MTMV mtmv, boolean includeExhaustedStreams) {
        return new IvmRewriteContext(Mode.INCREMENTAL, mtmv, includeExhaustedStreams);
    }

    public static IvmRewriteContext full(MTMV mtmv) {
        return new IvmRewriteContext(Mode.FULL, mtmv, false);
    }

    public static IvmRewriteContext full(MTMV mtmv,
            Map<BaseTableInfo, Set<Long>> resetPartitionIds,
            StreamReadMode nonPctReadMode) {
        return new IvmRewriteContext(Mode.FULL, mtmv, false, resetPartitionIds,
                Optional.of(Objects.requireNonNull(nonPctReadMode, "nonPctReadMode can not be null")));
    }

    public Mode getMode() {
        return mode;
    }

    public MTMV getMtmv() {
        return mtmv;
    }

    public boolean isIncludeExhaustedStreams() {
        return includeExhaustedStreams;
    }

    public boolean hasFullRefreshStreamScans() {
        return !fullRefreshResetPartitionIds.isEmpty() || fullRefreshNonPctReadMode.isPresent();
    }

    public Optional<Set<Long>> getFullRefreshResetPartitionIds(BaseTableInfo baseTableInfo) {
        return Optional.ofNullable(fullRefreshResetPartitionIds.get(baseTableInfo)).map(HashSet::new);
    }

    public Optional<StreamReadMode> getFullRefreshNonPctReadMode() {
        return fullRefreshNonPctReadMode;
    }
}
