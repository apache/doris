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
 * create when an IVM materialized view is created, normalize when an IVM materialized view is analyzed,
 * incremental when an incremental refresh plan is generated, and full when a complete refresh plan uses IVM
 * stream scans.
 * It contains rewrite configuration only; per-statement rewrite artifacts are stored in
 * {@link IvmRewriteResult}.
 */
public class IvmRewriteContext {
    public enum Mode {
        /** Normalize an IVM materialized-view query during CREATE MATERIALIZED VIEW. */
        CREATE,
        /** Normalize the materialized-view query and collect its IVM layout metadata. */
        NORMALIZE,
        /** Rewrite the normalized query into an incremental refresh delta plan. */
        INCREMENTAL,
        /** Rewrite a complete refresh plan with the IVM full-refresh stream semantics. */
        FULL
    }

    private final Mode mode;
    private final MTMV mtmv;
    // The MTMV object does not exist yet during CREATE, so keep its name separately for diagnostics.
    private final String createMtmvName;
    private final boolean includeExhaustedStreams;
    private final boolean dryRun;
    private final Optional<DryRunLimit> dryRunLimit;
    private final Map<BaseTableInfo, Set<Long>> fullRefreshResetPartitionIds;
    private final Optional<StreamReadMode> fullRefreshNonPctReadMode;
    // Set by MTMVPlanUtil before normalization: true means the MV unique keys include identity key columns.
    // Null when the rewrite context is created outside the analyzeQuery flow.
    private Boolean useFullKeys;

    public IvmRewriteContext(Mode mode, MTMV mtmv, boolean includeExhaustedStreams) {
        this(mode, mtmv, null, includeExhaustedStreams, false, Optional.empty(),
                Collections.emptyMap(), Optional.empty());
    }

    private IvmRewriteContext(Mode mode, MTMV mtmv, String createMtmvName, boolean includeExhaustedStreams,
            boolean dryRun, Optional<DryRunLimit> dryRunLimit,
            Map<BaseTableInfo, Set<Long>> fullRefreshResetPartitionIds,
            Optional<StreamReadMode> fullRefreshNonPctReadMode) {
        this.mode = Objects.requireNonNull(mode, "mode can not be null");
        this.mtmv = mode == Mode.CREATE ? mtmv : Objects.requireNonNull(mtmv, "mtmv can not be null");
        this.createMtmvName = createMtmvName;
        this.includeExhaustedStreams = includeExhaustedStreams;
        this.dryRun = dryRun;
        this.dryRunLimit = Objects.requireNonNull(dryRunLimit, "dryRunLimit can not be null");
        Map<BaseTableInfo, Set<Long>> resetPartitionIds = new HashMap<>();
        Objects.requireNonNull(fullRefreshResetPartitionIds, "fullRefreshResetPartitionIds can not be null")
                .forEach((baseTableInfo, partitionIds) -> resetPartitionIds.put(baseTableInfo,
                        Collections.unmodifiableSet(new HashSet<>(partitionIds))));
        this.fullRefreshResetPartitionIds = Collections.unmodifiableMap(resetPartitionIds);
        this.fullRefreshNonPctReadMode = Objects.requireNonNull(
                fullRefreshNonPctReadMode, "fullRefreshNonPctReadMode can not be null");
    }

    public static IvmRewriteContext create(String mtmvName) {
        return new IvmRewriteContext(Mode.CREATE, null,
                Objects.requireNonNull(mtmvName, "mtmvName can not be null"), false,
                false, Optional.empty(), Collections.emptyMap(), Optional.empty());
    }

    public static IvmRewriteContext normalize(MTMV mtmv) {
        return new IvmRewriteContext(Mode.NORMALIZE, Objects.requireNonNull(mtmv, "mtmv can not be null"), false);
    }

    public static IvmRewriteContext incremental(MTMV mtmv, boolean includeExhaustedStreams) {
        return new IvmRewriteContext(Mode.INCREMENTAL, mtmv, includeExhaustedStreams);
    }

    public static IvmRewriteContext incrementalDryRun(MTMV mtmv, Optional<DryRunLimit> dryRunLimit) {
        return new IvmRewriteContext(Mode.INCREMENTAL, mtmv, null, false,
                true, dryRunLimit, Collections.emptyMap(), Optional.empty());
    }

    public static IvmRewriteContext full(MTMV mtmv) {
        return new IvmRewriteContext(Mode.FULL, mtmv, false);
    }

    public static IvmRewriteContext full(MTMV mtmv,
            Map<BaseTableInfo, Set<Long>> resetPartitionIds,
            StreamReadMode nonPctReadMode) {
        return new IvmRewriteContext(Mode.FULL, mtmv, null, false, false, Optional.empty(), resetPartitionIds,
                Optional.of(Objects.requireNonNull(nonPctReadMode, "nonPctReadMode can not be null")));
    }

    public Mode getMode() {
        return mode;
    }

    public boolean isCreate() {
        return mode == Mode.CREATE;
    }

    public MTMV getMtmv() {
        return mtmv;
    }

    public String getMtmvName() {
        return mtmv == null ? createMtmvName : mtmv.getName();
    }

    public boolean isIncludeExhaustedStreams() {
        return includeExhaustedStreams;
    }

    // True for REFRESH ... INCREMENTAL WITH DRY RUN: the root plan must be a LogicalResultSink.
    public boolean isDryRun() {
        return dryRun;
    }

    // Present only for REFRESH ... INCREMENTAL WITH DRY RUN; empty otherwise.
    public Optional<DryRunLimit> getDryRunLimit() {
        return dryRunLimit;
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

    public Boolean getUseFullKeys() {
        return useFullKeys;
    }

    public void setUseFullKeys(boolean useFullKeys) {
        this.useFullKeys = useFullKeys;
    }
}
