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

package org.apache.doris.datasource.iceberg.source;

import java.util.Arrays;

/**
 * FE-side mode switch for Iceberg metadata planning.
 *
 * <p>Each mode knows how to resolve itself to a concrete planning decision
 * given a {@link PlanningInputs} snapshot of the current query context.
 * The AUTO mode implements a threshold-based algorithm; the thresholds are
 * internal constants that are not user-tunable.
 */
public enum IcebergMetadataPlanningMode {
    LOCAL("local"),
    DISTRIBUTED("distributed"),
    AUTO("auto");

    // ---- AUTO mode decision thresholds (not user-tunable) ----
    // Minimum ratio of BE available planning slots to FE local parallelism.
    // When R <= ALPHA * L the distributed path offers no meaningful parallelism advantage.
    private static final double AUTO_ALPHA = 1.25;
    // Minimum ratio of matching manifest count to FE local parallelism.
    // When M <= BETA * L the local thread pool is not the bottleneck.
    private static final int AUTO_BETA = 2;
    // Maximum manifest bytes a single FE planning thread can handle without becoming a bottleneck.
    // When total manifest bytes B <= L * SLOT_BYTES the local path is still efficient.
    private static final long AUTO_SLOT_BYTES = 8L * 1024 * 1024; // 8 MB
    // Upper bound on estimated scan tasks to be returned from distributed planning.
    // When S >= this threshold the result serialization / network cost offsets the BE parallelism gain.
    private static final long AUTO_RESULT_THRESHOLD = 1_000_000L;

    private final String name;

    IcebergMetadataPlanningMode(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static IcebergMetadataPlanningMode fromName(String name) {
        return Arrays.stream(values())
                .filter(mode -> mode.name.equalsIgnoreCase(name))
                .findFirst()
                .orElse(LOCAL);
    }

    /**
     * Resolves whether this mode should use distributed planning given the collected
     * query-time inputs.
     *
     * <p>Callers should NOT invoke this for {@link #LOCAL}; it always returns {@code false}
     * and the caller can short-circuit before collecting inputs.
     *
     * @param inputs lightweight manifest-list metadata collected without reading manifest files
     * @return {@code true} if distributed planning should be used, {@code false} for local
     */
    public boolean resolveDistributed(PlanningInputs inputs) {
        switch (this) {
            case DISTRIBUTED:
                // Hard gate: delete manifests require extra metadata that the distributed
                // path does not currently support.
                return !inputs.needStats;
            case AUTO:
                return resolveAuto(inputs);
            default: // LOCAL
                return false;
        }
    }

    private boolean resolveAuto(PlanningInputs inputs) {
        // need_stats: delete manifests require extra metadata; keep complexity local.
        if (inputs.needStats) {
            return false;
        }
        // R <= alpha * L: no meaningful BE parallelism advantage over local threads.
        if (inputs.beSlots <= AUTO_ALPHA * inputs.feParallelism) {
            return false;
        }
        // M <= beta * L: manifest count does not saturate the local thread pool.
        if (inputs.manifestCount <= AUTO_BETA * inputs.feParallelism) {
            return false;
        }
        // B <= L * slot_bytes: total manifest volume is within local capacity.
        if (inputs.manifestBytes <= (long) inputs.feParallelism * AUTO_SLOT_BYTES) {
            return false;
        }
        // S >= result_threshold: result serialization / network cost offsets the BE gain.
        if (inputs.estimatedFiles >= AUTO_RESULT_THRESHOLD) {
            return false;
        }
        return true;
    }

    /**
     * Lightweight inputs for the planning mode decision.
     *
     * <p>All fields are derived from manifest-list metadata without reading manifest files.
     */
    public static class PlanningInputs {
        /** True when delete manifests are present; signals that extra metadata is needed. */
        public final boolean needStats;
        /** M: number of data manifests that pass partition/filter pruning. */
        public final int manifestCount;
        /** B: total byte size of matching data manifests. */
        public final long manifestBytes;
        /** S: estimated number of data files across all matching manifests. */
        public final long estimatedFiles;
        /** L: FE local planning parallelism (Iceberg catalog executor thread count). */
        public final int feParallelism;
        /** R: available BE planning slots (alive backend count). */
        public final int beSlots;

        public PlanningInputs(boolean needStats, int manifestCount, long manifestBytes,
                long estimatedFiles, int feParallelism, int beSlots) {
            this.needStats = needStats;
            this.manifestCount = manifestCount;
            this.manifestBytes = manifestBytes;
            this.estimatedFiles = estimatedFiles;
            this.feParallelism = feParallelism;
            this.beSlots = beSlots;
        }
    }
}
