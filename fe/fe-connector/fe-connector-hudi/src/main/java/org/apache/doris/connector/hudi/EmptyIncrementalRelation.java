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

package org.apache.doris.connector.hudi;

import org.apache.hudi.common.model.FileSlice;

import java.util.Collections;
import java.util.List;

/**
 * The {@code @incr} relation for an empty completed timeline: it selects nothing. Connector-internal port of
 * legacy {@code datasource.hudi.source.EmptyIncrementalRelation}. The scan planner routes the empty-timeline
 * case here (legacy {@code LogicalHudiScan.withScanParams:261-262}), so COW/MOR are never built empty.
 *
 * <p>Legacy's {@code getHoodieParams()} (which set {@code hoodie.datasource.read.incr.operation} /
 * {@code includeStartTime} on the BE-facing params) is intentionally NOT ported: the connector uses the FE-side
 * synthetic-predicate model for row correctness and emits NO {@code hoodie.datasource.read.*} incremental keys
 * to BE, so those keys are inert (see the incremental-read step design). {@code getStartTs()} is likewise
 * dropped (the resolved window lives on the handle); {@code getEndTs()} returns the legacy {@code "000"} bound.
 */
final class EmptyIncrementalRelation implements IncrementalRelation {

    private static final String EMPTY_TS = "000";

    @Override
    public List<FileSlice> collectFileSlices() {
        return Collections.emptyList();
    }

    @Override
    public List<HudiScanRange> collectSplits() {
        return Collections.emptyList();
    }

    @Override
    public boolean fallbackFullTableScan() {
        return false;
    }

    @Override
    public String getEndTs() {
        return EMPTY_TS;
    }
}
