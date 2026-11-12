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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.catalog.stream.BaseTableStream;

/**
 * A scan that can expose a changelog and answer its shape.
 *
 * <p>Implementations answer whether the changelog is enabled and whether the changelog this scan
 * reads contains only appends, and build the pre- and post-refresh snapshots an incremental
 * refresh reads. It says nothing about the plan a refresh runs: an {@link LogicalOlapTableStreamScan}
 * inherits this interface only through {@link LogicalOlapScan}, and it rejects every method because
 * a stream scan is never a base scan of an IVM query.
 */
public interface SupportChangelog {
    /** Whether the changelog this scan reads is enabled on its source. */
    boolean isChangelogEnabled();

    /**
     * True when the changelog this scan reads contains only appends: no DELETE or UPDATE_BEFORE,
     * so every change carries dml_factor = +1.
     */
    boolean isChangelogAppendOnly();

    /**
     * Build a snapshot of this scan from {@code stream}: the pre-refresh snapshot when
     * {@code preSnapshot} is true, the post-refresh snapshot otherwise.
     */
    LogicalPlan withSnapshot(boolean preSnapshot, BaseTableStream stream);
}
