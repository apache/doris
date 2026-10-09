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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.system.Backend;

import org.apache.paimon.table.Table;

import java.util.Collection;
import java.util.Map;

/**
 * Shared definition of the reader-produced primary-key vector (ANN) search output
 * column for the index-only (Approach B) search path.
 *
 * <p>The column holds a <b>distance</b>, not paimon-rust's score. paimon-rust emits
 * an Arrow field named {@code __paimon_search_score} carrying a higher-is-better
 * score, and the BE PaimonRustTableReader inverts that transform in place while
 * filling the block, so the value Doris sees is exactly what
 * {@code l2_distance_approximate} / {@code inner_product_approximate} are defined
 * to return.
 *
 * <p>That is why the Doris-side name is {@link #SEARCH_DISTANCE_COLUMN}
 * ({@code __paimon_search_score_to_dis}) and not the Arrow name: the two differ in
 * meaning, so they must not share a name. Keeping them distinct also leaves
 * {@code __paimon_search_score} free should a later version want to expose the raw
 * score as its own column. BE bridges the two names explicitly when filling the
 * block; the Arrow-side name stays owned by paimon-rust. Unlike Doris hidden columns
 * this one deliberately does not carry the {@code __DORIS_} prefix.
 *
 * <p>Because the column is reader-produced (not a real Paimon table column), its
 * slot must still carry a non-null {@link Column} so it survives
 * {@code FileQueryScanNode.initSchemaParams}, but it is excluded from the Parquet/ORC
 * column-position mapping (see {@code FileQueryScanNode.setColumnPositionMapping}).
 */
public final class PaimonVectorSearch {

    /**
     * Doris-side name of the reader-produced distance column. Must match
     * {@code kPaimonSearchDistanceColumn} in the BE reader, which maps
     * paimon-rust's {@code __paimon_search_score} Arrow field onto it.
     */
    public static final String SEARCH_DISTANCE_COLUMN = "__paimon_search_score_to_dis";

    private static final Column DISTANCE_COLUMN = createDistanceColumn();

    private PaimonVectorSearch() {}

    /** Shared, immutable distance column instance; do not mutate. */
    public static Column getDistanceColumn() {
        return DISTANCE_COLUMN;
    }

    private static Column createDistanceColumn() {
        // Non-nullable FLOAT, matching the distance slot produced by the rewrite rule.
        Column column = new Column(SEARCH_DISTANCE_COLUMN, Type.FLOAT, false,
                "Paimon primary-key vector search distance (reader-produced)");
        column.setIsVisible(false);
        return column;
    }

    /**
     * Whether a PK-vector (ANN) top-N on this table can execute on the paimon-rust
     * vector search path. This is the single definition shared by the
     * {@code PushDownVectorTopNIntoPaimonScan} rewrite's pre-flight and
     * {@code PaimonScanNode}'s per-split vector gate, because the rewrite is one-way:
     * a rewritten vector split has no JNI fallback (the JNI reader ignores the vector
     * payload and would return a non-ANN scan with an empty distance column), so the
     * scan node can only fail loudly — a gate-failing table must therefore not be
     * rewritten at all, leaving the query to run as an ordinary scan that computes the
     * distance in Doris.
     *
     * <p>Delegates to {@link PaimonRustEligibility}, the single computation of
     * the table-, storage- and backend-level gates also consumed by the batch
     * rust gate in {@code PaimonScanNode}. Does not cover the
     * projection-dependent bounds (ORC TIMESTAMP_LTZ, projected VARIANT): they
     * depend on the final scan tuple and stay scan-node-side. The scan node
     * evaluates the relation-option-processed table while the rewrite sees the
     * raw cached table, so a {@code t@options} override can still make the
     * scan node reject a split the pre-flight accepted — the scan node stays
     * authoritative and fails loudly. The BE capability negotiation requires
     * every candidate backend to report {@code supports_paimon_rust_reader}:
     * a PK-vector split is only decodable by a rust-capable BE.
     */
    public static boolean vectorScanEligible(Table paimonTable, String tableLocation,
            Map<String, String> backendStorageProperties, Collection<Backend> backends) {
        return PaimonRustEligibility.of(paimonTable, tableLocation,
                backendStorageProperties, backends).eligible();
    }
}
