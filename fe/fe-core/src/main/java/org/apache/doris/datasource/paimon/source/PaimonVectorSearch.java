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
}
