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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.thrift.TVectorMetric;

import java.util.Arrays;
import java.util.Objects;

/**
 * Primary-key vector (ANN) top-N request pushed down into a file scan (Paimon).
 *
 * <p>Groups the query vector, the indexed vector column name, the retrieval
 * limit (already folded as {@code user_limit + user_offset}) and the distance
 * metric so they travel together through the logical/physical scan plumbing
 * instead of as four loose parameters. Immutable.
 *
 * <p>{@code metric} is the index's distance metric, validated by the rewrite rule
 * to match the distance function ({@code L2} or {@code DOT_PRODUCT}; v1 has no
 * cosine). The BE needs it because the reader-produced score column carries a
 * metric-dependent transform that it undoes in place, so an unset or wrong metric
 * silently corrupts the returned distances.
 *
 * <p>{@code queryVector} is {@code float[]} because paimon-rust's vector index
 * operates on f32; carrying the query as floats (rather than {@code List<Double>})
 * avoids a double&rarr;float narrowing on the wire and lets
 * {@code PaimonScanNode} encode it straight into a {@link
 * org.apache.doris.thrift.TSearchVector} with {@code FLOAT32} element type.
 */
public class AnnTopNInfo {
    private final float[] queryVector;
    private final String columnName;
    private final long limit;
    private final TVectorMetric metric;

    /**
     * Constructor. {@code metric} must be non-null; see the field assignment below.
     * Defensive-copies {@code queryVector} so later mutation by the caller cannot
     * corrupt the value that travels through the plan.
     */
    public AnnTopNInfo(float[] queryVector, String columnName, long limit, TVectorMetric metric) {
        this.queryVector = queryVector == null ? null : queryVector.clone();
        this.columnName = columnName;
        this.limit = limit;
        // Enforced here rather than at the planning sites downstream. By the time this
        // object exists the rewrite has already committed to the distance column, so no
        // later stage can safely degrade to a non-vector scan (see
        // PaimonScanNode.isVectorSearch); a null metric can only be a programming error
        // in a future caller, and failing at construction points straight at it.
        this.metric = Objects.requireNonNull(metric, "ANN distance metric can not be null");
    }

    public float[] getQueryVector() {
        return queryVector == null ? null : queryVector.clone();
    }

    public String getColumnName() {
        return columnName;
    }

    public long getLimit() {
        return limit;
    }

    public TVectorMetric getMetric() {
        return metric;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AnnTopNInfo that = (AnnTopNInfo) o;
        return limit == that.limit
                && Arrays.equals(queryVector, that.queryVector)
                && Objects.equals(columnName, that.columnName)
                && metric == that.metric;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(columnName, limit, metric);
        result = 31 * result + Arrays.hashCode(queryVector);
        return result;
    }

    @Override
    public String toString() {
        return "AnnTopNInfo{column=" + columnName + ", limit=" + limit
                + ", metric=" + metric
                + ", vectorDim=" + (queryVector == null ? 0 : queryVector.length) + "}";
    }
}
