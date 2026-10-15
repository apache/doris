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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.trees.expressions.ExprId;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A named cross-table mapping from determinant expressions to positions in the storage distribution key.
 */
public class DistributionMapping {
    private final String mappingId;
    private final List<ExprId> determinantExprIds;
    private final List<Integer> targetDistributionIndices;

    /** Constructor. */
    public DistributionMapping(String mappingId, List<ExprId> determinantExprIds,
            List<Integer> targetDistributionIndices) {
        this.mappingId = Objects.requireNonNull(mappingId, "mappingId should not be null");
        this.determinantExprIds = ImmutableList.copyOf(determinantExprIds);
        this.targetDistributionIndices = ImmutableList.copyOf(targetDistributionIndices);
    }

    public String getMappingId() {
        return mappingId;
    }

    public List<ExprId> getDeterminantExprIds() {
        return determinantExprIds;
    }

    public List<Integer> getTargetDistributionIndices() {
        return targetDistributionIndices;
    }

    /** Remap determinant expressions through a projection, or drop the mapping if any determinant is absent. */
    public Optional<DistributionMapping> project(Map<ExprId, ExprId> projections) {
        ImmutableList.Builder<ExprId> projected = ImmutableList.builderWithExpectedSize(determinantExprIds.size());
        for (ExprId determinant : determinantExprIds) {
            ExprId projectedExprId = projections.get(determinant);
            if (projectedExprId == null) {
                return Optional.empty();
            }
            projected.add(projectedExprId);
        }
        return Optional.of(new DistributionMapping(mappingId, projected.build(), targetDistributionIndices));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DistributionMapping)) {
            return false;
        }
        DistributionMapping that = (DistributionMapping) o;
        return mappingId.equals(that.mappingId)
                && determinantExprIds.equals(that.determinantExprIds)
                && targetDistributionIndices.equals(that.targetDistributionIndices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mappingId, determinantExprIds, targetDistributionIndices);
    }

    @Override
    public String toString() {
        return "DistributionMapping{"
                + "mappingId='" + mappingId + '\''
                + ", determinantExprIds=" + determinantExprIds
                + ", targetDistributionIndices=" + targetDistributionIndices
                + '}';
    }
}
