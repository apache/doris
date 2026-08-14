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
import java.util.Objects;

/** Paimon HASH_DYNAMIC input routing with one SDK bucket assigner per owning writer. */
public final class DistributionSpecPaimonHashDynamic
        extends DistributionSpecExternalTableSinkHashPartitioned {

    private final ImmutableList<Integer> partitionFieldIndexes;
    private final ImmutableList<Integer> primaryKeyFieldIndexes;
    private final int numAssigners;

    /** Create an exact Paimon RowAssignerChannelComputer distribution. */
    public DistributionSpecPaimonHashDynamic(List<ExprId> routeExprIds,
            List<Integer> partitionFieldIndexes, List<Integer> primaryKeyFieldIndexes,
            int numAssigners) {
        super(routeExprIds);
        this.partitionFieldIndexes = checkedIndexes(
                partitionFieldIndexes, routeExprIds.size(), false);
        this.primaryKeyFieldIndexes = checkedIndexes(
                primaryKeyFieldIndexes, routeExprIds.size(), true);
        if (numAssigners <= 0) {
            throw new IllegalArgumentException("Paimon assigner count must be positive");
        }
        this.numAssigners = numAssigners;
    }

    private static ImmutableList<Integer> checkedIndexes(
            List<Integer> indexes, int exprCount, boolean requireNonEmpty) {
        Objects.requireNonNull(indexes, "Paimon field indexes must not be null");
        if (requireNonEmpty && indexes.isEmpty()) {
            throw new IllegalArgumentException("Paimon primary-key field indexes must not be empty");
        }
        for (Integer index : indexes) {
            if (index == null || index < 0 || index >= exprCount) {
                throw new IllegalArgumentException("Invalid Paimon route expression index: " + index);
            }
        }
        return ImmutableList.copyOf(indexes);
    }

    public List<Integer> getPartitionFieldIndexes() {
        return partitionFieldIndexes;
    }

    public List<Integer> getPrimaryKeyFieldIndexes() {
        return primaryKeyFieldIndexes;
    }

    public int getNumAssigners() {
        return numAssigners;
    }

    @Override
    public HashAlgorithm getHashAlgorithm() {
        return HashAlgorithm.PAIMON_HASH_DYNAMIC;
    }

    @Override
    public WriterAssignment getWriterAssignment() {
        return WriterAssignment.IDENTITY;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) {
            return false;
        }
        DistributionSpecPaimonHashDynamic that = (DistributionSpecPaimonHashDynamic) other;
        return partitionFieldIndexes.equals(that.partitionFieldIndexes)
                && primaryKeyFieldIndexes.equals(that.primaryKeyFieldIndexes)
                && numAssigners == that.numAssigners;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), partitionFieldIndexes,
                primaryKeyFieldIndexes, numAssigners);
    }
}
