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

/** Paimon-compatible partition and fixed-bucket routing with stable writer ownership. */
public final class DistributionSpecPaimonTableSinkHashPartitioned
        extends DistributionSpecExternalTableSinkHashPartitioned {

    private final int numBuckets;
    private final ImmutableList<Integer> partitionFieldIndexes;
    private final ImmutableList<Integer> bucketFieldIndexes;

    /** Creates a Paimon fixed-bucket distribution specification. */
    public DistributionSpecPaimonTableSinkHashPartitioned(List<ExprId> routeExprIds,
            int numBuckets, List<Integer> partitionFieldIndexes, List<Integer> bucketFieldIndexes) {
        super(routeExprIds);
        if (numBuckets <= 0) {
            throw new IllegalArgumentException("Paimon bucket count must be positive");
        }
        this.numBuckets = numBuckets;
        this.partitionFieldIndexes = checkedIndexes(partitionFieldIndexes, routeExprIds.size(), false);
        this.bucketFieldIndexes = checkedIndexes(bucketFieldIndexes, routeExprIds.size(), true);
    }

    private static ImmutableList<Integer> checkedIndexes(
            List<Integer> indexes, int exprCount, boolean requireNonEmpty) {
        Objects.requireNonNull(indexes, "Paimon field indexes must not be null");
        if (requireNonEmpty && indexes.isEmpty()) {
            throw new IllegalArgumentException("Paimon bucket field indexes must not be empty");
        }
        for (Integer index : indexes) {
            if (index == null || index < 0 || index >= exprCount) {
                throw new IllegalArgumentException("Invalid Paimon route expression index: " + index);
            }
        }
        return ImmutableList.copyOf(indexes);
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public List<Integer> getPartitionFieldIndexes() {
        return partitionFieldIndexes;
    }

    public List<Integer> getBucketFieldIndexes() {
        return bucketFieldIndexes;
    }

    @Override
    public HashAlgorithm getHashAlgorithm() {
        return HashAlgorithm.PAIMON_FIXED_BUCKET;
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
        DistributionSpecPaimonTableSinkHashPartitioned that
                = (DistributionSpecPaimonTableSinkHashPartitioned) other;
        return numBuckets == that.numBuckets
                && partitionFieldIndexes.equals(that.partitionFieldIndexes)
                && bucketFieldIndexes.equals(that.bucketFieldIndexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), numBuckets, partitionFieldIndexes, bucketFieldIndexes);
    }
}
