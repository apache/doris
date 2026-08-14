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

/** Common logical-partition and writer-assignment contract for external table sink writers. */
public abstract class DistributionSpecExternalTableSinkHashPartitioned extends DistributionSpec {
    public static final int MIN_BE_EXEC_VERSION = 12;

    private final ImmutableList<ExprId> outputColumnExprIds;

    protected DistributionSpecExternalTableSinkHashPartitioned(List<ExprId> outputColumnExprIds) {
        this.outputColumnExprIds = ImmutableList.copyOf(
                Objects.requireNonNull(outputColumnExprIds, "outputColumnExprIds should not be null"));
        if (this.outputColumnExprIds.isEmpty()) {
            throw new IllegalArgumentException("outputColumnExprIds should not be empty");
        }
    }

    public List<ExprId> getOutputColumnExprIds() {
        return outputColumnExprIds;
    }

    public abstract HashAlgorithm getHashAlgorithm();

    public abstract WriterAssignment getWriterAssignment();

    public List<String> getPartitionTransforms() {
        return ImmutableList.of();
    }

    @Override
    public boolean satisfy(DistributionSpec required) {
        return required instanceof DistributionSpecAny || equals(required);
    }

    @Override
    public String shapeInfo() {
        return getClass().getSimpleName();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        DistributionSpecExternalTableSinkHashPartitioned that =
                (DistributionSpecExternalTableSinkHashPartitioned) other;
        return outputColumnExprIds.equals(that.outputColumnExprIds)
                && getHashAlgorithm() == that.getHashAlgorithm()
                && getWriterAssignment() == that.getWriterAssignment()
                && getPartitionTransforms().equals(that.getPartitionTransforms());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), outputColumnExprIds, getHashAlgorithm(), getWriterAssignment(),
                getPartitionTransforms());
    }

    /** Algorithms shared by FE planning and the external sink exchange protocol. */
    public enum HashAlgorithm {
        DIRECT_HASH,
        ICEBERG_TRANSFORM,
        PAIMON_FIXED_BUCKET,
        PAIMON_HASH_DYNAMIC
    }

    /** Maps logical sink partitions to Doris exchange writers. */
    public enum WriterAssignment {
        IDENTITY,
        SKEWED
    }
}
