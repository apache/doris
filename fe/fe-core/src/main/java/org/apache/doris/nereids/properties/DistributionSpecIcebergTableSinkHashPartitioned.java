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

/** Hash final Iceberg partition values to a unique external table sink writer. */
public final class DistributionSpecIcebergTableSinkHashPartitioned
        extends DistributionSpecExternalTableSinkHashPartitioned {

    private final ImmutableList<String> partitionTransforms;

    /** Create an Iceberg ownership distribution from source columns and positional transforms. */
    public DistributionSpecIcebergTableSinkHashPartitioned(
            List<ExprId> outputColumnExprIds, List<String> partitionTransforms) {
        super(outputColumnExprIds);
        this.partitionTransforms = ImmutableList.copyOf(
                Objects.requireNonNull(partitionTransforms, "partitionTransforms should not be null"));
        if (this.partitionTransforms.size() != outputColumnExprIds.size()) {
            throw new IllegalArgumentException(
                    "partitionTransforms must match outputColumnExprIds");
        }
    }

    @Override
    public HashAlgorithm getHashAlgorithm() {
        return HashAlgorithm.ICEBERG_TRANSFORM;
    }

    @Override
    public List<String> getPartitionTransforms() {
        return partitionTransforms;
    }
}
