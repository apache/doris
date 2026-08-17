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

import java.util.List;

/** Hash MaxCompute partition values using the legacy Hive ScaleWriter behavior. */
public final class DistributionSpecMaxComputeTableSinkHashPartitioned
        extends DistributionSpecExternalTableSinkHashPartitioned {

    /** Create a MaxCompute ownership distribution from dynamic partition columns. */
    public DistributionSpecMaxComputeTableSinkHashPartitioned(List<ExprId> outputColumnExprIds) {
        super(outputColumnExprIds);
    }

    @Override
    public HashAlgorithm getHashAlgorithm() {
        return HashAlgorithm.DIRECT_HASH;
    }

    @Override
    public WriterAssignment getWriterAssignment() {
        // Keep the pre-refactoring behavior for external formats that have not yet defined
        // their own writer-ownership contract. Only Hive and Iceberg are modeled explicitly
        // in this phase.
        return WriterAssignment.SKEWED;
    }
}
