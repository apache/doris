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

import org.apache.doris.connector.spi.write.ConnectorWriteDistribution.WriterAssignment;
import org.apache.doris.nereids.trees.expressions.ExprId;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Opaque connector partition-function request for an external table sink. */
public final class DistributionSpecExternalTableSinkHashPartitioned extends DistributionSpec {

    public static final int MIN_BE_EXEC_VERSION = 13;

    private final ImmutableList<ExprId> outputColumnExprIds;
    private final String partitionFunction;
    private final ImmutableMap<String, String> partitionFunctionOptions;
    private final WriterAssignment writerAssignment;

    public DistributionSpecExternalTableSinkHashPartitioned(List<ExprId> outputColumnExprIds,
            String partitionFunction, Map<String, String> partitionFunctionOptions,
            WriterAssignment writerAssignment) {
        this.outputColumnExprIds = ImmutableList.copyOf(outputColumnExprIds);
        this.partitionFunction = Objects.requireNonNull(partitionFunction);
        this.partitionFunctionOptions = ImmutableMap.copyOf(partitionFunctionOptions);
        this.writerAssignment = Objects.requireNonNull(writerAssignment);
    }

    public List<ExprId> getOutputColumnExprIds() {
        return outputColumnExprIds;
    }

    public String getPartitionFunction() {
        return partitionFunction;
    }

    public Map<String, String> getPartitionFunctionOptions() {
        return partitionFunctionOptions;
    }

    public WriterAssignment getWriterAssignment() {
        return writerAssignment;
    }

    @Override
    public boolean satisfy(DistributionSpec required) {
        return required instanceof DistributionSpecAny || equals(required);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DistributionSpecExternalTableSinkHashPartitioned)) {
            return false;
        }
        DistributionSpecExternalTableSinkHashPartitioned that
                = (DistributionSpecExternalTableSinkHashPartitioned) other;
        return outputColumnExprIds.equals(that.outputColumnExprIds)
                && partitionFunction.equals(that.partitionFunction)
                && partitionFunctionOptions.equals(that.partitionFunctionOptions)
                && writerAssignment == that.writerAssignment;
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputColumnExprIds, partitionFunction,
                partitionFunctionOptions, writerAssignment);
    }
}
