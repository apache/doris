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

package org.apache.doris.datasource;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** Immutable result of the generic external-table writer parallelism policy. */
public final class ExternalWriterParallelism {
    private final int plannedWriterCount;
    private final Long estimatedOwnershipCount;
    private final String fallbackReason;

    public ExternalWriterParallelism(int plannedWriterCount,
            Long estimatedOwnershipCount, String fallbackReason) {
        if (plannedWriterCount <= 0) {
            throw new IllegalArgumentException("plannedWriterCount must be positive");
        }
        if (estimatedOwnershipCount != null && estimatedOwnershipCount <= 0) {
            throw new IllegalArgumentException("estimatedOwnershipCount must be positive");
        }
        this.plannedWriterCount = plannedWriterCount;
        this.estimatedOwnershipCount = estimatedOwnershipCount;
        this.fallbackReason = fallbackReason;
    }

    public int getPlannedWriterCount() {
        return plannedWriterCount;
    }

    public OptionalLong getEstimatedOwnershipCount() {
        return estimatedOwnershipCount == null
                ? OptionalLong.empty() : OptionalLong.of(estimatedOwnershipCount);
    }

    public Optional<String> getFallbackReason() {
        return Optional.ofNullable(fallbackReason);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ExternalWriterParallelism)) {
            return false;
        }
        ExternalWriterParallelism that = (ExternalWriterParallelism) other;
        return plannedWriterCount == that.plannedWriterCount
                && Objects.equals(estimatedOwnershipCount, that.estimatedOwnershipCount)
                && Objects.equals(fallbackReason, that.fallbackReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(plannedWriterCount, estimatedOwnershipCount, fallbackReason);
    }
}
