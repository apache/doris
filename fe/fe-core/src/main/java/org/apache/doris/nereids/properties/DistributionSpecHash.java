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

import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.trees.expressions.ExprId;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;


/**
 * Describe hash distribution.
 */
@Developing
public class DistributionSpecHash extends DistributionSpec {

    private final List<ExprId> shuffledColumns;

    private final ShuffleType shuffleType;

    private final long tableId;

    private final Set<Long> partitionIds;

    public DistributionSpecHash(List<ExprId> shuffledColumns, ShuffleType shuffleType) {
        // Preconditions.checkState(!shuffledColumns.isEmpty());
        this.shuffledColumns = shuffledColumns;
        this.shuffleType = shuffleType;
        this.tableId = -1L;
        this.partitionIds = Collections.emptySet();
    }

    public DistributionSpecHash(List<ExprId> shuffledColumns, ShuffleType shuffleType,
            long tableId, Set<Long> partitionIds) {
        // Preconditions.checkState(!shuffledColumns.isEmpty());
        this.shuffledColumns = shuffledColumns;
        this.shuffleType = shuffleType;
        this.tableId = tableId;
        this.partitionIds = partitionIds;
    }

    public List<ExprId> getShuffledColumns() {
        return shuffledColumns;
    }

    public ShuffleType getShuffleType() {
        return shuffleType;
    }

    public long getTableId() {
        return tableId;
    }

    public Set<Long> getPartitionIds() {
        return partitionIds;
    }

    @Override
    public boolean satisfy(DistributionSpec required) {
        if (required instanceof DistributionSpecAny) {
            return true;
        }

        if (!(required instanceof DistributionSpecHash)) {
            return false;
        }

        DistributionSpecHash requiredHash = (DistributionSpecHash) required;

        if (this.shuffledColumns.size() > requiredHash.shuffledColumns.size()) {
            return false;
        }

        // TODO: need consider following logic whether is right, and maybe need consider more.
        // TODO: consider Agg.
        // Current shuffleType is LOCAL/AGG, allow if current is contained by required
        if (this.shuffleType == ShuffleType.NATURAL && requiredHash.shuffleType == ShuffleType.AGGREGATE) {
            return new HashSet<>(requiredHash.shuffledColumns).containsAll(shuffledColumns);
        }

        if (this.shuffleType == ShuffleType.AGGREGATE && requiredHash.shuffleType == ShuffleType.JOIN) {
            return this.shuffledColumns.size() == requiredHash.shuffledColumns.size()
                    && this.shuffledColumns.equals(requiredHash.shuffledColumns);
        } else if (this.shuffleType == ShuffleType.JOIN && requiredHash.shuffleType == ShuffleType.AGGREGATE) {
            return new HashSet<>(requiredHash.shuffledColumns).containsAll(shuffledColumns);
        }

        if (!this.shuffleType.equals(requiredHash.shuffleType)) {
            return false;
        }

        return this.shuffledColumns.size() == requiredHash.shuffledColumns.size()
                && this.shuffledColumns.equals(requiredHash.shuffledColumns);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        DistributionSpecHash that = (DistributionSpecHash) o;
        return shuffledColumns.equals(that.shuffledColumns)
                && shuffleType.equals(that.shuffleType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shuffledColumns, shuffleType);
    }

    /**
     * Enums for concrete shuffle type.
     */
    public enum ShuffleType {
        // for olap scan node and colocate join
        NATURAL,
        // for easy to translate to bucket shuffle join
        BUCKET,
        // for shuffle to Aggregate node
        AGGREGATE,
        // for Shuffle to Join node
        JOIN,
        // for add distribute node Explicitly
        ENFORCE
    }
}
