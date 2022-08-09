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
import org.apache.doris.nereids.trees.expressions.SlotReference;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;


/**
 * Describe hash distribution.
 */
@Developing
public class DistributionSpecHash extends DistributionSpec {

    private final List<SlotReference> shuffledColumns;

    private final ShuffleType shuffleType;

    public DistributionSpecHash(List<SlotReference> shuffledColumns, ShuffleType shuffleType) {
        // Preconditions.checkState(!shuffledColumns.isEmpty());
        this.shuffledColumns = shuffledColumns;
        this.shuffleType = shuffleType;
    }

    public List<SlotReference> getShuffledColumns() {
        return shuffledColumns;
    }

    public ShuffleType getShuffleType() {
        return shuffleType;
    }

    @Override
    public boolean satisfy(DistributionSpec other) {
        if (other instanceof DistributionSpecAny) {
            return true;
        }

        if (!(other instanceof DistributionSpecHash)) {
            return false;
        }

        DistributionSpecHash spec = (DistributionSpecHash) other;

        if (shuffledColumns.size() > spec.shuffledColumns.size()) {
            return false;
        }

        // TODO: need consider following logic whether is right, and maybe need consider more.
        // TODO: consider Agg.
        // Current shuffleType is LOCAL/AGG, allow if current is contained by other
        if (shuffleType == ShuffleType.LOCAL || spec.shuffleType == ShuffleType.AGG) {
            return new HashSet<>(spec.shuffledColumns).containsAll(shuffledColumns);
        }

        if (shuffleType == ShuffleType.AGG && spec.shuffleType == ShuffleType.JOIN) {
            return shuffledColumns.size() == spec.shuffledColumns.size()
                    && shuffledColumns.equals(spec.shuffledColumns);
        } else if (shuffleType == ShuffleType.JOIN && spec.shuffleType == ShuffleType.AGG) {
            return new HashSet<>(spec.shuffledColumns).containsAll(shuffledColumns);
        }

        if (!shuffleType.equals(spec.shuffleType)) {
            return false;
        }

        return shuffledColumns.size() == spec.shuffledColumns.size()
                && shuffledColumns.equals(spec.shuffledColumns);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        DistributionSpecHash that = (DistributionSpecHash) o;
        return shuffledColumns.equals(that.shuffledColumns)
                && shuffleType.equals(that.shuffleType);
        // && propertyInfo.equals(that.propertyInfo)
    }

    @Override
    public int hashCode() {
        return Objects.hash(shuffledColumns, shuffleType);
    }

    /**
     * Enums for concrete shuffle type.
     */
    public enum ShuffleType {
        LOCAL,
        BUCKET,
        // Shuffle Aggregation
        AGG,
        // Shuffle Join
        JOIN,
        ENFORCE
    }
}
