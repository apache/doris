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

package org.apache.doris.nereids.trees.plans.algebra;

import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.DistributeType.JoinDistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Common interface for logical/physical join.
 */
public interface Join {
    /**
     * join shuffle type
     */
    enum ShuffleType {
        shuffle,
        broadcast,
        bucketShuffle,
        shuffleBucket,
        colocated,
        unknown
    }

    JoinType getJoinType();

    List<Expression> getHashJoinConjuncts();

    default List<EqualTo> getEqualToConjuncts() {
        return getHashJoinConjuncts().stream().filter(EqualTo.class::isInstance).map(EqualTo.class::cast)
                .collect(Collectors.toList());
    }

    default List<EqualPredicate> getEqualPredicates() {
        return Stream.concat(getHashJoinConjuncts().stream(), getMarkJoinConjuncts().stream())
                .filter(EqualPredicate.class::isInstance).map(EqualPredicate.class::cast)
                .collect(Collectors.toList());
    }

    List<Expression> getOtherJoinConjuncts();

    List<Expression> getMarkJoinConjuncts();

    Optional<Expression> getOnClauseCondition();

    DistributeHint getDistributeHint();

    boolean isMarkJoin();

    Optional<MarkJoinSlotReference> getMarkJoinSlotReference();

    default boolean hasDistributeHint() {
        return getDistributeHint().distributeType != DistributeType.NONE;
    }

    default JoinDistributeType getLeftHint() {
        return JoinDistributeType.NONE;
    }

    /**
     * Get the hint type of join's right child.
     */
    default JoinDistributeType getRightHint() {
        switch (getDistributeHint().distributeType) {
            case SHUFFLE_RIGHT:
                return JoinDistributeType.SHUFFLE;
            case BROADCAST_RIGHT:
                return JoinDistributeType.BROADCAST;
            default:
                return JoinDistributeType.NONE;
        }
    }
}
