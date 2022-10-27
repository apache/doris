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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * All job type in Nereids.
 */
public enum JoinType {
    INNER_JOIN,
    LEFT_OUTER_JOIN,
    RIGHT_OUTER_JOIN,
    FULL_OUTER_JOIN,
    LEFT_SEMI_JOIN,
    RIGHT_SEMI_JOIN,
    LEFT_ANTI_JOIN,
    RIGHT_ANTI_JOIN,
    CROSS_JOIN,
    ;

    private static final Map<JoinType, JoinType> joinSwapMap = ImmutableMap
            .<JoinType, JoinType>builder()
            .put(INNER_JOIN, INNER_JOIN)
            .put(CROSS_JOIN, CROSS_JOIN)
            .put(FULL_OUTER_JOIN, FULL_OUTER_JOIN)
            .put(LEFT_SEMI_JOIN, RIGHT_SEMI_JOIN)
            .put(RIGHT_SEMI_JOIN, LEFT_SEMI_JOIN)
            .put(LEFT_OUTER_JOIN, RIGHT_OUTER_JOIN)
            .put(RIGHT_OUTER_JOIN, LEFT_OUTER_JOIN)
            .put(LEFT_ANTI_JOIN, RIGHT_ANTI_JOIN)
            .put(RIGHT_ANTI_JOIN, LEFT_ANTI_JOIN)
            .build();

    /**
     * Convert join type in Nereids to legacy join type in Doris.
     *
     * @param joinType join type in Nereids
     * @return legacy join type in Doris
     * @throws AnalysisException throw this exception when input join type cannot convert to legacy join type in Doris
     */
    public static JoinOperator toJoinOperator(JoinType joinType) {
        switch (joinType) {
            case INNER_JOIN:
                return JoinOperator.INNER_JOIN;
            case LEFT_OUTER_JOIN:
                return JoinOperator.LEFT_OUTER_JOIN;
            case RIGHT_OUTER_JOIN:
                return JoinOperator.RIGHT_OUTER_JOIN;
            case FULL_OUTER_JOIN:
                return JoinOperator.FULL_OUTER_JOIN;
            case LEFT_ANTI_JOIN:
                return JoinOperator.LEFT_ANTI_JOIN;
            case RIGHT_ANTI_JOIN:
                return JoinOperator.RIGHT_ANTI_JOIN;
            case LEFT_SEMI_JOIN:
                return JoinOperator.LEFT_SEMI_JOIN;
            case RIGHT_SEMI_JOIN:
                return JoinOperator.RIGHT_SEMI_JOIN;
            case CROSS_JOIN:
                return JoinOperator.CROSS_JOIN;
            default:
                throw new RuntimeException("Unexpected join operator: " + joinType.name());
        }
    }

    public final boolean isCrossJoin() {
        return this == CROSS_JOIN;
    }

    public final boolean isInnerJoin() {
        return this == INNER_JOIN;
    }

    public final boolean isInnerOrCrossJoin() {
        return this == INNER_JOIN || this == CROSS_JOIN;
    }

    public final boolean isLeftJoin() {
        return this == LEFT_OUTER_JOIN || this == LEFT_ANTI_JOIN || this == LEFT_SEMI_JOIN;
    }

    public final boolean isRightJoin() {
        return this == RIGHT_OUTER_JOIN || this == RIGHT_ANTI_JOIN || this == RIGHT_SEMI_JOIN;
    }

    public final boolean isFullOuterJoin() {
        return this == FULL_OUTER_JOIN;
    }

    public final boolean isLeftOuterJoin() {
        return this == LEFT_OUTER_JOIN;
    }

    public final boolean isRightOuterJoin() {
        return this == RIGHT_OUTER_JOIN;
    }

    public final boolean isLeftSemiOrAntiJoin() {
        return this == LEFT_SEMI_JOIN || this == LEFT_ANTI_JOIN;
    }

    public final boolean isRightSemiOrAntiJoin() {
        return this == RIGHT_SEMI_JOIN || this == RIGHT_ANTI_JOIN;
    }

    public final boolean isSemiOrAntiJoin() {
        return this == LEFT_SEMI_JOIN || this == RIGHT_SEMI_JOIN || this == LEFT_ANTI_JOIN || this == RIGHT_ANTI_JOIN;
    }

    public final boolean isOuterJoin() {
        return this == LEFT_OUTER_JOIN || this == RIGHT_OUTER_JOIN || this == FULL_OUTER_JOIN;
    }

    public final boolean isRemainLeftJoin() {
        return this != RIGHT_SEMI_JOIN && this != RIGHT_ANTI_JOIN;
    }

    public final boolean isRemainRightJoin() {
        return this != LEFT_SEMI_JOIN && this != LEFT_ANTI_JOIN;
    }

    public final boolean isSwapJoinType() {
        return joinSwapMap.containsKey(this);
    }

    public JoinType swap() {
        return joinSwapMap.get(this);
    }
}
