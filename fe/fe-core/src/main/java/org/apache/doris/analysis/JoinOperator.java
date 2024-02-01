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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/JoinOperator.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.thrift.TJoinOp;

public enum JoinOperator {
    INNER_JOIN("INNER JOIN", TJoinOp.INNER_JOIN),
    LEFT_OUTER_JOIN("LEFT OUTER JOIN", TJoinOp.LEFT_OUTER_JOIN),

    LEFT_SEMI_JOIN("LEFT SEMI JOIN", TJoinOp.LEFT_SEMI_JOIN),
    LEFT_ANTI_JOIN("LEFT ANTI JOIN", TJoinOp.LEFT_ANTI_JOIN),
    RIGHT_SEMI_JOIN("RIGHT SEMI JOIN", TJoinOp.RIGHT_SEMI_JOIN),
    RIGHT_ANTI_JOIN("RIGHT ANTI JOIN", TJoinOp.RIGHT_ANTI_JOIN),
    RIGHT_OUTER_JOIN("RIGHT OUTER JOIN", TJoinOp.RIGHT_OUTER_JOIN),
    FULL_OUTER_JOIN("FULL OUTER JOIN", TJoinOp.FULL_OUTER_JOIN),
    CROSS_JOIN("CROSS JOIN", TJoinOp.CROSS_JOIN),
    // Variant of the LEFT ANTI JOIN that is used for the equal of
    // NOT IN subqueries. It can have a single equality join conjunct
    // that returns TRUE when the rhs is NULL.
    NULL_AWARE_LEFT_ANTI_JOIN("NULL AWARE LEFT ANTI JOIN",
            TJoinOp.NULL_AWARE_LEFT_ANTI_JOIN),
    NULL_AWARE_LEFT_SEMI_JOIN("NULL AWARE LEFT SEMI JOIN",
            TJoinOp.NULL_AWARE_LEFT_SEMI_JOIN);

    private final String  description;
    private final TJoinOp thriftJoinOp;

    JoinOperator(String description, TJoinOp thriftJoinOp) {
        this.description = description;
        this.thriftJoinOp = thriftJoinOp;
    }

    @Override
    public String toString() {
        return description;
    }

    public TJoinOp toThrift() {
        return thriftJoinOp;
    }

    public boolean isOuterJoin() {
        return this == LEFT_OUTER_JOIN || this == RIGHT_OUTER_JOIN || this == FULL_OUTER_JOIN;
    }

    public boolean isSemiAntiJoin() {
        return this == LEFT_SEMI_JOIN || this == RIGHT_SEMI_JOIN || this == LEFT_ANTI_JOIN
                || this == NULL_AWARE_LEFT_ANTI_JOIN || this == RIGHT_ANTI_JOIN;
    }

    public boolean isSemiJoin() {
        return this == JoinOperator.LEFT_SEMI_JOIN || this == JoinOperator.LEFT_ANTI_JOIN
                || this == JoinOperator.RIGHT_SEMI_JOIN || this == JoinOperator.RIGHT_ANTI_JOIN
                || this == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN;
    }

    public boolean isSemiOrAntiJoinNoNullAware() {
        return this == JoinOperator.LEFT_SEMI_JOIN || this == JoinOperator.LEFT_ANTI_JOIN
                || this == JoinOperator.RIGHT_SEMI_JOIN || this == JoinOperator.RIGHT_ANTI_JOIN;
    }

    public boolean isAntiJoinNullAware() {
        return this == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN;
    }

    public boolean isAntiJoinNoNullAware() {
        return this == JoinOperator.LEFT_ANTI_JOIN || this == JoinOperator.RIGHT_ANTI_JOIN;
    }

    public boolean isLeftSemiJoin() {
        return this.thriftJoinOp == TJoinOp.LEFT_SEMI_JOIN;
    }

    public boolean isInnerJoin() {
        return this.thriftJoinOp == TJoinOp.INNER_JOIN;
    }

    public boolean isAntiJoin() {
        return this == JoinOperator.LEFT_ANTI_JOIN || this == JoinOperator.RIGHT_ANTI_JOIN
                || this == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN;
    }

    public boolean supportMarkJoin() {
        return this == JoinOperator.LEFT_ANTI_JOIN || this == JoinOperator.LEFT_SEMI_JOIN
                || this == JoinOperator.CROSS_JOIN || this == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN;
    }

    public boolean isCrossJoin() {
        return this == CROSS_JOIN;
    }

    public boolean isFullOuterJoin() {
        return this == FULL_OUTER_JOIN;
    }

    public boolean isLeftOuterJoin() {
        return this == LEFT_OUTER_JOIN;
    }

    public boolean isRightOuterJoin() {
        return this == RIGHT_OUTER_JOIN;
    }
}
