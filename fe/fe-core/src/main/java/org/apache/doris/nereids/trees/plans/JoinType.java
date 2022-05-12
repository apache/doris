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

    /**
     * Convert join type in Nereids to legacy join type in Doris.
     *
     * @param joinType join type in Nereids
     * @return legacy join type in Doris
     * @throws AnalysisException throw this exception when input join type cannot convert to legacy join type in Doris
     */
    public static JoinOperator toJoinOperator(JoinType joinType) throws AnalysisException {
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
                throw new AnalysisException("Not support join operator: " + joinType.name());
        }
    }
}
