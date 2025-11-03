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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.FunctionTrait;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;

/** SequenceFunction */
public interface SequenceFunction extends FunctionTrait {
    @Override
    default void checkLegalityBeforeTypeCoercion() {
        String functionName = getName();
        Expression firstArg = getArgument(0);
        if (!(firstArg instanceof StringLikeLiteral)) {
            throw new AnalysisException("The pattern param `" + firstArg.toSql() + "` of " + functionName
                    + " function must be string literal, but it is "
                    + firstArg.getClass().getSimpleName());
        }
        if (!getArgumentType(1).isDateLikeType()) {
            throw new AnalysisException("The timestamp params of " + functionName
                    + " function must be DATE or DATETIME, but it is " + getArgumentType(1));
        }
        String pattern = ((StringLikeLiteral) firstArg).getStringValue();
        if (!FunctionCallExpr.parsePattern(pattern)) {
            throw new AnalysisException("The format of pattern params is wrong: " + this.toSql());
        }

        for (int i = 2; i < arity(); i++) {
            if (!getArgumentType(i).isBooleanType()) {
                throw new AnalysisException("The param `" + child(i).toSql() + "` of "
                        + functionName + " function must be boolean, but it is "
                        + getArgumentType(i).getClass().getSimpleName());
            }
        }
    }
}
