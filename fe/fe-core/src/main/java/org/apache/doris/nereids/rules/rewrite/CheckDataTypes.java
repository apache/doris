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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.UnsupportedType;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Check all supported DataTypes
 */
public class CheckDataTypes implements CustomRewriter {

    private static final Set<Class<? extends DataType>> UNSUPPORTED_TYPE = ImmutableSet.of(
            UnsupportedType.class);

    @Override
    public Plan rewriteRoot(Plan rootPlan, JobContext jobContext) {
        checkPlan(rootPlan);
        return rootPlan;
    }

    private void checkPlan(Plan plan) {
        if (plan instanceof LogicalJoin) {
            checkLogicalJoin((LogicalJoin<? extends Plan, ? extends Plan>) plan);
        }
        plan.getExpressions().forEach(ExpressionChecker.INSTANCE::check);
        plan.children().forEach(this::checkPlan);
    }

    private void checkLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> plan) {
        plan.getHashJoinConjuncts().forEach(expr -> {
            DataType leftType = expr.child(0).getDataType();
            DataType rightType = expr.child(1).getDataType();
            if (!leftType.acceptsType(rightType)) {
                throw new AnalysisException(
                        String.format("type %s is not same as %s in hash join condition %s",
                                leftType, rightType, expr.toSql()));
            }
        });
    }

    private static class ExpressionChecker extends DefaultExpressionVisitor<Expression, Void> {
        public static final ExpressionChecker INSTANCE = new ExpressionChecker();

        public void check(Expression expression) {
            expression.accept(this, null);
        }

        public Expression visit(Expression expr, Void unused) {
            try {
                checkTypes(expr.getDataType());
            } catch (UnboundException ignored) {
                return expr;
            }
            expr.children().forEach(child -> child.accept(this, null));
            return expr;
        }

        private void checkTypes(DataType dataType) {
            if (dataType instanceof ArrayType) {
                checkTypes(((ArrayType) dataType).getItemType());
            } else if (dataType instanceof MapType) {
                checkTypes(((MapType) dataType).getKeyType());
                checkTypes(((MapType) dataType).getValueType());
            } else if (dataType instanceof StructType) {
                ((StructType) dataType).getFields().forEach(f -> this.checkTypes(f.getDataType()));
            } else if (UNSUPPORTED_TYPE.contains(dataType.getClass())) {
                throw new AnalysisException(String.format("type %s is unsupported for Nereids", dataType));
            }
        }
    }
}
