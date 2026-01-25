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
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.generator.Unnest;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StructElement;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Before:
 *          project(unnest(x) as a, unnest(y) as b)
 *
 * After:
 *
 *          project(struct_element($c$1, col1) as a, struct_element($c$1, co2) as b)
 *             │
 *             ▼
 *          generate(unnest(x, y) as $c$1)
 */
public class PushDownUnnestInProject extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalProject().when(project -> containUnnestFunction(project.getProjects())).then(project -> {
            List<NamedExpression> outputs = project.getProjects();
            Set<Alias> existedAlias = ExpressionUtils.collect(outputs, Alias.class::isInstance);
            List<Unnest> toBePushedDown = ExpressionUtils.collectToList(outputs, Unnest.class::isInstance);
            Function newFunction = ExpressionUtils.convertUnnest(validateAndMergeUnnest(toBePushedDown));
            NormalizeToSlot.NormalizeToSlotContext context =
                    NormalizeToSlot.NormalizeToSlotContext.buildContext(existedAlias, toBePushedDown);
            String columnName = ConnectContext.get() != null
                    ? ConnectContext.get().getStatementContext().generateColumnName() : "expand_cols";
            SlotReference outputSlot = new SlotReference("unnest_temp_table", newFunction.getDataType(),
                    newFunction.nullable(), ImmutableList.of(columnName));
            List<Alias> newProjects = new ArrayList<>(toBePushedDown.size());
            Map<Expression, NormalizeToSlot.NormalizeToSlotTriplet> slotTripletMap = context.getNormalizeToSlotMap();
            if (toBePushedDown.size() > 1) {
                // struct_element(#expand_col#k, #k) as #k
                // struct_element(#expand_col#v, #v) as #v
                List<StructField> fields = ((StructType) outputSlot.getDataType()).getFields();
                Preconditions.checkState(fields.size() == toBePushedDown.size(), String.format("push down"
                                + "unnest function has error, function count is %d, pushed down count is %d",
                        toBePushedDown.size(), fields.size()));
                for (int i = 0; i < fields.size(); ++i) {
                    Slot remainExpr = slotTripletMap.get(toBePushedDown.get(i)).remainExpr;
                    newProjects.add(new Alias(remainExpr.getExprId(), new StructElement(
                            outputSlot, new StringLiteral(fields.get(i).getName())), remainExpr.getName()));
                }
            } else {
                Slot remainExpr = slotTripletMap.get(toBePushedDown.get(0)).remainExpr;
                newProjects.add(new Alias(remainExpr.getExprId(), outputSlot, remainExpr.getName()));
            }
            Map<Slot, Expression> replaceMap = ExpressionUtils.generateReplaceMap(newProjects);
            List<NamedExpression> newOutputs = context.normalizeToUseSlotRef(outputs);
            return project.withProjectsAndChild(ExpressionUtils.replaceNamedExpressions(newOutputs, replaceMap),
                    new LogicalGenerate(ImmutableList.of(newFunction), ImmutableList.of(outputSlot), project.child()));
        }).toRule(RuleType.PUSH_DOWN_UNNEST_IN_PROJECT);
    }

    private boolean containUnnestFunction(List<NamedExpression> expressions) {
        for (NamedExpression expr : expressions) {
            if (expr.containsType(Unnest.class)) {
                return true;
            }
        }
        return false;
    }

    private Unnest validateAndMergeUnnest(List<Unnest> functions) {
        // TODO: PG only support ARRAY type, and explode_map and explode_bitmap only support 1 argument in doris
        // so we only allow array for now, too
        int typeCounter = 0;
        int size = functions.size();
        List<Expression> expressions = new ArrayList<>(size);
        for (Unnest unnest : functions) {
            if (unnest.children().size() > 1) {
                throw new AnalysisException("UNNEST function can have more than one arguments only in from clause");
            }
            Expression expr = unnest.child(0);
            DataType dataType = expr.getDataType();
            // we only care about generic type info are same
            // so ARRAY<INT>, ARRAY<DOUBLE> will be treated as same type
            // we use 1, 2, 3 as some mark value for array, map and bitmap type separately
            if (dataType.isArrayType()) {
                typeCounter += 1;
            } else if (dataType.isMapType()) {
                typeCounter += 2;
            } else if (dataType.isBitmapType()) {
                typeCounter += 3;
            }
            expressions.add(expr);
        }
        // TODO: remove this after doris support multiple arguments for explode_map and explode_bitmap
        if (expressions.size() > 1 && typeCounter != 1 * size) {
            throw new AnalysisException("multiple UNNEST functions in same place must have ARRAY argument type");
        }
        if (typeCounter == 1 * size || typeCounter == 2 * size || typeCounter == 3 * size) {
            return new Unnest(expressions, false, false);
        } else {
            throw new AnalysisException("UNNEST functions must have same argument type");
        }
    }
}
