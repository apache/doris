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

package org.apache.doris.nereids.trees.plans.visitor;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Infer output column name when it refers an expression and not has an alias manually.
 */
public class InferPlanOutputAlias {

    public static final Logger LOG = LogManager.getLogger(InferPlanOutputAlias.class);

    private final List<Slot> currentOutputs;
    private final List<NamedExpression> finalOutputs;
    private final Set<Integer> shouldProcessOutputIndex;

    /** InferPlanOutputAlias */
    public InferPlanOutputAlias(List<Slot> currentOutputs) {
        this.currentOutputs = currentOutputs;
        this.finalOutputs = new ArrayList<>(currentOutputs);
        this.shouldProcessOutputIndex = new HashSet<>();
        for (int i = 0; i < currentOutputs.size(); i++) {
            shouldProcessOutputIndex.add(i);
        }
    }

    /** infer */
    public List<NamedExpression> infer(Plan plan, ImmutableMultimap<ExprId, Integer> currentExprIdAndIndexMap) {
        ImmutableSet<ExprId> currentOutputExprIdSet = currentExprIdAndIndexMap.keySet();
        final Map<ExprId, ExprId> childToParentMapping = new HashMap<>();
        // Breath First Search
        plan.foreachBreath(childPlan -> {
            if (shouldProcessOutputIndex.isEmpty()) {
                return true;
            }
            if (childPlan instanceof LogicalSetOperation) {
                // Such as LogicalUnion(finalOutputs=['back'#41, 'we'#42], regularChildrenOutputs=
                // [['back'#38, 'we'#40],['back'#36, 'we'#37])
                //         -- LogicalUnion(finalOutputs=['back'#38, 'we'#40], regularChildrenOutputs=
                //         [['back'#35, 'we'#36], ['back'#33, 'we'#34]])
                //           -- LogicalProject(finalOutputs=['back'#35, 'we'#36])
                //           -- LogicalProject(finalOutputs=['back'#33, 'we'#34])
                List<SlotReference> regularChildOutputs = ((LogicalSetOperation) childPlan).getRegularChildOutput(0);
                List<NamedExpression> currentOutputs = ((LogicalSetOperation) childPlan).getOutputs();
                if (regularChildOutputs.size() != currentOutputs.size()) {
                    // set current output size is different from currentOutputs, not excepted
                    LOG.error("InferPlanOutputAlias infer regularChildOutputs is different from currentOutputs,"
                            + "child plan is {}", ((LogicalSetOperation) childPlan).treeString());
                    return true;
                }
                if (childToParentMapping.isEmpty()) {
                    // if multi union, this handle first set only
                    // childToParentMapping is {{back'#38 : back'#41},{'we'#40 : 'we'#42}}
                    for (int index = 0; index < regularChildOutputs.size(); index++) {
                        childToParentMapping.put(regularChildOutputs.get(index).getExprId(),
                                currentOutputs.get(index).getExprId());
                    }
                } else {
                    // this handle child set
                    Map<ExprId, ExprId> tmpChildToParentMapping = Maps.newHashMap(childToParentMapping);
                    childToParentMapping.clear();
                    // childToParentMapping is {{back'#35 : back'#41},{'we'#36 : 'we'#42}}
                    for (int index = 0; index < regularChildOutputs.size(); index++) {
                        childToParentMapping.put(regularChildOutputs.get(index).getExprId(),
                                tmpChildToParentMapping.get(currentOutputs.get(index).getExprId()));
                    }
                }
            }
            for (Expression expression : ((Plan) childPlan).getExpressions()) {
                if (!(expression instanceof Alias)) {
                    continue;
                }
                Alias projectItem = (Alias) expression;
                ExprId exprId = projectItem.getExprId();
                // Infer name when alias child is expression and alias's name is from child
                if (contains(currentOutputExprIdSet, exprId, childToParentMapping)
                        && (projectItem.isNameFromChild())) {
                    String inferredAliasName = projectItem.child().getExpressionName();
                    ImmutableCollection<Integer> outputExprIndexes = getOutputSlotIndex(currentExprIdAndIndexMap,
                            exprId, childToParentMapping);
                    // replace output name by inferred name
                    for (Integer index : outputExprIndexes) {
                        Slot slot = currentOutputs.get(index);
                        finalOutputs.set(index, slot.withName("__" + inferredAliasName + "_" + index));
                        shouldProcessOutputIndex.remove(index);

                        if (shouldProcessOutputIndex.isEmpty()) {
                            // replace finished
                            return true;
                        }
                    }
                }
            }
            // continue replace
            return false;
        });
        return finalOutputs;
    }

    /**
     * Such as LogicalIntersect, targetExprIdSet is ['back'#41, 'we'#42], but child is ['back'#38, 'we'#40]
     * should construct childToParentMapping {
     *     'back'#38 : 'back'#41,
     *     'we'#40 : 'we'#42
     * }
     * LogicalIntersect ( qualifier=DISTINCT,
     * outputs=['back'#41, 'we'#42],
     * regularChildrenOutputs=[['back'#38, 'we'#40],
     * [col_char_25__undef_signed#39, col_varchar_25__undef_signed_not_null#27]] )
     */
    private static boolean contains(ImmutableSet<ExprId> targetExprIdSet, ExprId exprId,
            Map<ExprId, ExprId> childToParentMapping) {
        if (targetExprIdSet.contains(exprId)) {
            return true;
        }
        return targetExprIdSet.contains(childToParentMapping.get(exprId));
    }

    /**
     * Such as LogicalIntersect, currentExprIdAndIndexMap is
     * {
     *     'back'#38 : [0],
     *     'we'#40 : [1]
     * }
     * LogicalIntersect ( qualifier=DISTINCT,
     * outputs=['back'#41, 'we'#42],
     * regularChildrenOutputs=[['back'#38, 'we'#40],
     * [col_char_25__undef_signed#39, col_varchar_25__undef_signed_not_null#27]] )
     * exprId is 'back'#38
     * childToParentMapping {
     *     'back'#38 : 'back'#41,
     *     'we'#40 : 'we'#42
     * }
     */
    private static ImmutableCollection<Integer> getOutputSlotIndex(
            ImmutableMultimap<ExprId, Integer> currentExprIdAndIndexMap,
            ExprId exprId,
            Map<ExprId, ExprId> childToParentMapping) {
        ImmutableCollection<Integer> indexes = currentExprIdAndIndexMap.get(exprId);
        if (!indexes.isEmpty()) {
            return indexes;
        }
        return currentExprIdAndIndexMap.get(childToParentMapping.get(exprId));
    }
}
