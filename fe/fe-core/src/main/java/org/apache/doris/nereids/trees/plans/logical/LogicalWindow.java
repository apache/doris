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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.DataTrait.Builder;
import org.apache.doris.nereids.properties.FdItem;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.window.DenseRank;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Window;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * logical node to deal with window functions;
 */
public class LogicalWindow<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements Window {

    // List<Alias<WindowExpression>>
    private final List<NamedExpression> windowExpressions;

    private final boolean isChecked;

    public LogicalWindow(List<NamedExpression> windowExpressions, CHILD_TYPE child) {
        this(windowExpressions, false, Optional.empty(), Optional.empty(), child);
    }

    public LogicalWindow(List<NamedExpression> windowExpressions, boolean isChecked, CHILD_TYPE child) {
        this(windowExpressions, isChecked, Optional.empty(), Optional.empty(), child);
    }

    public LogicalWindow(List<NamedExpression> windowExpressions, boolean isChecked,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.LOGICAL_WINDOW, groupExpression, logicalProperties, child);
        this.windowExpressions = ImmutableList.copyOf(Objects.requireNonNull(windowExpressions, "output expressions"
                + "in LogicalWindow cannot be null"));
        this.isChecked = isChecked;
    }

    public boolean isChecked() {
        return isChecked;
    }

    @Override
    public List<NamedExpression> getWindowExpressions() {
        return windowExpressions;
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return windowExpressions;
    }

    public LogicalWindow<Plan> withExpressionsAndChild(List<NamedExpression> windowExpressions, Plan child) {
        return new LogicalWindow<>(windowExpressions, isChecked, child);
    }

    public LogicalWindow<Plan> withChecked(List<NamedExpression> windowExpressions, Plan child) {
        return new LogicalWindow<>(windowExpressions, true, Optional.empty(),
                Optional.of(getLogicalProperties()), child);
    }

    @Override
    public LogicalUnary<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalWindow<>(windowExpressions, isChecked, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalWindow(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalWindow<>(windowExpressions, isChecked,
                groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalWindow<>(windowExpressions, isChecked, groupExpression, logicalProperties, children.get(0));
    }

    /**
     * LogicalWindow need to add child().getOutput() as its outputs, to resolve patterns like the following
     * after the implementation rule LogicalWindowToPhysicalWindow:
     * <p>
     * origin:
     * LogicalProject( projects = [row_number as `row_number`, rank as `rank`]
     * +--LogicalWindow( windowExpressions = [row_number() over(order by c1), rank() over(order by c2)]
     * <p>
     * after(not show PhysicalLogicalQuickSort generated by enforcer):
     * PhysicalProject( projects = [row_number as `row_number`, rank as `rank`]
     * +--PhysicalWindow( windowExpressions = [row_number() over(order by c1)])
     * +----PhysicalWindow( windowExpressions = [rank() over(order by c2)])
     * <p>
     * if we don't add child().getOutput(), the top-PhysicalProject cannot find rank()
     */
    @Override
    public List<Slot> computeOutput() {
        return new ImmutableList.Builder<Slot>()
            .addAll(child().getOutput())
            .addAll(windowExpressions.stream()
            .map(NamedExpression::toSlot)
            .collect(ImmutableList.toImmutableList()))
            .build();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalWindow",
                "windowExpressions", windowExpressions,
                "isChecked", isChecked
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalWindow<?> that = (LogicalWindow<?>) o;
        return Objects.equals(windowExpressions, that.windowExpressions)
                && isChecked == that.isChecked;
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowExpressions, isChecked);
    }

    /**
     * Get push down window function candidate and corresponding partition limit.
     *
     * @param filter
     *              For partition topN filter cases, it means the topN filter;
     *              For partition limit cases, it will be null.
     * @param partitionLimit
     *              For partition topN filter cases, it means the filter boundary,
     *                  e.g, 100 for the case rn <= 100;
     *              For partition limit cases, it means the limit.
     * @return
     *              Return null means invalid cases or the opt option is disabled,
     *              else return the chosen window function and the chosen partition limit.
     *              A special limit -1 means the case can be optimized as empty relation.
     */
    public Pair<WindowExpression, Long> getPushDownWindowFuncAndLimit(LogicalFilter<?> filter, long partitionLimit) {
        if (!ConnectContext.get().getSessionVariable().isEnablePartitionTopN()) {
            return null;
        }
        // We have already done such optimization rule, so just ignore it.
        if (child(0) instanceof LogicalPartitionTopN
                || (child(0) instanceof LogicalFilter
                && child(0).child(0) != null
                && child(0).child(0) instanceof LogicalPartitionTopN)) {
            return null;
        }

        // Check the window function. There are some restrictions for window function:
        // 1. The window function should be one of the 'row_number()', 'rank()', 'dense_rank()'.
        // 2. The window frame should be 'UNBOUNDED' to 'CURRENT'.
        // 3. The 'PARTITION' key and 'ORDER' key can not be empty at the same time.
        WindowExpression chosenWindowFunc = null;
        long chosenPartitionLimit = Long.MAX_VALUE;
        long chosenRowNumberPartitionLimit = Long.MAX_VALUE;
        boolean hasRowNumber = false;
        for (NamedExpression windowExpr : windowExpressions) {
            if (windowExpr == null || windowExpr.children().size() != 1
                    || !(windowExpr.child(0) instanceof WindowExpression)) {
                continue;
            }
            WindowExpression windowFunc = (WindowExpression) windowExpr.child(0);

            // Check the window function name.
            if (!(windowFunc.getFunction() instanceof RowNumber
                    || windowFunc.getFunction() instanceof Rank
                    || windowFunc.getFunction() instanceof DenseRank)) {
                continue;
            }

            // Check the partition key and order key.
            if (windowFunc.getPartitionKeys().isEmpty() && windowFunc.getOrderKeys().isEmpty()) {
                continue;
            }

            // Check the window type and window frame.
            Optional<WindowFrame> windowFrame = windowFunc.getWindowFrame();
            if (windowFrame.isPresent()) {
                WindowFrame frame = windowFrame.get();
                if (!(frame.getLeftBoundary().getFrameBoundType() == WindowFrame.FrameBoundType.UNBOUNDED_PRECEDING
                        && frame.getRightBoundary().getFrameBoundType() == WindowFrame.FrameBoundType.CURRENT_ROW)) {
                    continue;
                }
            } else {
                continue;
            }

            // Check filter conditions.
            if (filter != null) {
                // We currently only support simple conditions of the form 'column </ <=/ = constant'.
                // We will extract some related conjuncts and do some check.
                boolean hasPartitionLimit = false;
                long curPartitionLimit = Long.MAX_VALUE;
                Set<Expression> conjuncts = filter.getConjuncts();
                Set<Expression> relatedConjuncts = extractRelatedConjuncts(conjuncts, windowExpr.getExprId());
                for (Expression conjunct : relatedConjuncts) {
                    // Pre-checking has been done in former extraction
                    BinaryOperator op = (BinaryOperator) conjunct;
                    Expression rightChild = op.children().get(1);
                    long limitVal = ((IntegerLikeLiteral) rightChild).getLongValue();
                    // Adjust the value for 'limitVal' based on the comparison operators.
                    if (conjunct instanceof LessThan) {
                        limitVal--;
                    }
                    if (limitVal < 0) {
                        // Set return limit value as -1 for indicating a empty relation opt case
                        chosenPartitionLimit = -1;
                        chosenRowNumberPartitionLimit = -1;
                        break;
                    }
                    if (hasPartitionLimit) {
                        curPartitionLimit = Math.min(curPartitionLimit, limitVal);
                    } else {
                        curPartitionLimit = limitVal;
                        hasPartitionLimit = true;
                    }
                }
                if (chosenPartitionLimit == -1) {
                    chosenWindowFunc = windowFunc;
                    break;
                } else if (windowFunc.getFunction() instanceof RowNumber) {
                    // choose row_number first any way
                    // if multiple exists, choose the one with minimal limit value
                    if (curPartitionLimit < chosenRowNumberPartitionLimit) {
                        chosenRowNumberPartitionLimit = curPartitionLimit;
                        chosenWindowFunc = windowFunc;
                        hasRowNumber = true;
                    }
                } else if (!hasRowNumber) {
                    // if no row_number, choose the one with minimal limit value
                    if (curPartitionLimit < chosenPartitionLimit) {
                        chosenPartitionLimit = curPartitionLimit;
                        chosenWindowFunc = windowFunc;
                    }
                }
            } else {
                // limit
                chosenWindowFunc = windowFunc;
                chosenPartitionLimit = partitionLimit;
                if (windowFunc.getFunction() instanceof RowNumber) {
                    break;
                }
            }
        }
        if (chosenWindowFunc == null || (chosenPartitionLimit == Long.MAX_VALUE
                && chosenRowNumberPartitionLimit == Long.MAX_VALUE)) {
            return null;
        } else {
            return Pair.of(chosenWindowFunc, hasRowNumber ? chosenRowNumberPartitionLimit : chosenPartitionLimit);
        }
    }

    /**
     * pushPartitionLimitThroughWindow is used to push the partitionLimit through the window
     * and generate the partitionTopN. If the window can not meet the requirement,
     * it will return null. So when we use this function, we need check the null in the outside.
     */
    public Plan pushPartitionLimitThroughWindow(WindowExpression windowFunc,
            long partitionLimit, boolean hasGlobalLimit) {
        LogicalWindow<?> window = (LogicalWindow<?>) withChildren(new LogicalPartitionTopN<>(windowFunc, hasGlobalLimit,
                partitionLimit, child(0)));
        return window;
    }

    private Set<Expression> extractRelatedConjuncts(Set<Expression> conjuncts, ExprId slotRefID) {
        Predicate<Expression> condition = conjunct -> {
            if (!(conjunct instanceof BinaryOperator)) {
                return false;
            }
            BinaryOperator op = (BinaryOperator) conjunct;
            Expression leftChild = op.children().get(0);
            Expression rightChild = op.children().get(1);

            if (!(conjunct instanceof LessThan || conjunct instanceof LessThanEqual || conjunct instanceof EqualTo)) {
                return false;
            }

            // TODO: Now, we only support the column on the left side.
            if (!(leftChild instanceof SlotReference) || !(rightChild instanceof IntegerLikeLiteral)) {
                return false;
            }
            return ((SlotReference) leftChild).getExprId() == slotRefID;
        };

        return conjuncts.stream()
                .filter(condition)
                .collect(ImmutableSet.toImmutableSet());
    }

    private boolean isUnique(NamedExpression namedExpression) {
        if (namedExpression.children().size() != 1 || !(namedExpression.child(0) instanceof WindowExpression)) {
            return false;
        }
        WindowExpression windowExpr = (WindowExpression) namedExpression.child(0);
        List<Expression> partitionKeys = windowExpr.getPartitionKeys();
        // Now we only support slot type keys
        if (!partitionKeys.stream().allMatch(Slot.class::isInstance)) {
            return false;
        }
        ImmutableSet<Slot> slotSet = partitionKeys.stream()
                .map(s -> (Slot) s)
                .collect(ImmutableSet.toImmutableSet());
        // if partition by keys are uniform or empty, output is unique
        if (child(0).getLogicalProperties().getTrait().isUniformAndNotNull(slotSet)
                || slotSet.isEmpty()) {
            if (windowExpr.getFunction() instanceof RowNumber) {
                return true;
            }
        }
        return false;
    }

    private boolean isUniform(NamedExpression namedExpression) {
        if (namedExpression.children().size() != 1 || !(namedExpression.child(0) instanceof WindowExpression)) {
            return false;
        }
        WindowExpression windowExpr = (WindowExpression) namedExpression.child(0);
        List<Expression> partitionKeys = windowExpr.getPartitionKeys();
        // Now we only support slot type keys
        if (!partitionKeys.stream().allMatch(Slot.class::isInstance)) {
            return false;
        }
        ImmutableSet<Slot> slotSet = partitionKeys.stream()
                .map(s -> (Slot) s)
                .collect(ImmutableSet.toImmutableSet());
        // if partition by keys are unique, output is uniform
        if (child(0).getLogicalProperties().getTrait().isUniqueAndNotNull(slotSet)) {
            if (windowExpr.getFunction() instanceof RowNumber
                    || windowExpr.getFunction() instanceof Rank
                    || windowExpr.getFunction() instanceof DenseRank) {
                return true;
            }
        }
        return false;
    }

    private void updateFuncDepsByWindowExpr(NamedExpression namedExpression, ImmutableSet.Builder<FdItem> builder) {
        if (namedExpression.children().size() != 1 || !(namedExpression.child(0) instanceof WindowExpression)) {
            return;
        }
        WindowExpression windowExpr = (WindowExpression) namedExpression.child(0);
        List<Expression> partitionKeys = windowExpr.getPartitionKeys();

        // Now we only support slot type keys
        if (!partitionKeys.stream().allMatch(Slot.class::isInstance)) {
            return;
        }
        //ImmutableSet<Slot> slotSet = partitionKeys.stream()
        //        .map(s -> (Slot) s)
        //        .collect(ImmutableSet.toImmutableSet());
        // TODO: if partition by keys are unique, output is uniform
        // TODO: if partition by keys are uniform, output is unique
    }

    @Override
    public void computeUnique(Builder builder) {
        builder.addUniqueSlot(child(0).getLogicalProperties().getTrait());
        for (NamedExpression namedExpression : windowExpressions) {
            if (isUnique(namedExpression)) {
                builder.addUniqueSlot(namedExpression.toSlot());
            }
        }
    }

    @Override
    public void computeUniform(Builder builder) {
        builder.addUniformSlot(child(0).getLogicalProperties().getTrait());
        for (NamedExpression namedExpression : windowExpressions) {
            if (isUniform(namedExpression)) {
                builder.addUniformSlot(namedExpression.toSlot());
            }
        }
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        builder.addEqualSet(child(0).getLogicalProperties().getTrait());
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        builder.addFuncDepsDG(child().getLogicalProperties().getTrait());
    }
}
