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

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.window.DenseRank;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Window;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

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

    public LogicalWindow<Plan> withExpression(List<NamedExpression> windowExpressions, Plan child) {
        return new LogicalWindow<>(windowExpressions, isChecked, Optional.empty(),
                Optional.empty(), child);
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
     * pushPartitionLimitThroughWindow is used to push the partitionLimit through the window
     * and generate the partitionTopN. If the window can not meet the requirement,
     * it will return null. So when we use this function, we need check the null in the outside.
     */
    public Optional<Plan> pushPartitionLimitThroughWindow(long partitionLimit, boolean hasGlobalLimit) {
        if (!ConnectContext.get().getSessionVariable().isEnablePartitionTopN()) {
            return Optional.empty();
        }
        // We have already done such optimization rule, so just ignore it.
        if (child(0) instanceof LogicalPartitionTopN) {
            return Optional.empty();
        }

        // Check the window function. There are some restrictions for window function:
        // 1. The number of window function should be 1.
        // 2. The window function should be one of the 'row_number()', 'rank()', 'dense_rank()'.
        // 3. The window frame should be 'UNBOUNDED' to 'CURRENT'.
        // 4. The 'PARTITION' key and 'ORDER' key can not be empty at the same time.
        if (windowExpressions.size() != 1) {
            return Optional.empty();
        }
        NamedExpression windowExpr = windowExpressions.get(0);
        if (windowExpr.children().size() != 1 || !(windowExpr.child(0) instanceof WindowExpression)) {
            return Optional.empty();
        }

        WindowExpression windowFunc = (WindowExpression) windowExpr.child(0);
        // Check the window function name.
        if (!(windowFunc.getFunction() instanceof RowNumber
                || windowFunc.getFunction() instanceof Rank
                || windowFunc.getFunction() instanceof DenseRank)) {
            return Optional.empty();
        }

        // Check the partition key and order key.
        if (windowFunc.getPartitionKeys().isEmpty() && windowFunc.getOrderKeys().isEmpty()) {
            return Optional.empty();
        }

        // Check the window type and window frame.
        Optional<WindowFrame> windowFrame = windowFunc.getWindowFrame();
        if (windowFrame.isPresent()) {
            WindowFrame frame = windowFrame.get();
            if (!(frame.getLeftBoundary().getFrameBoundType() == WindowFrame.FrameBoundType.UNBOUNDED_PRECEDING
                    && frame.getRightBoundary().getFrameBoundType() == WindowFrame.FrameBoundType.CURRENT_ROW)) {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }

        LogicalWindow<?> window = (LogicalWindow<?>) withChildren(new LogicalPartitionTopN<>(windowFunc, hasGlobalLimit,
                partitionLimit, child(0)));

        return Optional.ofNullable(window);
    }
}
