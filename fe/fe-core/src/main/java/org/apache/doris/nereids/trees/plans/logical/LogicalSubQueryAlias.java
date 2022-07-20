package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class LogicalSubQueryAlias<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> {
    private final String tableAlias;

    public LogicalSubQueryAlias(String tableAlias, CHILD_TYPE child) {
        this(tableAlias, Optional.empty(), Optional.empty(), child);
    }

    public LogicalSubQueryAlias(String tableAlias, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_SUBQUERY_ALIAS, groupExpression, logicalProperties, child);
        this.tableAlias = tableAlias;
    }

    public List<Slot> computeOutput(Plan input) {
        return input.getOutput();
    }

    @Override
    public String toString() {
        return "LogicalSubQueryAlias (" + tableAlias + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalSubQueryAlias that = (LogicalSubQueryAlias) o;
        return tableAlias.equals(that.tableAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableAlias);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalSubQueryAlias<>(tableAlias, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitSubQueryAlias((LogicalSubQueryAlias<Plan>) this, context);
    }

    @Override
    public List<Expression> getExpressions() {
        return null;
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalSubQueryAlias<>(tableAlias, groupExpression, Optional.of(logicalProperties), child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalSubQueryAlias<>(tableAlias, Optional.empty(), logicalProperties, child());
    }
}
