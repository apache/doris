package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class LogicalSubQueryAlias<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> {
    private final String alias;

    public LogicalSubQueryAlias(String tableAlias, CHILD_TYPE child) {
        this(tableAlias, Optional.empty(), Optional.empty(), child);
    }

    public LogicalSubQueryAlias(String tableAlias, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_SUBQUERY_ALIAS, groupExpression, logicalProperties, child);
        this.alias = tableAlias;

    }

    public List<Slot> computeOutput(Plan input) {
        List<Slot> collect = input.getOutput().stream()
                .map(slot -> new SlotReference(slot.getName(), slot.getDataType(), slot.nullable(),
                        Lists.newArrayList(alias)))
                .collect(Collectors.toList());
        return collect;
    }

    public String getAlias() {
        return alias;
    }

    @Override
    public String toString() {
        return "LogicalSubQueryAlias (" + alias.toString() + ")";
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
        return alias.equals(that.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalSubQueryAlias<>(alias, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitSubQueryAlias((LogicalSubQueryAlias<Plan>) this, context);
    }

    @Override
    public List<Expression> getExpressions() {
        return Collections.emptyList();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalSubQueryAlias<>(alias, groupExpression, Optional.of(logicalProperties), child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalSubQueryAlias<>(alias, Optional.empty(), logicalProperties, child());
    }
}
