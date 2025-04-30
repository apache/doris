package org.apache.doris.nereids.trees.plans.logical;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.doris.catalog.DorisTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class LogicalDorisScan extends LogicalExternalRelation {
    /**
     * Constructor for LogicalDorisScan.
     */
    public LogicalDorisScan(RelationId id, TableIf table, List<String> qualifier,
                           Optional<GroupExpression> groupExpression,
                           Optional<LogicalProperties> logicalProperties,
                           Set<Expression> conjuncts) {
        super(id, PlanType.LOGICAL_JDBC_SCAN, table, qualifier, conjuncts, groupExpression, logicalProperties);
    }

    public LogicalDorisScan(RelationId id, TableIf table, List<String> qualifier) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
            ImmutableSet.of());
    }

    @Override
    public TableIf getTable() {
        Preconditions.checkArgument(table instanceof ExternalTable || table instanceof DorisTable,
            String.format("Table %s is neither ExternalTable nor DorisTable", table.getName()));
        return table;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalDorisScan",
            "qualified", qualifiedName(),
            "output", getOutput()
        );
    }

    @Override
    public LogicalDorisScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalDorisScan(relationId, table, qualifier, groupExpression,
            Optional.of(getLogicalProperties()), conjuncts);
    }

    @Override
    public LogicalDorisScan withConjuncts(Set<Expression> conjuncts) {
        return new LogicalDorisScan(relationId, table, qualifier, Optional.empty(),
            Optional.of(getLogicalProperties()), conjuncts);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalDorisScan(relationId, table, qualifier, groupExpression, logicalProperties,
            conjuncts);
    }

    @Override
    public LogicalDorisScan withRelationId(RelationId relationId) {
        return new LogicalDorisScan(relationId, table, qualifier, Optional.empty(), Optional.empty(),
            conjuncts);
    }

    public LogicalDorisScan withLogicalProperties(LogicalProperties logicalProperties) {
        return new LogicalDorisScan(relationId, table, qualifier, groupExpression, Optional.of(logicalProperties),
            conjuncts);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalDorisScan(this, context);
    }
}
