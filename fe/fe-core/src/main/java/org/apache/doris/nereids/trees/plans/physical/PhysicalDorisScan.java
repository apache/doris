package org.apache.doris.nereids.trees.plans.physical;

import com.google.common.collect.ImmutableSet;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class PhysicalDorisScan extends PhysicalCatalogRelation {
    private final Set<Expression> conjuncts;

    public PhysicalDorisScan(RelationId id, TableIf table, List<String> qualifier,
                            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties, Set<Expression> conjuncts) {
        this(id, table, qualifier, groupExpression, logicalProperties,
            null, null, conjuncts);
    }

    public PhysicalDorisScan(RelationId id, TableIf table, List<String> qualifier,
                            Optional<GroupExpression> groupExpression,
                            LogicalProperties logicalProperties, PhysicalProperties physicalProperties, Statistics statistics,
                            Set<Expression> conjuncts) {
        super(id, PlanType.PHYSICAL_JDBC_SCAN, table, qualifier, groupExpression,
            logicalProperties, physicalProperties, statistics);
        this.conjuncts = ImmutableSet.copyOf(
            Objects.requireNonNull(conjuncts, "conjuncts should not be null"));

    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalJdbcScan",
            "qualified", Utils.qualifiedName(qualifier, table.getName()),
            "output", getOutput(),
            "stats", statistics
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalDorisScan(this, context);
    }

    @Override
    public PhysicalDorisScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalDorisScan(relationId, table, qualifier, groupExpression, getLogicalProperties(),
            conjuncts);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalDorisScan(relationId, table, qualifier, groupExpression, logicalProperties.get(),
            conjuncts);
    }

    @Override
    public PhysicalDorisScan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
                                                           Statistics statistics) {
        return new PhysicalDorisScan(relationId, table, qualifier, groupExpression,
            getLogicalProperties(), physicalProperties, statistics,
            conjuncts);
    }

    public Set<Expression> getConjuncts() {
        return this.conjuncts;
    }
}
