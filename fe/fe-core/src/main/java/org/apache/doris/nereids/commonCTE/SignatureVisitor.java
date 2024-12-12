package org.apache.doris.nereids.commonCTE;

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;

import com.google.common.collect.ImmutableSet;

import java.util.Map;

public class SignatureVisitor extends DefaultPlanVisitor <TableSignature, Map<Plan, TableSignature>>{

    @Override
    public TableSignature visitLogicalCatalogRelation(LogicalCatalogRelation relation,
                Map<Plan, TableSignature> signatureMap) {
        TableSignature signature =  new TableSignature(true, false,
                ImmutableSet.of(relation.getTable().getId()));
        signatureMap.put(relation, signature);
        return signature;
    }

    @Override
    public TableSignature visitLogicalFilter(LogicalFilter<? extends Plan> filter,
                Map<Plan, TableSignature> signatureMap) {
        TableSignature childSignature = filter.child().accept(this, signatureMap);
        if (filter.child() instanceof LogicalAggregate) {
            return TableSignature.EMPTY;
        }
        signatureMap.put(filter, childSignature);
        return childSignature;
    }

    @Override
    public TableSignature visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
                Map<Plan, TableSignature> signatureMap) {
        TableSignature signature = TableSignature.EMPTY;
        TableSignature leftSignature = join.left().accept(this, signatureMap);

        TableSignature rightSignature = join.right().accept(this, signatureMap);

        if (leftSignature != TableSignature.EMPTY && rightSignature != TableSignature.EMPTY) {
            signature = new TableSignature(true,
                    leftSignature.isContainsAggregation() || rightSignature.isContainsAggregation(),
                    new ImmutableSet.Builder()
                            .addAll(leftSignature.getTableIds())
                            .addAll(rightSignature.getTableIds())
                            .build());
            signatureMap.put(join, signature);
        }
        return signature;
    }

    @Override
    public TableSignature visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate,
                Map<Plan, TableSignature> signatureMap) {
        TableSignature signature = TableSignature.EMPTY;
        TableSignature childSignature = aggregate.child().accept(this, signatureMap);
        if (childSignature != TableSignature.EMPTY) {
            signature = childSignature.withContainsAggregation(true);
            signatureMap.put(aggregate, signature);
        }
        return signature;
    }

    @Override
    public TableSignature visitLogicalProject(LogicalProject<? extends Plan> project,
                Map<Plan, TableSignature> signatureMap) {
        TableSignature childSignature = project.child().accept(this, signatureMap);
        if (childSignature != TableSignature.EMPTY) {
            signatureMap.put(project, childSignature);
        }
        return childSignature;
    }

    @Override
    public TableSignature visit(Plan plan, Map<Plan, TableSignature> signatureMap) {
        for (Plan child : plan.children()) {
            child.accept(this, signatureMap);
        }
        return TableSignature.EMPTY;
    }
}


