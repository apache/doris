package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.hint.LeadingHint;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.RelationUtil;

import java.util.List;
import java.util.Optional;

public class BindRelationForCreateView extends OneAnalysisRuleFactory {

    // TODO: cte will be copied to a sub-query with different names but the id of the unbound relation in them
    //  are the same, so we use new relation id when binding relation, and will fix this bug later.
    @Override
    public Rule build() {
        return unboundRelation().thenApply(ctx -> {
            Plan plan = doBindRelation(ctx);
            if (!(plan instanceof Unbound) && plan instanceof Relation) {
                // init output and allocate slot id immediately, so that the slot id increase
                // in the order in which the table appears.
                LogicalProperties logicalProperties = plan.getLogicalProperties();
                logicalProperties.getOutput();
            }
            return plan;
        }).toRule(RuleType.BINDING_RELATION_FOR_CREATE_VIEW);
    }

    private Plan doBindRelation(MatchingContext<UnboundRelation> ctx) {
        List<String> nameParts = ctx.root.getNameParts();
        switch (nameParts.size()) {
            case 1: { // table
                // Use current database name from catalog.
                return bindWithCurrentDb(ctx.cascadesContext, ctx.root);
            }
            case 2:
                // db.table
                // Use database name from table name parts.
            case 3: {
                // catalog.db.table
                // Use catalog and database name from name parts.
                return bind(ctx.cascadesContext, ctx.root);
            }
            default:
                throw new IllegalStateException("Table name [" + ctx.root.getTableName() + "] is invalid.");
        }
    }

    private LogicalPlan bindWithCurrentDb(CascadesContext cascadesContext, UnboundRelation unboundRelation) {
        String tableName = unboundRelation.getNameParts().get(0);
        // check if it is a CTE's name
        CTEContext cteContext = cascadesContext.getCteContext().findCTEContext(tableName).orElse(null);
        if (cteContext != null) {
            Optional<LogicalPlan> analyzedCte = cteContext.getAnalyzedCTEPlan(tableName);
            if (analyzedCte.isPresent()) {
                LogicalCTEConsumer consumer = new LogicalCTEConsumer(unboundRelation.getRelationId(),
                        cteContext.getCteId(), tableName, analyzedCte.get());
                if (cascadesContext.isLeadingJoin()) {
                    LeadingHint leading = (LeadingHint) cascadesContext.getHintMap().get("Leading");
                    leading.putRelationIdAndTableName(Pair.of(consumer.getRelationId(), tableName));
                    leading.getRelationIdToScanMap().put(consumer.getRelationId(), consumer);
                }
                return consumer;
            }
        }
        List<String> tableQualifier = RelationUtil.getQualifierName(cascadesContext.getConnectContext(),
                unboundRelation.getNameParts());
        unboundRelation.setTableQualifier(tableQualifier);
        return unboundRelation;
    }

    private LogicalPlan bind(CascadesContext cascadesContext, UnboundRelation unboundRelation) {
        List<String> tableQualifier = RelationUtil.getQualifierName(cascadesContext.getConnectContext(),
                unboundRelation.getNameParts());
        unboundRelation.setTableQualifier(tableQualifier);
        return unboundRelation;
    }
}
