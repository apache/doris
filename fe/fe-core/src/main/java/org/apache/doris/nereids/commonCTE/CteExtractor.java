package org.apache.doris.nereids.commonCTE;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.analysis.CheckAfterRewrite;
import org.apache.doris.nereids.rules.exploration.mv.ComparisonResult;
import org.apache.doris.nereids.rules.exploration.mv.LogicalCompatibilityContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.AbstractLogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class CteExtractor {
    private final CascadesContext cascadesContext;
    private final AbstractLogicalPlan plan;
    private final Map<AbstractLogicalPlan, TableSignature> planToSignature = new HashMap<>();
    private final Map<TableSignature, List<AbstractLogicalPlan>> signatureToPlanList = new HashMap<>();

    public CteExtractor(AbstractLogicalPlan plan, CascadesContext cascadesContext) {
        this.plan = plan;
        this.cascadesContext = cascadesContext;
    }

    public List<Plan> execute() {
        sign();
        return extract();
    }

    private void sign() {
        SignatureVisitor visitor = new SignatureVisitor();
        visitor.visit(plan, planToSignature);
    }

    public List<Plan> extract() {
        for (AbstractLogicalPlan plan : planToSignature.keySet()) {
            TableSignature signature = planToSignature.get(plan);
            List<AbstractLogicalPlan> plans = signatureToPlanList.computeIfAbsent(signature, key -> new ArrayList<>());
            plans.add(plan);
        }
        TableSignature targetSignature = null;
        for (TableSignature signature : signatureToPlanList.keySet()) {
            if (signature.isContainsAggregation()) {
                targetSignature = signature;
            }
        }
        if (targetSignature != null) {
            List<AbstractLogicalPlan> targetPlans = signatureToPlanList.get(targetSignature);
            Plan target0 = targetPlans.get(0);
            Optional<StructInfo> optStruct0 = MaterializationContext.constructStructInfo(target0, target0, cascadesContext, new BitSet());
            StructInfo viewStructInfo = optStruct0.get();
            Plan target1 = targetPlans.get(1);
            Optional<StructInfo> optStruct1 = MaterializationContext.constructStructInfo(target1, target1, cascadesContext, new BitSet());
            StructInfo queryStructInfo = optStruct1.get();
            List<RelationMapping> queryToViewTableMappings = RelationMapping.generate(optStruct0.get().getRelations(),
                    optStruct1.get().getRelations());
            RelationMapping queryToViewTableMapping = queryToViewTableMappings.get(0);
            SlotMapping queryToViewSlotMapping = SlotMapping.generate(queryToViewTableMapping);
            SlotMapping viewToQuerySlotMapping = queryToViewSlotMapping.inverse();
            LogicalCompatibilityContext compatibilityContext = LogicalCompatibilityContext.from(
                    queryToViewTableMapping, viewToQuerySlotMapping, optStruct0.get(), optStruct1.get());
            ComparisonResult comparisonResult = StructInfo.isGraphLogicalEquals(queryStructInfo, viewStructInfo,
                    compatibilityContext);
            if (!comparisonResult.isInvalid()) {
                CTEId cteId = StatementScopeIdGenerator.newCTEId();
                Plan producerBody = LogicalPlanDeepCopier.INSTANCE
                        .deepCopy((LogicalPlan) target0, new DeepCopierContext());
                LogicalCTEProducer producer = new LogicalCTEProducer(cteId, producerBody);
                ConsumerRewriterContext context = new ConsumerRewriterContext(Lists.newArrayList(target0, target1), producer, cteId);
                Plan rewritten = plan.accept(ConsumerRewriter.INSTANCE, context);
                AbstractLogicalPlan root = new LogicalCTEAnchor(cteId, producer, rewritten);
                CheckAfterRewrite checker = new CheckAfterRewrite();
                checker.checkValidation(root);
                return Lists.newArrayList(root);
            }
            System.out.println(queryToViewTableMappings);
        }
        return Lists.newArrayList();
    }

    private void checkPlanValidation(Plan plan) {

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (TableSignature signature : signatureToPlanList.keySet()) {
            sb.append(String.format("%s\n", signature.toString()));
            sb.append("    ");
            for (AbstractLogicalPlan plan : signatureToPlanList.get(signature)) {
                sb.append(plan.getId()).append(" ");
            }
            sb.append("\n");
        }
        return sb.toString();
    }


}
