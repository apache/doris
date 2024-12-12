package org.apache.doris.nereids.commonCTE;

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.AbstractLogicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CteExtractor {
    private AbstractLogicalPlan plan;
    private Map<Plan, TableSignature> anchorToSignature = new HashMap<>();
    private Map<TableSignature, List<Plan>> signatureToAnchorList = new HashMap<>();

    public CteExtractor(AbstractLogicalPlan plan) {
        this.plan = plan;
    }

    public AbstractLogicalPlan execute() {
        sign();
        return plan;
    }

    private void sign() {
        SignatureVisitor visitor = new SignatureVisitor();
        visitor.visit(plan, anchorToSignature);
        extract();
    }

    private void extract() {
        List<TableSignature> a = anchorToSignature.values().stream().collect(Collectors.toList());


        for (Plan plan : anchorToSignature.keySet()) {
            TableSignature signature = anchorToSignature.get(plan);
            List<Plan> plans = signatureToAnchorList.computeIfAbsent(signature, key -> new ArrayList<>());
            plans.add(plan);
        }
    }


}
