package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDorisScan;

import java.util.Optional;

public class LogicalDorisScanToPhysicalDorisScan extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalDorisScan().then(dorisScan ->
            new PhysicalDorisScan(
                dorisScan.getRelationId(),
                dorisScan.getTable(),
                dorisScan.getQualifier(),
                Optional.empty(),
                dorisScan.getLogicalProperties(),
                dorisScan.getConjuncts())
        ).toRule(RuleType.LOGICAL_DORIS_SCAN_TO_PHYSICAL_DORIS_SCAN_RULE);
    }
}
