package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class EliminateAliasNode extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return RuleType.ELIMINATE_ALIAS_NODE.build(
                logicalSubQueryAlias().then(alias -> {

                            return null;
                        }
                )
        );
    }
}
