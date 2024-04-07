package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;

/** PushCountIntoUnionAll */
public class PushCountIntoUnionAll extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate(logicalSetOperation())
                //.when()
                .then(agg -> {
                    // 把union all的每一个孩子上面都加上一个agg算子，group by列为上层agg的group by列
                    // outputExpression为count()和group by列。
                    // 想想限制：agg的输出中只有count()聚合函数。需要是union all才行。不能是count(distinct)
                    // 列的话应该这样就行了，考虑一下表达式。
                    // count()里面的表达式不影响，可能需要改下名字，放下去就行。
                    // group by 后面有表达式，导致agg和union之间有project的时候也可以转。
                    return agg;
                }).toRule(RuleType.PUSH_COUNT_INTO_UNION_ALL);
    }
}
