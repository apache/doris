// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.executor.AbstractBatchJobExecutor;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.BindSink;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rules.ConvertAggStateCast;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.rules.rewrite.MergeProjectable;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.rules.rewrite.PushProjectIntoUnion;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/** InsertIntoValuesAnalyzer */
public class InsertIntoValuesAnalyzer extends AbstractBatchJobExecutor {
    public static final List<RewriteJob> INSERT_JOBS = jobs(
            bottomUp(
                    new InlineTableToUnionOrOneRowRelation(),
                    new BindSink(),
                    new MergeProjectable(),
                    // after bind olap table sink, the LogicalProject will be generated under LogicalOlapTableSink,
                    // we should convert the agg state function in the project, and evaluate some env parameters
                    // like encrypt key reference, for example: `values (aes_encrypt("abc",key test.my_key))`,
                    // we should replace the `test.my_key` to real key
                    new RewriteInsertIntoExpressions(ExpressionRewrite.bottomUp(
                            ConvertAggStateCast.INSTANCE,
                            FoldConstantRuleOnFE.PATTERN_MATCH_INSTANCE
                    ))
            )
    );

    public static final List<RewriteJob> BATCH_INSERT_JOBS = jobs(
            bottomUp(
                    new InlineTableToUnionOrOneRowRelation(),
                    new BindSink(),
                    new MergeProjectable(),

                    // the BatchInsertIntoTableCommand need send StringLiteral to backend,
                    // and only support alias(literal as xx) or alias(cast(literal as xx)),
                    // but not support alias(cast(slotRef as xx)) which create in BindSink,
                    // we should push down the cast into Union or OneRowRelation.
                    // the InsertIntoTableCommand support translate slotRef in the TPlan,
                    // so we don't need this rules, just evaluate in backend
                    new PushProjectIntoUnion(),

                    new RewriteInsertIntoExpressions(ExpressionRewrite.bottomUp(
                            ConvertAggStateCast.INSTANCE,
                            FoldConstantRuleOnFE.PATTERN_MATCH_INSTANCE
                    ))
            )
    );

    private final boolean batchInsert;

    public InsertIntoValuesAnalyzer(CascadesContext cascadesContext, boolean batchInsert) {
        super(cascadesContext);
        this.batchInsert = batchInsert;
    }

    @Override
    public List<RewriteJob> getJobs() {
        return batchInsert ? BATCH_INSERT_JOBS : INSERT_JOBS;
    }

    private static class RewriteInsertIntoExpressions extends ExpressionRewrite {
        public RewriteInsertIntoExpressions(ExpressionRewriteRule... rules) {
            super(rules);
        }

        @Override
        public List<Rule> buildRules() {
            return ImmutableList.of(
                    new ProjectExpressionRewrite().build(),
                    new OneRowRelationExpressionRewrite().build()
            );
        }
    }

    private static class InlineTableToUnionOrOneRowRelation extends OneRewriteRuleFactory {
        @Override
        public Rule build() {
            return inlineTable().then(inlineTable -> {
                List<List<NamedExpression>> originConstants = inlineTable.getConstantExprsList();
                if (originConstants.size() > 1) {
                    Pair<List<List<NamedExpression>>, List<Boolean>> castedConstantsAndNullables
                            = LogicalUnion.castCommonDataTypeAndNullableByConstants(originConstants);
                    List<List<NamedExpression>> castedRows = castedConstantsAndNullables.key();
                    List<Boolean> nullables = castedConstantsAndNullables.value();
                    List<NamedExpression> outputs = Lists.newArrayList();
                    List<NamedExpression> firstRow = originConstants.get(0);
                    for (int columnId = 0; columnId < firstRow.size(); columnId++) {
                        String name = firstRow.get(columnId).getName();
                        DataType commonDataType = castedRows.get(0).get(columnId).getDataType();
                        outputs.add(new SlotReference(name, commonDataType, nullables.get(columnId)));
                    }
                    return new LogicalUnion(Qualifier.ALL, castedRows, ImmutableList.of()).withNewOutputs(outputs);
                } else if (originConstants.size() == 1) {
                    return new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(), originConstants.get(0));
                } else {
                    throw new AnalysisException("Illegal inline table with empty constants");
                }
            }).toRule(RuleType.LOGICAL_INLINE_TABLE_TO_LOGICAL_UNION_OR_ONE_ROW_RELATION);
        }
    }
}
