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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.catalog.KeysType;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PushDownAggOperator;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.HllType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.NumericType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * push aggregate without group by exprs to olap scan.
 */
public class PushAggregateToOlapScan implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate(logicalOlapScan())
                        .when(aggregate -> check(aggregate, aggregate.child()))
                        .then(aggregate -> {
                            LogicalOlapScan olapScan = aggregate.child();
                            Map<Slot, Slot> projections = Maps.newHashMap();
                            olapScan.getOutput().forEach(s -> projections.put(s, s));
                            LogicalOlapScan pushed = pushAggregateToOlapScan(aggregate, olapScan, projections);
                            if (pushed == olapScan) {
                                return aggregate;
                            } else {
                                return aggregate.withChildren(pushed);
                            }
                        })
                        .toRule(RuleType.PUSH_AGGREGATE_TO_OLAP_SCAN),
                logicalAggregate(logicalProject(logicalOlapScan()))
                        .when(aggregate -> check(aggregate, aggregate.child().child()))
                        .then(aggregate -> {
                            LogicalProject<LogicalOlapScan> project = aggregate.child();
                            LogicalOlapScan olapScan = project.child();
                            Map<Slot, Slot> projections = Maps.newHashMap();
                            olapScan.getOutput().forEach(s -> projections.put(s, s));
                            project.getProjects().stream()
                                    .filter(Alias.class::isInstance)
                                    .map(Alias.class::cast)
                                    .filter(alias -> alias.child() instanceof Slot)
                                    .forEach(alias -> projections.put(alias.toSlot(), (Slot) alias.child()));
                            LogicalOlapScan pushed = pushAggregateToOlapScan(aggregate, olapScan, projections);
                            if (pushed == olapScan) {
                                return aggregate;
                            } else {
                                return aggregate.withChildren(project.withChildren(pushed));
                            }
                        })
                        .toRule(RuleType.PUSH_AGGREGATE_TO_OLAP_SCAN)
        );
    }

    private boolean check(LogicalAggregate<? extends Plan> aggregate, LogicalOlapScan olapScan) {
        // session variables
        if (ConnectContext.get() != null && !ConnectContext.get().getSessionVariable().enablePushDownNoGroupAgg()) {
            return false;
        }

        // olap scan
        if (olapScan.isAggPushed()) {
            return false;
        }
        KeysType keysType = olapScan.getTable().getKeysType();
        if (keysType == KeysType.UNIQUE_KEYS || keysType == KeysType.PRIMARY_KEYS) {
            return false;
        }

        // aggregate
        if (!aggregate.getGroupByExpressions().isEmpty()) {
            return false;
        }
        List<AggregateFunction> aggregateFunctions = aggregate.getOutputExpressions().stream()
                .<Set<AggregateFunction>>map(e -> e.collect(AggregateFunction.class::isInstance))
                .flatMap(Set::stream).collect(Collectors.toList());
        if (aggregateFunctions.stream().anyMatch(af -> af.arity() > 1)) {
            return false;
        }
        if (!aggregateFunctions.stream()
                .allMatch(af -> af instanceof Count || af instanceof Min || af instanceof Max)) {
            return false;
        }

        // both
        if (aggregateFunctions.stream().anyMatch(Count.class::isInstance) && keysType != KeysType.DUP_KEYS) {
            return false;
        }

        return true;

    }

    private LogicalOlapScan pushAggregateToOlapScan(
            LogicalAggregate<? extends Plan> aggregate,
            LogicalOlapScan olapScan,
            Map<Slot, Slot> projections) {
        List<AggregateFunction> aggregateFunctions = aggregate.getOutputExpressions().stream()
                .<Set<AggregateFunction>>map(e -> e.collect(AggregateFunction.class::isInstance))
                .flatMap(Set::stream).collect(Collectors.toList());

        PushDownAggOperator pushDownAggOperator = olapScan.getPushDownAggOperator();
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            pushDownAggOperator = pushDownAggOperator.merge(aggregateFunction.getName());
            if (aggregateFunction.arity() == 0) {
                continue;
            }
            Expression child = aggregateFunction.child(0);
            Slot slot;
            if (child instanceof Slot) {
                slot = (Slot) child;
            } else if (child instanceof Cast && child.child(0) instanceof SlotReference) {
                if (child.getDataType() instanceof NumericType
                        && child.child(0).getDataType() instanceof NumericType) {
                    slot = (Slot) child.child(0);
                } else {
                    return olapScan;
                }
            } else {
                return olapScan;
            }

            // replace by SlotReference in olap table. check no complex project on this SlotReference.
            if (!projections.containsKey(slot)) {
                return olapScan;
            }
            slot = projections.get(slot);

            DataType dataType = slot.getDataType();
            if (pushDownAggOperator.containsMinMax()) {

                if (dataType instanceof ArrayType
                        || dataType instanceof HllType
                        || dataType instanceof BitmapType
                        || dataType instanceof StringType) {
                    return olapScan;
                }
            }

            // The zone map max length of CharFamily is 512, do not
            // over the length: https://github.com/apache/doris/pull/6293
            if (dataType instanceof CharacterType
                    && (((CharacterType) dataType).getLen() > 512 || ((CharacterType) dataType).getLen() < 0)) {
                return olapScan;
            }

            if (pushDownAggOperator.containsCount() && slot.nullable()) {
                return olapScan;
            }
        }
        return olapScan.withPushDownAggregateOperator(pushDownAggOperator);
    }
}
