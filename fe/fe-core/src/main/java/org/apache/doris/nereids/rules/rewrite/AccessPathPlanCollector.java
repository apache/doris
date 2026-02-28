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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.AccessPathInfo;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.rewrite.AccessPathExpressionCollector.CollectAccessPathResult;
import org.apache.doris.nereids.rules.rewrite.AccessPathExpressionCollector.CollectorContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.generator.Explode;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeMap;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeMapOuter;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeOuter;
import org.apache.doris.nereids.trees.expressions.functions.generator.PosExplode;
import org.apache.doris.nereids.trees.expressions.functions.generator.PosExplodeOuter;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.types.NestedColumnPrunable;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

/** AccessPathPlanCollector */
public class AccessPathPlanCollector extends DefaultPlanVisitor<Void, StatementContext> {
    private Multimap<Integer, CollectAccessPathResult> allSlotToAccessPaths = LinkedHashMultimap.create();
    private Map<Slot, List<CollectAccessPathResult>> scanSlotToAccessPaths = new LinkedHashMap<>();

    public Map<Slot, List<CollectAccessPathResult>> collect(Plan root, StatementContext context) {
        root.accept(this, context);
        return scanSlotToAccessPaths;
    }

    @Override
    public Void visitLogicalGenerate(LogicalGenerate<? extends Plan> generate, StatementContext context) {
        List<Function> generators = generate.getGenerators();
        List<Slot> output = generate.getGeneratorOutput();

        AccessPathExpressionCollector exprCollector
                = new AccessPathExpressionCollector(context, allSlotToAccessPaths, false);
        for (int i = 0; i < output.size(); i++) {
            Slot generatorOutput = output.get(i);
            Function function = generators.get(i);
            Collection<CollectAccessPathResult> accessPaths = allSlotToAccessPaths.get(
                    generatorOutput.getExprId().asInt());
            if (function instanceof Explode || function instanceof ExplodeOuter) {
                if (accessPaths.isEmpty()) {
                    // use the whole column
                    for (Expression child : function.children()) {
                        exprCollector.collect(child);
                    }
                } else {
                    for (CollectAccessPathResult accessPath : accessPaths) {
                        List<String> path = accessPath.getPath();
                        if (function.arity() == 1) {
                            // $c$1.VALUES.b
                            CollectorContext argumentContext = new CollectorContext(context, false);
                            argumentContext.setType(accessPath.getType());
                            argumentContext.getAccessPathBuilder()
                                    .addSuffix(AccessPathInfo.ACCESS_ALL)
                                    .addSuffix(path.subList(1, path.size()));
                            function.child(0).accept(exprCollector, argumentContext);
                            continue;
                        } else if (path.size() >= 2) {
                            // $c$1.col1.VALUES.b will be extract 'col1'
                            String colName = path.get(1);
                            // extract '1' in 'col1'
                            int colIndex = Integer.parseInt(colName.substring(StructLiteral.COL_PREFIX.length())) - 1;
                            CollectorContext argumentContext = new CollectorContext(context, false);
                            argumentContext.setType(accessPath.getType());
                            argumentContext.getAccessPathBuilder()
                                    .addSuffix(AccessPathInfo.ACCESS_ALL)
                                    .addSuffix(path.subList(2, path.size()));
                            function.child(colIndex).accept(exprCollector, argumentContext);
                            continue;
                        }
                        // use the whole column
                        for (Expression child : function.children()) {
                            exprCollector.collect(child);
                        }
                    }
                }
            } else if (function instanceof ExplodeMap || function instanceof ExplodeMapOuter) {
                if (accessPaths.isEmpty()) {
                    // use the whole column
                    for (Expression child : function.children()) {
                        exprCollector.collect(child);
                    }
                } else {
                    for (CollectAccessPathResult accessPath : accessPaths) {
                        List<String> path = accessPath.getPath();
                        if (path.size() >= 2) {
                            if (path.get(1).equalsIgnoreCase(StructLiteral.COL_PREFIX + "1")) {
                                // key
                                for (Expression child : function.children()) {
                                    CollectorContext argumentContext = new CollectorContext(context, false);
                                    argumentContext.setType(accessPath.getType());
                                    argumentContext.getAccessPathBuilder()
                                            .addSuffix(AccessPathInfo.ACCESS_MAP_KEYS)
                                            .addSuffix(path.subList(2, path.size()));
                                    child.accept(exprCollector, argumentContext);
                                }
                                continue;
                            } else if (path.get(1).equalsIgnoreCase(StructLiteral.COL_PREFIX + "2")) {
                                // value
                                for (Expression child : function.children()) {
                                    CollectorContext argumentContext = new CollectorContext(context, false);
                                    argumentContext.setType(accessPath.getType());
                                    argumentContext.getAccessPathBuilder()
                                            .addSuffix(AccessPathInfo.ACCESS_MAP_VALUES)
                                            .addSuffix(path.subList(2, path.size()));
                                    child.accept(exprCollector, argumentContext);
                                }
                                continue;
                            }
                        }
                        // use the whole column
                        exprCollector.collect(function.child(0));
                    }
                }
            } else if (function instanceof PosExplode || function instanceof PosExplodeOuter) {
                if (accessPaths.isEmpty()) {
                    // use the whole column
                    for (Expression child : function.children()) {
                        exprCollector.collect(child);
                    }
                } else {
                    boolean useWholeItem = false;
                    Set<Integer> prunedChildIndex = new TreeSet<>();
                    for (CollectAccessPathResult accessPath : accessPaths) {
                        List<String> path = accessPath.getPath();
                        if (path.size() >= 2) {
                            // $c$1.col1.VALUES.b will be extract 'col1'
                            String colName = path.get(1);
                            if (colName.startsWith(StructLiteral.COL_PREFIX)) {
                                // $c$1.col1.VALUES.b will be extract 'col1'
                                // extract '1' in 'col1'
                                int colIndex
                                        = Integer.parseInt(colName.substring(StructLiteral.COL_PREFIX.length())) - 1;
                                CollectorContext argumentContext = new CollectorContext(context, false);
                                argumentContext.setType(accessPath.getType());
                                argumentContext.getAccessPathBuilder()
                                        .addSuffix(AccessPathInfo.ACCESS_ALL)
                                        .addSuffix(path.subList(2, path.size()));
                                function.child(colIndex).accept(exprCollector, argumentContext);
                                prunedChildIndex.add(colIndex);
                            }
                        } else {
                            useWholeItem = true;
                            break;
                        }
                    }
                    if (useWholeItem) {
                        // use the whole column
                        for (Expression child : function.children()) {
                            exprCollector.collect(child);
                        }
                    } else {
                        for (int j = 0; j < function.arity(); j++) {
                            if (!prunedChildIndex.contains(j)) {
                                exprCollector.collect(function.child(j));
                            }
                        }
                    }
                }
            } else {
                exprCollector.collect(function);
            }
        }
        return generate.child().accept(this, context);
    }

    @Override
    public Void visitLogicalProject(LogicalProject<? extends Plan> project, StatementContext context) {
        AccessPathExpressionCollector exprCollector
                = new AccessPathExpressionCollector(context, allSlotToAccessPaths, false);
        for (NamedExpression output : project.getProjects()) {
            // e.g. select struct_element(s, 'city') from (select s from tbl)a;
            // we will not treat the inner `s` access all path
            if (output instanceof Slot && allSlotToAccessPaths.containsKey(output.getExprId().asInt())) {
                continue;
            } else if (output instanceof Alias && output.child(0) instanceof Slot
                    && allSlotToAccessPaths.containsKey(output.getExprId().asInt())) {
                Slot innerSlot = (Slot) output.child(0);
                Collection<CollectAccessPathResult> outerSlotAccessPaths = allSlotToAccessPaths.get(
                        output.getExprId().asInt());
                for (CollectAccessPathResult outerSlotAccessPath : outerSlotAccessPaths) {
                    List<String> outerPath = outerSlotAccessPath.getPath();
                    List<String> replaceSlotNamePath = new ArrayList<>();
                    replaceSlotNamePath.add(innerSlot.getName());
                    replaceSlotNamePath.addAll(outerPath.subList(1, outerPath.size()));
                    allSlotToAccessPaths.put(
                            innerSlot.getExprId().asInt(),
                            new CollectAccessPathResult(
                                    replaceSlotNamePath,
                                    outerSlotAccessPath.isPredicate(),
                                    outerSlotAccessPath.getType()
                            )
                    );
                }
            } else {
                exprCollector.collect(output);
            }
        }
        return project.child().accept(this, context);
    }

    @Override
    public Void visitLogicalFilter(LogicalFilter<? extends Plan> filter, StatementContext context) {
        boolean bottomFilter = filter.child().arity() == 0;
        collectByExpressions(filter, context, bottomFilter);
        return filter.child().accept(this, context);
    }

    @Override
    public Void visitLogicalCTEAnchor(
            LogicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor, StatementContext context) {
        // first, collect access paths in the outer slots, and propagate outer slot's access path to inner slots
        cteAnchor.right().accept(this, context);

        // second, push down access path in the inner slots
        cteAnchor.left().accept(this, context);
        return null;
    }

    @Override
    public Void visitLogicalCTEConsumer(LogicalCTEConsumer cteConsumer, StatementContext context) {
        // propagate outer slot's access path to inner slots
        for (Entry<Slot, Slot> slots : cteConsumer.getConsumerToProducerOutputMap().entrySet()) {
            Slot outerSlot = slots.getKey();

            if (outerSlot.getDataType() instanceof NestedColumnPrunable) {
                int outerSlotId = outerSlot.getExprId().asInt();
                int innerSlotId = slots.getValue().getExprId().asInt();
                allSlotToAccessPaths.putAll(innerSlotId, allSlotToAccessPaths.get(outerSlotId));
            }
        }
        return null;
    }

    @Override
    public Void visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer, StatementContext context) {
        return cteProducer.child().accept(this, context);
    }

    @Override
    public Void visitLogicalUnion(LogicalUnion union, StatementContext context) {
        // now we will not prune complex type through union, because we can not prune the complex type's literal,
        // for example, we can not prune the literal now: array(map(1, named_struct('a', 100, 'b', 100))),
        // so we can not prune this sql:
        // select struct_element(map_values(s[0]), 'a')
        // from (
        //     select s from tbl
        //     union all
        //     select array(map(1, named_struct('a', 100, 'b', 100)))
        // ) tbl;
        //
        // so we will not propagate access paths through the union
        for (Plan child : union.children()) {
            child.accept(this, context);
        }
        return null;
    }

    @Override
    public Void visitLogicalOlapScan(LogicalOlapScan olapScan, StatementContext context) {
        for (Slot slot : olapScan.getOutput()) {
            if (!(slot.getDataType() instanceof NestedColumnPrunable)) {
                continue;
            }
            Collection<CollectAccessPathResult> accessPaths = allSlotToAccessPaths.get(slot.getExprId().asInt());
            if (!accessPaths.isEmpty()) {
                scanSlotToAccessPaths.put(slot, new ArrayList<>(accessPaths));
            }
        }
        return null;
    }

    @Override
    public Void visitLogicalFileScan(LogicalFileScan fileScan, StatementContext context) {
        for (Slot slot : fileScan.getOutput()) {
            if (!(slot.getDataType() instanceof NestedColumnPrunable)) {
                continue;
            }
            Collection<CollectAccessPathResult> accessPaths = allSlotToAccessPaths.get(slot.getExprId().asInt());
            if (!accessPaths.isEmpty()) {
                scanSlotToAccessPaths.put(slot, new ArrayList<>(accessPaths));
            }
        }
        return null;
    }

    @Override
    public Void visitLogicalTVFRelation(LogicalTVFRelation tvfRelation, StatementContext context) {
        for (Slot slot : tvfRelation.getOutput()) {
            if (!(slot.getDataType() instanceof NestedColumnPrunable)) {
                continue;
            }
            Collection<CollectAccessPathResult> accessPaths = allSlotToAccessPaths.get(slot.getExprId().asInt());
            if (!accessPaths.isEmpty()) {
                scanSlotToAccessPaths.put(slot, new ArrayList<>(accessPaths));
            }
        }
        return null;
    }

    @Override
    public Void visit(Plan plan, StatementContext context) {
        collectByExpressions(plan, context);

        for (Plan child : plan.children()) {
            child.accept(this, context);
        }
        return null;
    }

    private void collectByExpressions(Plan plan, StatementContext context) {
        collectByExpressions(plan, context, false);
    }

    private void collectByExpressions(Plan plan, StatementContext context, boolean bottomPredicate) {
        AccessPathExpressionCollector exprCollector
                = new AccessPathExpressionCollector(context, allSlotToAccessPaths, bottomPredicate);
        for (Expression expression : plan.getExpressions()) {
            exprCollector.collect(expression);
        }
    }
}
