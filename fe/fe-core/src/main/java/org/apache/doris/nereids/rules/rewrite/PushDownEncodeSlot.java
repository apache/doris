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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DecodeAsVarchar;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EncodeString;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeaf;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * push down encode_as_int(slot) down
 * example:
 *   group by x
 *     -->project(encode_as_int(A) as x)
 *      -->Any(A)
 *       -->project(A)
 *          --> scan
 *   =>
 *   group by x
 *     -->project(x)
 *      -->Any(x)
 *       --> project(encode_as_int(A) as x)
 *          -->scan
 * Note:
 * do not push down encode if encode.child() is not slot,
 * example
 * group by encode_as_int(A + B)
 *    --> any(A, B)
 */
public class PushDownEncodeSlot extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalProject()
                .when(topN -> ConnectContext.get() != null
                        && ConnectContext.get().getSessionVariable().enableCompressMaterialize)
                .whenNot(project -> project.child() instanceof LogicalRepeat)
                .whenNot(project -> (project.child() instanceof LogicalLeaf))
                .then(this::pushDownEncodeSlot)
                .toRule(RuleType.PUSH_DOWN_ENCODE_SLOT);
    }

    private List<Alias> collectEncodeAliases(LogicalProject<? extends Plan> project) {
        List<Alias> encodeAliases = new ArrayList<>();
        Set<Slot> computingSlots = new HashSet<>();
        for (NamedExpression e : project.getProjects()) {
            if (e instanceof Alias) {
                Expression aliasBody = e.child(0);
                if (!(aliasBody instanceof SlotReference) && !(aliasBody instanceof EncodeString)) {
                    computingSlots.addAll(e.getInputSlots());
                }
            }
        }
        for (NamedExpression e : project.getProjects()) {
            if (e instanceof Alias && e.child(0) instanceof EncodeString
                    && e.child(0).child(0) instanceof SlotReference
                    && !computingSlots.contains(e.child(0).child(0))) {
                encodeAliases.add((Alias) e);
            }
        }
        return encodeAliases;
    }

    /**
     * case 1
     * project(encode(A) as B)
     *    --> any(A)
     * =>
     * project(B)
     *     -->any(A): push "encode(A) as B"
     *
     * case 2
     * project(A, encode(A) as B)
     *      -->any(A)
     * =>
     * project(decode(B) as A, B)
     *      -->any(A): push "encode(A) as B"
     *
     * case 3
     * project(A as C, encode(A) as B)
     *      -->any(A)
     * =>
     * project(decode(B) as C, B)
     *      -->any(A): push "encode(A) as B"
     */
    private LogicalProject<? extends Plan> rewriteRootProject(LogicalProject<? extends Plan> project,
            List<Alias> pushedEncodeAlias) {
        if (pushedEncodeAlias.isEmpty()) {
            return project;
        }
        Map<Expression, Alias> encodeBodyToEncodeAlias = new HashMap<>();
        for (Alias alias : pushedEncodeAlias) {
            Expression encodeBody = alias.child().child(0);
            encodeBodyToEncodeAlias.put(encodeBody, alias);
        }
        List<NamedExpression> projections = Lists.newArrayListWithCapacity(project.getProjects().size());
        for (NamedExpression e : project.getProjects()) {
            if (pushedEncodeAlias.contains(e)) {
                // case 1
                projections.add(e.toSlot());
            } else if (encodeBodyToEncodeAlias.containsKey(e)) {
                // case 2
                ExprId id = e.getExprId();
                DecodeAsVarchar decode = new DecodeAsVarchar(encodeBodyToEncodeAlias.get(e).toSlot());
                Alias alias = new Alias(id, decode, decode.toSql());
                projections.add(alias);
            } else if (e instanceof Alias && encodeBodyToEncodeAlias.containsKey(e.child(0))) {
                // case 3
                Alias alias = (Alias) e;
                DecodeAsVarchar decode = new DecodeAsVarchar(encodeBodyToEncodeAlias.get(e.child(0)).toSlot());
                Alias newAlias = (Alias) alias.withChildren(decode);
                projections.add(newAlias);
            } else {
                projections.add(e);
            }
        }
        return project.withProjects(projections);

    }

    private LogicalProject<? extends Plan> pushDownEncodeSlot(LogicalProject<? extends Plan> project) {
        List<Alias> encodeAliases = collectEncodeAliases(project);
        if (encodeAliases.isEmpty()) {
            return project;
        }

        PushDownContext ctx = new PushDownContext(project, encodeAliases);
        ctx.prepare();
        if (ctx.notPushed.size() == encodeAliases.size()) {
            return project;
        }
        Plan child = project.child();
        PushDownContext childContext = new PushDownContext(child, ctx.toBePushedToChild.get(child));
        Plan newChild = child.accept(EncodeSlotPushDownVisitor.INSTANCE, childContext);
        List<Alias> pushed = ctx.toBePused;
        if (child != newChild) {
            if (newChild instanceof LogicalProject) {
                pushed.removeAll(childContext.notPushed);
                newChild = ((LogicalProject<?>) newChild).child();
            }
            project = (LogicalProject<? extends Plan>) project.withChildren(newChild);
            project = rewriteRootProject(project, pushed);
        }
        return project;
    }

    /**
     * push down encode slot context
     */
    public static class PushDownContext {
        public Plan plan;

        public List<Alias> encodeAliases;
        // encode_as_int(slot1) as slot2
        // replaceMap:
        // slot1 -> slot2
        Map<Expression, SlotReference> replaceMap = new HashMap<>();
        // child plan -> aliases in encodeAliases which can be pushed down to child plan
        Map<Plan, List<Alias>> toBePushedToChild = new HashMap<>();
        List<Alias> toBePused = new ArrayList<>();
        // the aliases that cannot be pushed down to any child plan
        // for example:
        // encode(A+B) as x, where plan is a join, and A, B comes from join's left and right child respectively
        List<Alias> notPushed = new ArrayList<>();

        public PushDownContext(Plan plan, List<Alias> encodeAliases) {
            this.plan = plan;
            this.encodeAliases = encodeAliases;
        }

        // init replaceMap/toBePushed/notPushed
        private void prepare() {
            List<Set<Slot>> childrenPassThroughSlots =
                    plan.children().stream().map(n -> getPassThroughSlots(n)).collect(Collectors.toList());
            for (int i = 0; i < plan.children().size(); i++) {
                Plan child = plan.children().get(i);
                if (child instanceof LogicalJoin) {
                    LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) child;
                    BiMap<SlotReference, SlotReference> compareSlots = EncodeSlotPushDownVisitor
                            .getEncodeCandidateSlotsAndShouldNotPushSlotsFromJoinCondition(join).first;
                    childrenPassThroughSlots.get(i).addAll(compareSlots.keySet());
                }
            }
            for (Alias alias : encodeAliases) {
                EncodeString encode = (EncodeString) alias.child();
                Expression strExpr = encode.child();
                boolean pushed = false;
                Preconditions.checkArgument(strExpr instanceof SlotReference,
                        "expect encode_as_xxx(slot), but " + alias);

                for (int i = 0; i < childrenPassThroughSlots.size(); i++) {
                    if (childrenPassThroughSlots.get(i).contains(strExpr)) {
                        toBePushedToChild.putIfAbsent(plan.child(i), new ArrayList<>());
                        toBePushedToChild.get(plan.child(i)).add(alias);
                        toBePused.add(alias);
                        replaceMap.put(alias.child().child(0), (SlotReference) alias.toSlot());
                        pushed = true;
                        break;
                    }
                }
                if (!pushed) {
                    notPushed.add(alias);
                }
            }
        }

        /**
         * expandEncodeAliasForJoin
         */
        public void expandEncodeAliasForJoin(BiMap<SlotReference, SlotReference> equalSlots) {
            List<Alias> expanded = new ArrayList<>();
            for (Alias alias : encodeAliases) {
                if (alias.child().child(0) instanceof SlotReference) {
                    SlotReference slot = (SlotReference) alias.child().child(0);
                    SlotReference otherHand = equalSlots.get(slot);
                    if (otherHand != null) {
                        EncodeString encodeOtherHand = (EncodeString) alias.child().withChildren(otherHand);
                        Alias encodeOtherHandAlias = new Alias(encodeOtherHand, encodeOtherHand.toSql());
                        if (!encodeAliases.contains(encodeOtherHandAlias)) {
                            expanded.add(encodeOtherHandAlias);
                        }
                    }
                }
            }
            encodeAliases.addAll(expanded);
        }

        // the child of alias is a slot reference. for example: slotA as B
        //
        private boolean isSlotAlias(Expression expr) {
            return expr instanceof Alias && expr.child(0) instanceof SlotReference;
        }

        private Set<Slot> getPassThroughSlots(Plan plan) {
            Set<Slot> outputSlots = Sets.newHashSet(plan.getOutputSet());
            Set<Slot> keySlots = Sets.newHashSet();
            for (Expression e : plan.getExpressions()) {
                if (!(e instanceof SlotReference) && !isSlotAlias(e)) {
                    keySlots.addAll(e.getInputSlots());
                }
            }
            outputSlots.removeAll(keySlots);
            return outputSlots;
        }
    }

    /**
     * push down encode slot
     */
    public static class EncodeSlotPushDownVisitor extends PlanVisitor<Plan, PushDownContext> {
        public static EncodeSlotPushDownVisitor INSTANCE = new EncodeSlotPushDownVisitor();

        /**
         * visitChildren
         */
        public Plan visitChildren(Plan plan, PushDownContext ctx) {
            ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
            boolean hasNewChildren = false;
            for (Plan child : plan.children()) {
                Plan newChild;
                if (ctx.toBePushedToChild.containsKey(child)) {
                    newChild = child.accept(this, new PushDownContext(child, ctx.toBePushedToChild.get(child)));
                    if (!hasNewChildren && newChild != child) {
                        hasNewChildren = true;
                    }
                } else {
                    newChild = child;
                }
                newChildren.add(newChild);
            }
            if (hasNewChildren) {
                plan = plan.withChildren(newChildren.build());
            }
            return plan;
        }

        private Plan projectNotPushedAlias(Plan plan, List<Alias> notPushedAlias) {
            if (!notPushedAlias.isEmpty()) {
                // project encode expressions if they are not pushed down
                // project(encode)
                //   +--> plan
                List<NamedExpression> projections =
                        notPushedAlias.stream().map(e -> (NamedExpression) e).collect(Collectors.toList());
                projections.addAll(plan.getOutput());
                plan = new LogicalProject<>(projections, plan);
            }
            return plan;
        }

        @Override
        public Plan visit(Plan plan, PushDownContext ctx) {
            ctx.prepare();
            plan = visitChildren(plan, ctx);
            plan = projectNotPushedAlias(plan, ctx.notPushed);
            return plan;
        }

        @Override
        public Plan visitLogicalRepeat(LogicalRepeat repeat, PushDownContext ctx) {
            Plan plan = projectNotPushedAlias(repeat, ctx.encodeAliases);
            return plan;
        }

        private Optional<Alias> findEncodeAliasByEncodeSlot(SlotReference slot, List<Alias> aliases) {
            for (Alias alias : aliases) {
                if (alias.child().child(0).equals(slot)) {
                    return Optional.of(alias);
                }
            }
            return Optional.empty();
        }

        @Override
        public LogicalProject<? extends Plan> visitLogicalProject(
                LogicalProject<? extends Plan> project, PushDownContext ctx) {
            /*
             * case 1
             * push down "encode(v1) as v2
             * project(v1, ...)
             *    +--->any(v1)
             * =>
             * project(v2, ...)
             *    +--->any(v1)
             * and push down "encode(v1) as v2" to any(v1)
             *
             * case 2
             * push down "encode(v1) as v2
             * project(k as v1, ...)
             *    +--->any(k)
             * =>
             * project(v2, ...)
             *    +--->any(k)
             * and push down "encode(k) as v2" to any(v1)
             *
             * case 3
             * push down "encode(v44) as v307"
             * project(decode(v305) as v44)
             *     +-->agg(v305, groupBy[v305])
             *         +--->project(encode(v44) as v305)
             * =>
             * project(v305 as v307)
             *     +-->agg
             *
             * case 4
             * push down "encode(v1) as v2
             * project(a + b as v1, ...)
             *    +--->any(a, b)
             * =>
             * project(encode(a+b) as v2, ...)
             *    +-->any(a, b)

             *
             */
            List<NamedExpression> projections = Lists.newArrayListWithCapacity(project.getProjects().size());
            List<Alias> toBePushed = Lists.newArrayList();
            List<Alias> notPushed = Lists.newArrayList(ctx.encodeAliases);

            for (NamedExpression e : project.getProjects()) {
                boolean changed = false;

                if (e instanceof SlotReference) {
                    Optional<Alias> encodeAliasOpt = findEncodeAliasByEncodeSlot((SlotReference) e, ctx.encodeAliases);
                    if (encodeAliasOpt.isPresent()) {
                        // case 1
                        projections.add(encodeAliasOpt.get().toSlot());
                        toBePushed.add(encodeAliasOpt.get());
                        notPushed.remove(encodeAliasOpt.get());
                        changed = true;
                    }
                } else {
                    // e is Alias
                    Expression aliasExpr = e.child(0);
                    if (aliasExpr instanceof SlotReference) {
                        //case 2
                        Optional<Alias> encodeAliasOpt = findEncodeAliasByEncodeSlot((SlotReference) e.toSlot(),
                                ctx.encodeAliases);
                        if (encodeAliasOpt.isPresent()) {
                            projections.add(encodeAliasOpt.get().toSlot());
                            Alias encodeAlias = encodeAliasOpt.get();
                            EncodeString encode = (EncodeString) encodeAlias.child();
                            SlotReference baseSlot = (SlotReference) aliasExpr;
                            Alias encodeAliasForChild = (Alias) encodeAlias.withChildren(encode.withChildren(baseSlot));
                            toBePushed.add(encodeAliasForChild);
                            notPushed.remove(encodeAlias);
                            changed = true;
                        }
                    } else {
                        Optional<Alias> encodeAliasOpt = findEncodeAliasByEncodeSlot((SlotReference) e.toSlot(),
                                ctx.encodeAliases);
                        if (encodeAliasOpt.isPresent()) {
                            Alias encodeAlias = encodeAliasOpt.get();
                            if (aliasExpr instanceof DecodeAsVarchar) {
                                // case 3
                                // push down "encode(v44) as v307"
                                // project(decode(v305) as v44)
                                //      +-->agg(v305, groupBy[v305])
                                //         +--->project(encode(v44) as v305)
                                Expression decodeBody = aliasExpr.child(0);
                                Alias aliasForProject = (Alias) encodeAlias.withChildren(decodeBody);
                                projections.add(aliasForProject);
                                notPushed.remove(encodeAlias);
                                changed = true;
                            } else {
                                // case 4
                                EncodeString encode = (EncodeString) encodeAlias.child();
                                Alias encodeAliasForProject = (Alias) encodeAlias
                                        .withChildren(encode.withChildren(aliasExpr));
                                projections.add(encodeAliasForProject);
                                notPushed.remove(encodeAlias);
                                changed = true;
                            }
                        }
                    }
                }
                if (!changed) {
                    projections.add(e);
                }
            }
            projections.addAll(notPushed);

            project = project.withProjects(projections);
            if (!toBePushed.isEmpty()) {
                PushDownContext childContext = new PushDownContext(project.child(), toBePushed);
                Plan newChild = project.child().accept(this, childContext);
                if (project.child() != newChild) {
                    project = (LogicalProject<? extends Plan>) project.withChildren(newChild);
                }
            }
            return project;
        }

        private static boolean canBothSidesEncode(ComparisonPredicate compare) {
            return compare.left().getDataType() instanceof CharacterType
                    && ((CharacterType) compare.left().getDataType()).getLen() < 15
                    && ((CharacterType) compare.right().getDataType()).getLen() < 15
                    && compare.left() instanceof SlotReference && compare.right() instanceof SlotReference;
        }

        /**
         * getEncodeCandidateSlotsFromJoinCondition
         *
         */
        public static Pair<BiMap<SlotReference, SlotReference>, Set<Slot>>
                getEncodeCandidateSlotsAndShouldNotPushSlotsFromJoinCondition(LogicalJoin<?, ?> join) {
            // T1 join T2 on v1=v2  => v1/v2 can be encoded
            // T1 join T2 on v1=v2 and fun(v1) => v1/v2 can not be encoded
            BiMap<SlotReference, SlotReference> compareSlots = HashBiMap.create();
            List<Expression> conditions = new ArrayList<>();
            conditions.addAll(join.getHashJoinConjuncts());
            conditions.addAll(join.getOtherJoinConjuncts());
            Set<Slot> shouldNotPushSlots = Sets.newHashSet();
            for (Expression e : conditions) {
                boolean canPush = false;
                if (e instanceof ComparisonPredicate) {
                    ComparisonPredicate compare = (ComparisonPredicate) e;
                    if (canBothSidesEncode(compare)) {
                        compareSlots.put((SlotReference) compare.left(), (SlotReference) compare.right());
                        canPush = true;
                    }
                }
                if (!canPush) {
                    shouldNotPushSlots.addAll(e.getInputSlots());
                }
            }
            for (Slot notPushSlot : shouldNotPushSlots) {
                if (compareSlots.isEmpty()) {
                    break;
                }
                compareSlots.remove((SlotReference) notPushSlot);
                compareSlots.inverse().remove((SlotReference) notPushSlot);
            }
            return Pair.of(compareSlots, shouldNotPushSlots);
        }

        @Override
        public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, PushDownContext ctx) {
            List<Alias> pushLeft = new ArrayList<>();
            List<Alias> pushRight = new ArrayList<>();
            Pair<BiMap<SlotReference, SlotReference>, Set<Slot>> pair =
                    getEncodeCandidateSlotsAndShouldNotPushSlotsFromJoinCondition(join);
            BiMap<SlotReference, SlotReference> encodeCandidateSlots = pair.first;
            Set<Slot> shouldNotPushSlots = pair.second;
            Set<Slot> leftOutputSlots = join.left().getOutputSet();
            Map<SlotReference, SlotReference> replaceMap = new HashMap<>();
            List<Alias> notPushed = new ArrayList<>();
            for (Alias encodeAlias : ctx.encodeAliases) {
                SlotReference encodeSlot = (SlotReference) encodeAlias.child().child(0);
                if (encodeCandidateSlots.containsKey(encodeSlot)) {
                    SlotReference otherHand = encodeCandidateSlots.get(encodeSlot);
                    Alias otherHandAlias = new Alias(encodeAlias.child().withChildren(otherHand));
                    if (leftOutputSlots.contains(encodeSlot)) {
                        pushLeft.add(encodeAlias);
                        pushRight.add(otherHandAlias);
                    } else {
                        pushRight.add(encodeAlias);
                        pushLeft.add(otherHandAlias);
                    }
                    replaceMap.put(encodeSlot, (SlotReference) encodeAlias.toSlot());
                    replaceMap.put(otherHand, (SlotReference) otherHandAlias.toSlot());
                } else if (!shouldNotPushSlots.contains(encodeSlot)) {
                    if (leftOutputSlots.contains(encodeSlot)) {
                        pushLeft.add(encodeAlias);
                    } else {
                        pushRight.add(encodeAlias);
                    }
                } else {
                    notPushed.add(encodeAlias);
                }
                replaceMap.put(encodeSlot, (SlotReference) encodeAlias.toSlot());
            }
            List<Expression> newConjuncts = Lists.newArrayListWithCapacity(join.getOtherJoinConjuncts().size());
            boolean changed = false;
            for (Expression e : join.getOtherJoinConjuncts()) {
                if (e instanceof ComparisonPredicate) {
                    ComparisonPredicate compare = (ComparisonPredicate) e;
                    if (canBothSidesEncode(compare)) {
                        SlotReference newLeft = replaceMap.get(compare.left());
                        SlotReference newRight = replaceMap.get(compare.right());
                        if (newLeft != null && newRight != null) {
                            compare = (ComparisonPredicate) compare.withChildren(newLeft, newRight);
                            changed = true;
                        }
                        Preconditions.checkArgument((newLeft == null) == (newRight == null),
                                "PushDownEncodeSlot replaceMap is not valid, " + compare);
                    }
                    newConjuncts.add(compare);
                } else {
                    newConjuncts.add(e);
                }
            }
            if (changed) {
                join = join.withJoinConjuncts(join.getHashJoinConjuncts(), newConjuncts, join.getJoinReorderContext());
            }
            Plan newLeft;
            if (pushLeft.isEmpty()) {
                newLeft = join.left();
            } else {
                newLeft = join.left().accept(this, new PushDownContext(join.left(), pushLeft));
            }
            Plan newRight;
            if (pushRight.isEmpty()) {
                newRight = join.right();
            } else {
                newRight = join.right().accept(this, new PushDownContext(join.right(), pushRight));
            }
            Plan result = join.withChildren(newLeft, newRight);
            if (!notPushed.isEmpty()) {
                List<NamedExpression> projections = new ArrayList<>();
                projections.addAll(notPushed);
                projections.addAll(join.getOutput());
                result = new LogicalProject<Plan>(projections, join);
            }
            return result;
        }

        @Override
        public Plan visitLogicalSetOperation(LogicalSetOperation op, PushDownContext ctx) {
            // push down "encode(v) as x" through
            // union(output[v], regular([v1],[v2]))
            //    -->child1(v1)
            //    -->child2(v2)
            // rewrite union to: union(output[x], regular([x1], [x2]))
            // and then push "encode(v1) as x1" to child(v1)
            //          push "encode(v2) as x2" to child(v2)

            List<NamedExpression> newOutput = Lists.newArrayListWithCapacity(op.getOutput().size());
            List<List<SlotReference>> newRegularOutputs = Lists.newArrayListWithCapacity(op.getOutput().size());
            for (int cid = 0; cid < op.children().size(); cid++) {
                newRegularOutputs.add(Lists.newArrayList(op.getRegularChildOutput(cid)));
            }

            for (int oid = 0; oid < op.getOutput().size(); oid++) {
                NamedExpression e = op.getOutput().get(oid);
                boolean changed = false;
                for (Alias alias : ctx.encodeAliases) {
                    if (alias.child().child(0).equals(e)) {
                        newOutput.add(alias.toSlot());
                        changed = true;
                        EncodeString encode = (EncodeString) alias.child();
                        ctx.toBePused.add(alias);
                        for (int cid = 0; cid < op.children().size(); cid++) {
                            Plan child = op.child(cid);
                            ctx.toBePushedToChild.putIfAbsent(child, new ArrayList<>());
                            Alias aliasForChild = new Alias(
                                    encode.withChildren(op.getRegularChildrenOutputs().get(cid).get(oid)));
                            ctx.toBePushedToChild.get(child).add(aliasForChild);
                            newRegularOutputs.get(cid).set(oid, (SlotReference) aliasForChild.toSlot());
                        }
                        break;
                    }
                }
                if (!changed) {
                    newOutput.add(e);
                }
            }
            op = op.withNewOutputs(newOutput);

            //rewrite children
            List<Plan> newChildren = Lists.newArrayListWithCapacity(op.children().size());
            for (Plan child : op.children()) {
                if (!ctx.toBePushedToChild.get(child).isEmpty()) {
                    PushDownContext childCtx = new PushDownContext(child, ctx.toBePushedToChild.get(child));
                    Plan newChild = child.accept(this, childCtx);
                    newChildren.add(newChild);
                } else {
                    newChildren.add(child);
                }
            }
            op = op.withChildrenAndTheirOutputs(newChildren, newRegularOutputs);
            return op;
        }
    }
}
