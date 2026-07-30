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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

/** OneListPartitionInputs */
public class OneListPartitionEvaluator<K>
        extends DefaultExpressionRewriter<Map<Slot, PartitionSlotInput>> implements OnePartitionEvaluator<K> {
    private final K partitionIdent;
    private final List<Slot> partitionSlots;
    private final ListPartitionItem partitionItem;
    private final ExpressionRewriteContext expressionRewriteContext;
    // Position-aligned with partitionSlots. A true entry means the corresponding
    // partition key is the output of a function expression (e.g. date_trunc(dt,'day')),
    // not the raw slot value. For those positions we must NOT replace the slot with
    // the stored key literal, because the filter references the raw slot (dt) whose
    // values do not equal the truncated key. Instead we leave the slot un-bound,
    // which forces evaluate() to produce a non-boolean, causing PartitionPruner to
    // keep the partition (safe over-scan rather than unsafe under-scan).
    private final boolean[] slotHasPartitionFunction;

    public OneListPartitionEvaluator(K partitionIdent, List<Slot> partitionSlots,
            ListPartitionItem partitionItem, CascadesContext cascadesContext) {
        this(partitionIdent, partitionSlots, partitionItem, cascadesContext, null);
    }

    /**
     * Constructor with per-position partition expressions. When a position's expression is a
     * function call (e.g. date_trunc(dt,'day')) the corresponding partition key is the function
     * output rather than the raw slot value; we skip literal binding for that slot to keep the
     * partition (safe over-scan) instead of misjudging equality with the truncated key.
     */
    public OneListPartitionEvaluator(K partitionIdent, List<Slot> partitionSlots,
            ListPartitionItem partitionItem, CascadesContext cascadesContext,
            List<Expr> partitionExprs) {
        this.partitionIdent = partitionIdent;
        this.partitionSlots = Objects.requireNonNull(partitionSlots, "partitionSlots cannot be null");
        this.partitionItem = Objects.requireNonNull(partitionItem, "partitionItem cannot be null");
        this.expressionRewriteContext = new ExpressionRewriteContext(
                Objects.requireNonNull(cascadesContext, "cascadesContext cannot be null"));
        this.slotHasPartitionFunction = new boolean[partitionSlots.size()];
        if (partitionExprs != null) {
            int n = Math.min(partitionSlots.size(), partitionExprs.size());
            for (int i = 0; i < n; i++) {
                slotHasPartitionFunction[i] = partitionExprs.get(i) instanceof FunctionCallExpr;
            }
        }
    }

    @Override
    public K getPartitionIdent() {
        return partitionIdent;
    }

    @Override
    public List<Map<Slot, PartitionSlotInput>> getOnePartitionInputs() {
        if (partitionSlots.size() == 1) {
            // fast path
            return getInputsByOneSlot();
        } else {
            // slow path
            return getInputsByMultiSlots();
        }
    }

    private List<Map<Slot, PartitionSlotInput>> getInputsByOneSlot() {
        ImmutableList.Builder<Map<Slot, PartitionSlotInput>> inputs
                = ImmutableList.builderWithExpectedSize(partitionItem.getItems().size());
        Slot slot = partitionSlots.get(0);
        // If this slot's partition key is a function output, we cannot substitute
        // the raw slot with the stored literal. Emit a single empty binding: every
        // predicate referencing this slot stays symbolic and the partition is kept.
        if (slotHasPartitionFunction[0]) {
            inputs.add(ImmutableMap.of());
            return inputs.build();
        }
        for (PartitionKey item : partitionItem.getItems()) {
            LiteralExpr legacy = item.getKeys().get(0);
            inputs.add(ImmutableMap.of(
                    slot,
                    new PartitionSlotInput(Literal.fromLegacyLiteral(legacy, legacy.getType()), ImmutableMap.of()))
            );
        }
        return inputs.build();
    }

    private List<Map<Slot, PartitionSlotInput>> getInputsByMultiSlots() {
        return partitionItem.getItems().stream()
                .map(keys -> {
                    List<Literal> literals = keys.getKeys()
                            .stream()
                            .map(literal -> Literal.fromLegacyLiteral(literal, literal.getType()))
                            .collect(ImmutableList.toImmutableList());

                    Map<Slot, PartitionSlotInput> row = new HashMap<>();
                    IntStream.range(0, partitionSlots.size()).forEach(index -> {
                        // Skip binding for positions whose partition key came from a function
                        // expression: leaving the slot un-bound is the safe default that
                        // avoids incorrect equality against the truncated key.
                        if (slotHasPartitionFunction[index]) {
                            return;
                        }
                        Slot partitionSlot = partitionSlots.get(index);
                        Literal literal = literals.get(index);
                        // list partition don't need to compute the slot's range,
                        // so we pass through an empty map
                        row.put(partitionSlot, new PartitionSlotInput(literal, ImmutableMap.of()));
                    });
                    return ImmutableMap.copyOf(row);
                }).collect(ImmutableList.toImmutableList());
    }

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, Map<Slot, PartitionSlotInput> context) {
        if (!inPredicate.optionsAreLiterals()) {
            return super.visitInPredicate(inPredicate, context);
        }

        Expression newCompareExpr = inPredicate.getCompareExpr().accept(this, context);
        if (newCompareExpr.isNullLiteral()) {
            return NullLiteral.BOOLEAN_INSTANCE;
        }

        try {
            // fast path
            boolean contains = inPredicate.getLiteralOptionSet().contains(newCompareExpr);
            if (contains) {
                return BooleanLiteral.TRUE;
            }
            if (inPredicate.optionsContainsNullLiteral()) {
                return NullLiteral.BOOLEAN_INSTANCE;
            }
            // If the compare expression is still a Slot (i.e. it references a slot whose
            // partition key is a function output and therefore un-bound), we cannot conclude
            // FALSE — fall through to the default rewriter which returns a non-boolean.
            if (!(newCompareExpr instanceof Literal)) {
                return super.visitInPredicate(inPredicate, context);
            }
            return BooleanLiteral.FALSE;
        } catch (Throwable t) {
            // slow path
            return super.visitInPredicate(inPredicate, context);
        }
    }

    @Override
    public Expression visit(Expression expr, Map<Slot, PartitionSlotInput> context) {
        expr = super.visit(expr, context);
        if (!(expr instanceof Literal)) {
            // just forward to fold constant rule
            return FoldConstantRuleOnFE.evaluate(expr, expressionRewriteContext);
        }
        return expr;
    }

    @Override
    public Expression visitSlot(Slot slot, Map<Slot, PartitionSlotInput> context) {
        // replace partition slot to literal, but only when it is bound.
        // Unbound slots (partition function positions) stay as-is so that any
        // predicate referencing them cannot fold to a boolean and the partition
        // is conservatively retained.
        PartitionSlotInput partitionSlotInput = context.get(slot);
        return partitionSlotInput == null ? slot : partitionSlotInput.result;
    }

    @Override
    public Expression evaluate(Expression expression, Map<Slot, PartitionSlotInput> currentInputs) {
        return expression.accept(this, currentInputs);
    }

    @Override
    public boolean isDefaultPartition() {
        return partitionItem.isDefaultPartition();
    }
}
