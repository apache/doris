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

package org.apache.doris.planner.normalize;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.ExprVisitor;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.LambdaFunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TSlotRef;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Visitor that produces a normalized {@link TExprNode} for query cache support.
 * Normalization maps slot IDs and tuple IDs through the {@link Normalizer} so that
 * structurally equivalent queries produce identical thrift representations.
 *
 * <p>Only {@link SlotRef} and {@link FunctionCallExpr} have custom normalize logic;
 * all other expression types delegate to {@link ExprToThriftVisitor}.
 */
public class ExprNormalizeVisitor extends ExprVisitor<Void, TExprNode> {

    private final Normalizer normalizer;

    public ExprNormalizeVisitor(Normalizer normalizer) {
        this.normalizer = normalizer;
    }

    /**
     * Normalize an entire expression tree into a {@link TExpr}.
     * Replaces {@code expr.normalize(normalizer)}.
     */
    public static TExpr normalize(Expr expr, Normalizer normalizer) {
        TExpr result = new TExpr();
        ExprNormalizeVisitor visitor = new ExprNormalizeVisitor(normalizer);
        ExprToThriftVisitor.treeToThriftHelper(expr, result, visitor);
        return result;
    }

    @Override
    public Void visit(Expr expr, TExprNode msg) {
        // Default: delegate to the standard toThrift visitor via double dispatch
        expr.accept(ExprToThriftVisitor.INSTANCE, msg);
        return null;
    }

    @Override
    public Void visitSlotRef(SlotRef expr, TExprNode msg) {
        msg.node_type = TExprNodeType.SLOT_REF;
        // Normalize slot_id and set tuple_id=0 to enable cache reuse across different tuple contexts
        msg.slot_ref = new TSlotRef(
                normalizer.normalizeSlotId(expr.getDesc().getId().asInt()),
                0
        );
        msg.slot_ref.setColUniqueId(expr.getDesc().getUniqueId());
        return null;
    }

    @Override
    public Void visitFunctionCallExpr(FunctionCallExpr expr, TExprNode msg) {
        String functionName = expr.getFnName().getFunction().toUpperCase();
        if (nonDeterministicFunctions.contains(functionName)
                || (nonDeterministicTimeFunctions.contains(functionName)
                        && expr.getChildren().isEmpty())) {
            throw new IllegalStateException("Can not normalize non deterministic functions");
        }
        // Delegate to standard toThrift for deterministic functions
        return ExprToThriftVisitor.INSTANCE.visitFunctionCallExpr(expr, msg);
    }

    @Override
    public Void visitLambdaFunctionCallExpr(LambdaFunctionCallExpr expr, TExprNode msg) {
        // LambdaFunctionCallExpr extends FunctionCallExpr; apply the same non-determinism check
        return visitFunctionCallExpr(expr, msg);
    }

    // All other visitXxx methods inherit from ExprVisitor and call visit(),
    // which delegates to ExprToThriftVisitor.INSTANCE.

    private static final Set<String> nonDeterministicFunctions =
            ImmutableSet.<String>builder()
                    .add("RAND")
                    .add("RANDOM")
                    .add("RANDOM_BYTES")
                    .add("CONNECTION_ID")
                    .add("DATABASE")
                    .add("USER")
                    .add("UUID")
                    .add("CURRENT_USER")
                    .add("UUID_NUMERIC")
                    .build();

    private static final Set<String> nonDeterministicTimeFunctions =
            ImmutableSet.<String>builder()
                    .add("NOW")
                    .add("CURDATE")
                    .add("CURRENT_DATE")
                    .add("UTC_TIMESTAMP")
                    .add("CURTIME")
                    .add("CURRENT_TIMESTAMP")
                    .add("CURRENT_TIME")
                    .add("UNIX_TIMESTAMP")
                    .add()
                    .build();
}
