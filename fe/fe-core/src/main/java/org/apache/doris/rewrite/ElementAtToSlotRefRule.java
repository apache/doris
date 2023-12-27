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

package org.apache.doris.rewrite;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.rewrite.ExprRewriter.ClauseType;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Transform element_at function to SlotReference for variant sub-column access.
 * This optimization will help query engine to prune as many sub columns as possible
 * to speed up query.
 * eg: element_at(element_at(v, "a"), "b") -> SlotReference(column=v, subColLabels=["a", "b"])
 */

public class ElementAtToSlotRefRule implements ExprRewriteRule  {
    public static final ElementAtToSlotRefRule INSTANCE = new ElementAtToSlotRefRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ClauseType clauseType) throws AnalysisException {
        // Only check element at of variant all rewrited to slots
        List<Expr> elementAtFunctions = Lists.newArrayList();
        getElementAtFunction(expr, elementAtFunctions);
        if (!elementAtFunctions.isEmpty()) {
            throw new AnalysisException("element_at should not appear in common rewrite stage");
        }
        return expr;
    }

    private Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException  {
        if (!(isElementAtOfVariantType(expr))) {
            return expr;
        }
        List<SlotRef> slotRefs =  Lists.newArrayList();
        expr.collect(SlotRef.class, slotRefs);
        if (slotRefs.size() != 1) {
            throw new AnalysisException("only support syntax like v[\"a\"][\"b\"][\"c\"]");
        }
        SlotRef slot = slotRefs.get(0);
        List<Expr> pathsExpr = Lists.newArrayList();
        // Traverse the expression tree to gather literals.
        // For instance, consider the expression v["a"]["b"]["c"], where it's represented as
        // element_at(element_at(element_at(v, 'a'), 'b'), 'c').The pathsExpr will contain
        // literals ['a', 'b', 'c'] representing the sequence of keys in the structure.
        expr.collect(Expr::isLiteral, pathsExpr);
        List<String> fullPaths = pathsExpr.stream()
                .map(node -> ((LiteralExpr) node).getStringValue())
                .collect(Collectors.toList());
        slot.setSubColPath(fullPaths);
        slot.analyzeImpl(analyzer);
        return slot;
    }

    // check if expr is element_at with variant slot type
    private static boolean isElementAtOfVariantType(Expr expr) {
        if (!(expr instanceof FunctionCallExpr)) {
            return false;
        }
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
        List<SlotRef> slotRefs =  Lists.newArrayList();
        expr.collect(SlotRef.class, slotRefs);
        if (slotRefs.size() != 1) {
            return false;
        }
        return functionCallExpr.getFnName().getFunction().equalsIgnoreCase("element_at")
                && slotRefs.get(0).getType().isVariantType();
    }

    public static boolean containsElementAtFunction(Expr expr) {
        List<Expr> result = Lists.newArrayList();
        getElementAtFunction(expr, result);
        return !result.isEmpty();
    }

    private static void getElementAtFunction(Expr expr, List<Expr> result) {
        if (isElementAtOfVariantType(expr)) {
            result.add(expr);
            return;
        }
        for (Expr child : expr.getChildren()) {
            getElementAtFunction(child, result);
        }
    }

    public Expr rewrite(Expr inputExpr, Analyzer analyzer)
            throws AnalysisException {
        List<Expr> originalFunctionElementAtExprs = Lists.newArrayList();
        boolean changed = false;
        Expr newExpr = inputExpr.clone();
        getElementAtFunction(inputExpr, originalFunctionElementAtExprs);
        for (Expr expr : originalFunctionElementAtExprs) {
            Expr rewriteExpr = apply(expr, analyzer);
            if (inputExpr.getId().equals(expr.getId())) {
                return rewriteExpr;
            }
            if (rewriteExpr != expr) {
                changed = true;
                replaceChildExpr(newExpr, expr.getId().toString(), rewriteExpr);
            }
        }
        return changed ? newExpr : inputExpr;
    }

    // Find child expr which id matches key and replace this child expr
    // with replacExpr and set replacExpr with same expr id.
    private void replaceChildExpr(Expr expr, String key, Expr replacExpr) {
        // ATTN: make sure the child order of expr keep unchanged
        for (int i = 0; i < expr.getChildren().size(); i++) {
            Expr child = expr.getChild(i);
            if ((isElementAtOfVariantType(child)) && key.equals(child.getId().toString())) {
                replacExpr.setId(child.getId());
                expr.setChild(i, replacExpr);
                break;
            }
            replaceChildExpr(child, key, replacExpr);
        }
    }
}
