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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.rewrite.ExprRewriter.ClauseType;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
        SlotRef slot = slotRefs.get(0);
        List<Expr> pathsExpr = Lists.newArrayList();
        expr.collect(Expr::isLiteral, pathsExpr);
        List<String> fullPaths = pathsExpr.stream()
                .map(node -> ((LiteralExpr) node).getStringValue())
                .collect(Collectors.toList());
        slot.setSubColPath(fullPaths);
        slot.analyzeImpl(analyzer);
        return slot;
    }

    private boolean isElementAtOfVariantType(Expr expr) {
        if (!(expr instanceof FunctionCallExpr)) {
            return false;
        }
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
        return functionCallExpr.getFnName().getFunction().equalsIgnoreCase("element_at")
                && functionCallExpr.getType() == Type.VARIANT;
    }

    private void getElementAtFunction(Expr expr, List<Expr> result) {
        if (isElementAtOfVariantType(expr)) {
            result.add(expr);
            return;
        }
        for (Expr child : expr.getChildren()) {
            if (isElementAtOfVariantType(expr)) {
                // get the top level element at function, ignore its child
                result.add(child);
                continue;
            }
            getElementAtFunction(child, result);
        }
    }

    public Expr rewrite(Expr inputExpr, Analyzer analyzer)
            throws AnalysisException {
        List<Expr> originalFunctionElementAtExprs = Lists.newArrayList();
        Expr newExpr = null;
        getElementAtFunction(inputExpr, originalFunctionElementAtExprs);
        for (Expr expr : originalFunctionElementAtExprs) {
            Expr rewriteExpr = apply(expr, analyzer);
            if (rewriteExpr != expr) {
                if (newExpr == null) {
                    newExpr = inputExpr.clone();
                }
                newExpr = replaceExpr(newExpr, expr.getId().toString(), rewriteExpr);
            }
        }
        return newExpr != null ? newExpr : inputExpr;
    }

    public boolean apply(Map<String, Expr> exprMap, Analyzer analyzer)
            throws AnalysisException {
        boolean changed = false;
        for (Entry<String, Expr> entry : exprMap.entrySet()) {
            List<Expr> originalFunctionElementAtExprs = Lists.newArrayList();
            getElementAtFunction(entry.getValue(), originalFunctionElementAtExprs);
            Expr originalExpr = entry.getValue();
            for (Expr expr : originalFunctionElementAtExprs) {
                Expr rewriteExpr = apply(expr, analyzer, null);
                if (rewriteExpr != expr) {
                    Expr newExpr = replaceExpr(originalExpr, expr.getId().toString(), rewriteExpr);
                    exprMap.put(entry.getKey(), newExpr);
                    changed = true;
                }
            }
        }
        return changed;
    }

    private Expr replaceExpr(Expr expr, String key, Expr replacExpr) {
        if (expr.getId().toString().equals(key)) {
            return replacExpr;
        }
        // ATTN: make sure the child order of expr keep unchanged
        for (int i = 0; i < expr.getChildren().size(); i++) {
            Expr child = expr.getChild(i);
            if ((isElementAtOfVariantType(child)) && key.equals(child.getId().toString())) {
                replacExpr.setId(child.getId());
                expr.setChild(i, replacExpr);
                break;
            }
            replaceExpr(child, key, replacExpr);
        }
        return expr;
    }
}
