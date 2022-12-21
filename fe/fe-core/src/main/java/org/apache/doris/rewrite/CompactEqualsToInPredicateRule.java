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
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rewrite.ExprRewriter.ClauseType;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/*
a = 1 or a = 2 or a = 3 or a in (4, 5, 6) => a in (1, 2, 3, 4, 5, 6)
 */
public class CompactEqualsToInPredicateRule implements ExprRewriteRule {
    public static CompactEqualsToInPredicateRule INSTANCE = new CompactEqualsToInPredicateRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ClauseType clauseType) throws AnalysisException {
        if (expr == null) {
            return expr;
        }
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate comp = (CompoundPredicate) expr;
            if (comp.getOp() == Operator.OR) {
                Pair<Boolean, Expr> compactResult = compactEqualsToInPredicate(expr);
                if (compactResult.first) {
                    return compactResult.second;
                }
            }
        }
        return expr;
    }

    /*
    expr in form of A or B or ...
     */
    private Pair<Boolean, Expr> compactEqualsToInPredicate(Expr expr) {
        int compactThreshold = ConnectContext.get().getSessionVariable().getCompactEqualToInPredicateThreshold();
        boolean changed = false;
        List<Expr> disConjuncts = getDisconjuncts(expr);
        if (disConjuncts.size() < compactThreshold) {
            return Pair.of(false, expr);
        }
        Map<SlotRef, Set<Expr>> equalMap = new HashMap<>();
        Map<SlotRef, InPredicate> inPredMap = new HashMap<>();
        Expr result = null;
        for (Expr disConj : disConjuncts) {
            if (disConj instanceof BinaryPredicate
                    && ((BinaryPredicate) disConj).getOp() == BinaryPredicate.Operator.EQ) {
                BinaryPredicate binary = (BinaryPredicate) disConj;
                if (binary.getChild(0) instanceof SlotRef
                        && binary.getChild(1) instanceof LiteralExpr) {
                    equalMap.computeIfAbsent((SlotRef) binary.getChild(0), k -> new HashSet<>());
                    equalMap.get((SlotRef) binary.getChild(0)).add((LiteralExpr) binary.getChild(1));
                } else if (binary.getChild(0) instanceof LiteralExpr
                        && binary.getChild(1) instanceof SlotRef) {
                    equalMap.computeIfAbsent((SlotRef) binary.getChild(1), k -> new HashSet<>());
                    equalMap.get((SlotRef) binary.getChild(1)).add((LiteralExpr) binary.getChild(0));
                } else {
                    result = addDisconjunct(result, disConj);
                }
            } else if (disConj instanceof InPredicate && !((InPredicate) disConj).isNotIn()) {
                InPredicate in = (InPredicate) disConj;
                Expr compareExpr = in.getChild(0);
                if (compareExpr instanceof SlotRef) {
                    SlotRef slot = (SlotRef) compareExpr;
                    InPredicate val = inPredMap.get(slot);
                    if (val == null) {
                        inPredMap.put(slot, in);
                    } else {
                        val.getChildren().addAll(in.getListChildren());
                        inPredMap.put(slot, val);
                    }
                }
            } else {
                result = addDisconjunct(result, disConj);
            }
        }

        for (Entry<SlotRef, Set<Expr>> entry : equalMap.entrySet()) {
            SlotRef slot = entry.getKey();
            InPredicate in = inPredMap.get(slot);
            if (entry.getValue().size() >= compactThreshold || in != null) {
                if (in == null) {
                    in = new InPredicate(entry.getKey(), Lists.newArrayList(entry.getValue()), false);
                    inPredMap.put(slot, in);
                } else {
                    in.getChildren().addAll(Lists.newArrayList(entry.getValue()));
                }
                changed = true;
            } else {
                for (Expr right : entry.getValue()) {
                    result = addDisconjunct(result,
                            new BinaryPredicate(BinaryPredicate.Operator.EQ,
                                    entry.getKey(),
                                    right));
                }
            }
        }
        for (InPredicate in : inPredMap.values()) {
            result = addDisconjunct(result, in);
        }
        return Pair.of(changed, result);
    }

    private Expr addDisconjunct(Expr result, Expr conj) {
        if (result == null) {
            return conj;
        } else {
            return new CompoundPredicate(Operator.OR, result, conj);
        }
    }

    private List<Expr> getDisconjuncts(Expr expr) {
        List<Expr> result = new ArrayList<>();
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate comp = ((CompoundPredicate) expr);
            if (comp.getOp() == Operator.OR) {
                result.addAll(getDisconjuncts(comp.getChild(0)));
                result.addAll(getDisconjuncts(comp.getChild(1)));
            } else {
                result.add(expr);
            }
        } else {
            result.add(expr);
        }
        return result;
    }
}
