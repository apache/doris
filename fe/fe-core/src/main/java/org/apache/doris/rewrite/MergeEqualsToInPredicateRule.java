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
import org.apache.doris.nereids.trees.expressions.functions.scalar.Bin;
import org.apache.doris.rewrite.ExprRewriter.ClauseType;

import com.clearspring.analytics.util.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MergeEqualsToInPredicateRule implements ExprRewriteRule {
    public static MergeEqualsToInPredicateRule INSTANCE = new MergeEqualsToInPredicateRule();
    private static final int MERGE_LIMIT = 5;

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ClauseType clauseType) throws AnalysisException {
        if (expr == null) {
            return expr;
        }
        List<Expr> disconjuncts = getDisconjuncts(expr);
        if(disconjuncts.size() < MERGE_LIMIT) {
            return expr;
        }
        Map<SlotRef, List<Expr>> equalMap = new HashMap<>();
        Expr result = null;
        for (Expr disconj: disconjuncts) {
            if (disconj instanceof BinaryPredicate
                    && ((BinaryPredicate) disconj).getOp() == BinaryPredicate.Operator.EQ) {
                BinaryPredicate binary = (BinaryPredicate) disconj;
                if (binary.getChild(0) instanceof SlotRef
                        && binary.getChild(1) instanceof LiteralExpr) {
                    equalMap.computeIfAbsent((SlotRef) binary.getChild(0), k -> Lists.newArrayList());
                    equalMap.get((SlotRef) binary.getChild(0)).add((LiteralExpr) binary.getChild(1));
                } else {
                    result = addDisconjunct(result, disconj);
                }
            } else {
                result = addDisconjunct(result, disconj);
            }
        }

        for(Entry<SlotRef, List<Expr>> entry: equalMap.entrySet()) {
            if (entry.getValue().size() >= MERGE_LIMIT) {
                InPredicate in  = new InPredicate(entry.getKey(), entry.getValue(), false);
                result = addDisconjunct(result, in);
            } else {
                for(Expr right: entry.getValue()) {
                    result = addDisconjunct(result,
                            new BinaryPredicate(BinaryPredicate.Operator.EQ,
                                    entry.getKey(),
                                    right));
                }
            }
        }
        return result;
    }

    private Expr addDisconjunct(Expr result, Expr conj) {
        if (result == null) {
            return conj;
        } else {
            return new CompoundPredicate(Operator.OR, result, conj);
        }
    }

    private Expr addConjunct(Expr result, Expr conj) {
        if (result == null) {
            return conj;
        } else {
            return new CompoundPredicate(Operator.AND, result, conj);
        }
    }
    private List<Expr> getDisconjuncts(Expr expr) {
        List<Expr> result = Lists.newArrayList();
        if (expr instanceof CompoundPredicate ) {
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
    //explain select * from region where r_regionkey= 1 or r_regionkey= 2 or r_regionkey= 3 or r_regionkey= 4 or r_regionkey= 5;
    //    explain select * from region where r_regionkey= 1 or r_regionkey= 2 or r_regionkey= 3 or r_regionkey= 4 or r_regionkey= 5 and r_name != "CHINA";
    //    explain select * from region where r_regionkey= 1 or r_regionkey= 2 or r_regionkey= 3 or r_regionkey= 4 or r_regionkey= 5 or r_regionkey=6 and r_name != "CHINA";
    //    explain select * from region where r_regionkey= 1 or r_regionkey= 2 or r_regionkey= 3 or r_regionkey= 4 or r_name = "ALL" or r_regionkey= 5 or r_regionkey=6 and r_name != "CHINA";