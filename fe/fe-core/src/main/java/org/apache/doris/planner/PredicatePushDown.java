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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.Predicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleId;

import org.apache.directory.api.util.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class PredicatePushDown {
    private final static Logger LOG = LogManager.getLogger(PredicatePushDown.class);

    public static PlanNode visitScanNode(ScanNode scanNode, JoinOperator joinOp, Analyzer analyzer) {
        switch (joinOp) {
            case INNER_JOIN:
            case LEFT_OUTER_JOIN:
                predicateFromLeftSidePropagatesToRightSide(scanNode, analyzer);
                break;
            // TODO
            default:
                break;
        }
        return scanNode;
    }

    private static void predicateFromLeftSidePropagatesToRightSide(ScanNode scanNode, Analyzer analyzer) {
        List<TupleId> tupleIdList = scanNode.getTupleIds();
        if (tupleIdList.size() != 1) {
            LOG.info("The predicate pushdown is not reflected "
                            + "because the scan node involves more then one tuple:{}",
                    Strings.listToString(tupleIdList));
            return;
        }
        TupleId rightSideTuple = tupleIdList.get(0);
        List<Expr> unassignedRightSideConjuncts = analyzer.getUnassignedConjuncts(scanNode);
        List<Expr> eqJoinPredicates = analyzer.getEqJoinConjuncts(rightSideTuple);
        if (eqJoinPredicates != null) {
            List<Expr> allConjuncts = analyzer.getConjuncts(analyzer.getAllTupleIds());
            allConjuncts.removeAll(unassignedRightSideConjuncts);
            for (Expr conjunct : allConjuncts) {
                if (!Predicate.canPushDownPredicate(conjunct)) {
                    continue;
                }
                for (Expr eqJoinPredicate : eqJoinPredicates) {
                    // we can ensure slot is left node, because NormalizeBinaryPredicatesRule
                    SlotRef otherSlot = conjunct.getChild(0).unwrapSlotRef();

                    // ensure the children for eqJoinPredicate both be SlotRef
                    if (eqJoinPredicate.getChild(0).unwrapSlotRef() == null
                            || eqJoinPredicate.getChild(1).unwrapSlotRef() == null) {
                        continue;
                    }

                    SlotRef leftSlot = eqJoinPredicate.getChild(0).unwrapSlotRef();
                    SlotRef rightSlot = eqJoinPredicate.getChild(1).unwrapSlotRef();
                    // ensure the type is match
                    if (!leftSlot.getDesc().getType().matchesType(rightSlot.getDesc().getType())) {
                        continue;
                    }

                    // example: t1.id = t2.id and t1.id = 1  => t2.id =1
                    if (otherSlot.isBound(leftSlot.getSlotId())
                            && rightSlot.isBound(rightSideTuple)) {
                        Expr pushDownConjunct = rewritePredicate(analyzer, conjunct, rightSlot);
                        LOG.debug("pushDownConjunct: {}", pushDownConjunct);
                        scanNode.addConjunct(pushDownConjunct);
                    } else if (otherSlot.isBound(rightSlot.getSlotId())
                            && leftSlot.isBound(rightSideTuple)) {
                        Expr pushDownConjunct = rewritePredicate(analyzer, conjunct, leftSlot);
                        LOG.debug("pushDownConjunct: {}", pushDownConjunct);
                        scanNode.addConjunct(pushDownConjunct);
                    }
                }
            }
        }
    }

    // Rewrite the oldPredicate with new leftChild
    // For example: oldPredicate is t1.id = 1, leftChild is t2.id, will return t2.id = 1
    private static Expr rewritePredicate(Analyzer analyzer, Expr oldPredicate, Expr leftChild) {
        if (oldPredicate instanceof BinaryPredicate) {
            BinaryPredicate oldBP = (BinaryPredicate) oldPredicate;
            BinaryPredicate bp = new BinaryPredicate(oldBP.getOp(), leftChild, oldBP.getChild(1));
            bp.analyzeNoThrow(analyzer);
            return bp;
        }

        if (oldPredicate instanceof InPredicate) {
            InPredicate oldIP = (InPredicate) oldPredicate;
            InPredicate ip = new InPredicate(leftChild, oldIP.getListChildren(), oldIP.isNotIn());
            ip.analyzeNoThrow(analyzer);
            return ip;
        }

        return oldPredicate;
    }
}
