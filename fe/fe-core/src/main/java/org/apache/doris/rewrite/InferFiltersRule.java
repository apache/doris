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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;

import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The function of this rule is to derive a new predicate based on the current predicate.
 *
 * <pre>
 * eg.
 * t1.id = t2.id and t2.id = t3.id and t3.id = 100;
 * -->
 * t1.id = 100 and t2.id = 100 and t3.id = 100;
 *
 * 1. Register a new rule InferFiltersRule and add it to GlobalState.
 * 2. Traverse Conjunct to construct on/where equivalence connection, numerical connection and isNullPredicate.
 * 3. Use Warshall to infer all equivalence connections.
 *    details: <a href="url">https://en.wikipedia.org/wiki/Floyd%E2%80%93Warshall_algorithm</a>
 * 4. Construct additional numerical connections and isNullPredicate.
 * </pre>
 */
public class InferFiltersRule implements ExprRewriteRule {
    private static final Logger LOG = LogManager.getLogger(InferFiltersRule.class);
    public static InferFiltersRule INSTANCE = new InferFiltersRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        if (expr == null) {
            return null;
        }

        if (!analyzer.enableInferPredicate() || clauseType == ExprRewriter.ClauseType.OTHER_CLAUSE) {
            return expr;
        }

        if (!clauseType.isInferable()) {
            return expr;
        }

        // slotEqSlotExpr: Record existing and infer equivalent connections
        List<Expr> slotEqSlotExpr = analyzer.getOnSlotEqSlotExpr();

        // slotEqSlotDeDuplication: De-Duplication for slotEqSlotExpr
        Set<Pair<Expr, Expr>> slotEqSlotDeDuplication = (clauseType.isOnClause())
                ? analyzer.getOnSlotEqSlotDeDuplication() : Sets.newHashSet();

        // slotToLiteralExpr: Record existing and infer expr which slot and literal are equal
        List<Expr> slotToLiteralExpr = analyzer.getOnSlotToLiteralExpr();

        // slotToLiteralDeDuplication: De-Duplication for slotToLiteralExpr
        Set<Pair<Expr, Expr>> slotToLiteralDeDuplication = (clauseType.isOnClause())
                ? analyzer.getOnSlotToLiteralDeDuplication() : Sets.newHashSet();

        // newExprWithState: just record infer expr which slot and literal are equal and which is not null predicate
        // false : Unexecutable intermediate results will be produced during the derivation process.
        // true : The new expr will be add to expr.
        List<Pair<Expr, Boolean>> newExprWithState = new ArrayList<>();

        // isNullExpr: Record existing and infer not null predicate
        List<Expr> isNullExpr = analyzer.getOnIsNullExpr();

        // isNullDeDuplication: De-Duplication for isNullExpr
        Set<Expr> isNullDeDuplication = (clauseType.isOnClause())
                ? analyzer.getOnIsNullDeDuplication() : Sets.newHashSet();

        // inExpr: Record existing and infer in predicate
        List<Expr> inExpr = analyzer.getInExpr();

        // inDeDuplication: De-Duplication for inExpr
        Set<Expr> inDeDuplication =
                (clauseType.isOnClause()) ? analyzer.getInDeDuplication() : Sets.newHashSet();

        // exprToWarshallArraySubscript/warshallArraySubscriptToExpr:
        // function is easy to build warshall and newExprWithState
        Map<Expr, Integer> exprToWarshallArraySubscript = new HashMap<>();
        Map<Integer, Expr> warshallArraySubscriptToExpr = new HashMap<>();


        initAllStructure(expr, slotEqSlotExpr, slotEqSlotDeDuplication,
                slotToLiteralExpr, slotToLiteralDeDuplication,
                isNullExpr, isNullDeDuplication,
                inExpr, inDeDuplication, analyzer, clauseType);


        genNewSlotEqSlotPredicate(slotEqSlotExpr, slotEqSlotDeDuplication, exprToWarshallArraySubscript,
                warshallArraySubscriptToExpr, analyzer, clauseType);

        inferSlotToLiteralPredicates(slotEqSlotExpr, slotToLiteralDeDuplication,
                slotToLiteralExpr, newExprWithState, analyzer, clauseType);

        inferIsNotNullPredicates(slotEqSlotExpr, isNullExpr,
                isNullDeDuplication, newExprWithState, analyzer, clauseType);

        inferInPredicate(slotEqSlotExpr, inDeDuplication,
                inExpr, newExprWithState, analyzer, clauseType);

        if (!newExprWithState.isEmpty()) {
            Expr rewriteExpr = expr;
            for (Pair<Expr, Boolean> exprBooleanPair : newExprWithState) {
                if (exprBooleanPair.second) {
                    rewriteExpr = new CompoundPredicate(CompoundPredicate.Operator.AND, rewriteExpr,
                            exprBooleanPair.first);
                }
            }
            return rewriteExpr;
        }

        return expr;
    }

    /**
     *  Initialize all data structures, count and connect compound predicates.
     *  Recursively traverse the compoundPredicate that is and, and add it to different structures according to the type
     *  @param slotEqSlotExpr: Expr is BinaryPredicate. Left expr is slot and right expr is slot;
     *  @param slotToLiteralExpr: Expr is BinaryPredicate. Left expr is slot and right expr is Literal;
     *  @param isNullExpr: Expr is isNullPredicate;
     */
    private void initAllStructure(Expr conjunct,
                                  List<Expr> slotEqSlotExpr,
                                  Set<Pair<Expr, Expr>> slotEqSlotDeDuplication,
                                  List<Expr> slotToLiteralExpr,
                                  Set<Pair<Expr, Expr>> slotToLiteralDeDuplication,
                                  List<Expr> isNullExpr,
                                  Set<Expr> isNullDeDuplication,
                                  List<Expr> inExpr,
                                  Set<Expr> inDeDuplication,
                                  Analyzer analyzer,
                                  ExprRewriter.ClauseType clauseType) {
        if (conjunct instanceof CompoundPredicate
                && ((CompoundPredicate) conjunct).getOp() == CompoundPredicate.Operator.AND) {
            for (int index = 0; index < conjunct.getChildren().size(); ++index) {
                initAllStructure(conjunct.getChild(index), slotEqSlotExpr,
                        slotEqSlotDeDuplication, slotToLiteralExpr,
                        slotToLiteralDeDuplication, isNullExpr,
                        isNullDeDuplication, inExpr, inDeDuplication,
                        analyzer, clauseType);
            }
        }

        if (conjunct instanceof BinaryPredicate
                && conjunct.getChild(0) != null
                && conjunct.getChild(1) != null) {
            if (conjunct.getChild(0).unwrapSlotRef() != null
                    && conjunct.getChild(1) instanceof LiteralExpr) {
                Pair<Expr, Expr> pair = Pair.of(conjunct.getChild(0).unwrapSlotRef(), conjunct.getChild(1));
                if (!slotToLiteralDeDuplication.contains(pair)) {
                    slotToLiteralDeDuplication.add(pair);
                    slotToLiteralExpr.add(conjunct);
                    if (clauseType.isOnClause()) {
                        analyzer.registerOnSlotToLiteralDeDuplication(pair);
                        analyzer.registerOnSlotToLiteralExpr(conjunct);
                    }
                    analyzer.registerGlobalSlotToLiteralDeDuplication(pair);
                }
            } else if (((BinaryPredicate) conjunct).getOp().isEquivalence()
                    && conjunct.getChild(0).unwrapSlotRef() != null
                    && conjunct.getChild(1).unwrapSlotRef() != null) {
                Pair<Expr, Expr> pair = Pair.of(conjunct.getChild(0).unwrapSlotRef(),
                                                   conjunct.getChild(1).unwrapSlotRef());
                Pair<Expr, Expr> eqPair = Pair.of(conjunct.getChild(1).unwrapSlotRef(),
                                                     conjunct.getChild(0).unwrapSlotRef());
                if (!slotEqSlotDeDuplication.contains(pair)
                        && !slotEqSlotDeDuplication.contains(eqPair)) {
                    slotEqSlotDeDuplication.add(pair);
                    slotEqSlotExpr.add(conjunct);
                    if (clauseType.isOnClause()) {
                        analyzer.registerOnSlotEqSlotDeDuplication(pair);
                        analyzer.registerOnSlotEqSlotExpr(conjunct);
                    }
                }
            }
        } else if (conjunct instanceof IsNullPredicate
                    && conjunct.getChild(0) != null
                    && conjunct.getChild(0).unwrapSlotRef() != null) {
            if (!isNullDeDuplication.contains(conjunct.getChild(0).unwrapSlotRef())
                    && ((IsNullPredicate) conjunct).isNotNull()) {
                isNullDeDuplication.add(conjunct.getChild(0).unwrapSlotRef());
                isNullExpr.add(conjunct);
                if (clauseType.isOnClause()) {
                    analyzer.registerOnIsNullDeDuplication(conjunct.getChild(0).unwrapSlotRef());
                    analyzer.registerOnIsNullExpr(conjunct);
                }
            }
        } else if (conjunct instanceof InPredicate
                    && conjunct.getChild(0) != null
                    && conjunct.getChild(0).unwrapSlotRef() != null) {
            if (!inDeDuplication.contains(conjunct.getChild(0).unwrapSlotRef())) {
                inDeDuplication.add(conjunct.getChild(0).unwrapSlotRef());
                inExpr.add(conjunct);
                if (clauseType.isOnClause()) {
                    analyzer.registerInExpr(conjunct);
                    analyzer.registerInDeDuplication(conjunct.getChild(0).unwrapSlotRef());
                }
                analyzer.registerGlobalInDeDuplication(conjunct.getChild(0).unwrapSlotRef());
            }
        }
    }

    /**
     * According to the current slotEqSlotExpr infer all slotEqSlotPredicate.
     * Use warshall algorithm to generate new slotEqSlotExpr
     * eg:
     * old expr:t1.id = t2.id and t2.id = t3.id and t3.id = t4.id
     * new expr:t1.id = t2.id and t2.id = t3.id and t3.id = t4.id and t1.id = t3.id and t1.id = t4.id and t2.id = t4.id
     *
     * @param slotEqSlotExpr slot to slot exprs
     * @param slotEqSlotDeDuplication set pairs in slot = slot exprs
     * @param exprToWarshallArraySubscript: A Map the key is Expr, the value is int
     * @param warshallArraySubscriptToExpr: A Map the key is int, the value is expr
     */
    private void genNewSlotEqSlotPredicate(List<Expr> slotEqSlotExpr,
                                           Set<Pair<Expr, Expr>> slotEqSlotDeDuplication,
                                           Map<Expr, Integer> exprToWarshallArraySubscript,
                                           Map<Integer, Expr> warshallArraySubscriptToExpr,
                                           Analyzer analyzer,
                                           ExprRewriter.ClauseType clauseType) {
        int arrayMaxSize = slotEqSlotExpr.size() * 2;
        int[][] warshall = new int[arrayMaxSize][arrayMaxSize];
        for (int index = 0; index < arrayMaxSize; index++) {
            warshall[index] = new int[arrayMaxSize];
            Arrays.fill(warshall[index], 0);
        }
        boolean needGenWarshallArray = initWarshallArray(warshall, arrayMaxSize,
                slotEqSlotExpr, exprToWarshallArraySubscript, warshallArraySubscriptToExpr);
        if (needGenWarshallArray) {
            List<Pair<Integer, Integer>> newSlotArray = new ArrayList<>();
            genWarshallArray(warshall, arrayMaxSize, newSlotArray);
            buildNewSlotEqSlotPredicate(newSlotArray, warshallArraySubscriptToExpr, slotEqSlotExpr,
                    slotEqSlotDeDuplication, analyzer, clauseType);
        }
    }

    /**
     * Initialize warshall array.
     * Specify a corresponding array_id for each slot, and add the two slots in slotEqSlotExpr
     * to the array in rows and columns
     *
     * @param warshall: Two-dimensional array
     * @param arrayMaxSize: slotEqSlotExpr.size() * 2
     * @param slotEqSlotExpr slot to slot exprs
     * @param exprToWarshallArraySubscript expr to offset in Warshall array
     * @param warshallArraySubscriptToExpr offset in Warshall array to expr
     * @return needGenWarshallArray. True:needGen; False:don't needGen
     */
    private boolean initWarshallArray(int[][] warshall,
                                      int arrayMaxSize,
                                      List<Expr> slotEqSlotExpr,
                                      Map<Expr, Integer> exprToWarshallArraySubscript,
                                      Map<Integer, Expr> warshallArraySubscriptToExpr) {
        boolean needGenWarshallArray = false;
        int index = 0;
        for (Expr slotEqSlot : slotEqSlotExpr) {
            int row;
            int column;
            if (!exprToWarshallArraySubscript.containsKey(slotEqSlot.getChild(0))) {
                exprToWarshallArraySubscript.put(slotEqSlot.getChild(0), index);
                warshallArraySubscriptToExpr.put(index, slotEqSlot.getChild(0));
                row = index;
                index++;
            } else {
                row = exprToWarshallArraySubscript.get(slotEqSlot.getChild(0));
            }

            if (!exprToWarshallArraySubscript.containsKey(slotEqSlot.getChild(1))) {
                exprToWarshallArraySubscript.put(slotEqSlot.getChild(1), index);
                warshallArraySubscriptToExpr.put(index, slotEqSlot.getChild(1));
                column = index;
                index++;
            } else {
                column = exprToWarshallArraySubscript.get(slotEqSlot.getChild(1));
            }

            if (row >= arrayMaxSize
                    || column >= arrayMaxSize) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Error row {} or column {}, but max size is {}.", row, column, arrayMaxSize);
                }
                needGenWarshallArray = false;
                break;
            } else {
                needGenWarshallArray = true;
                warshall[row][column] = 1;
                warshall[column][row] = 1;
            }
        }
        return needGenWarshallArray;
    }

    private void genWarshallArray(int[][] warshall, int arrayMaxSize, List<Pair<Integer, Integer>> newSlotsArray) {
        for (int k = 0; k < arrayMaxSize; k++) {
            for (int i = 0; i < arrayMaxSize; i++) {
                if (warshall[i][k] == 0) {
                    continue;
                }
                for (int j = 0; j < arrayMaxSize; j++) {
                    if (warshall[i][k] == 1
                            && warshall[k][j] == 1) {
                        if (i == j) {
                            continue;
                        }
                        warshall[i][j] = 1;
                        Pair<Integer, Integer> pair = Pair.of(i, j);
                        newSlotsArray.add(pair);
                    }
                }
            }
        }
    }

    /**
     * Construct a new SlotEqSLot based on the results of warshall.
     * Build new BinaryPredicate and add it into structures.
     */
    private void buildNewSlotEqSlotPredicate(List<Pair<Integer, Integer>> newSlots,
                                             Map<Integer, Expr> warshallArraySubscriptToExpr,
                                             List<Expr> slotEqSlotExpr,
                                             Set<Pair<Expr, Expr>> slotEqSlotDeDuplication,
                                             Analyzer analyzer,
                                             ExprRewriter.ClauseType clauseType) {
        for (Pair<Integer, Integer> slotPair : newSlots) {
            Pair<Expr, Expr> pair = Pair.of(warshallArraySubscriptToExpr.get(slotPair.first),
                    warshallArraySubscriptToExpr.get(slotPair.second));
            Pair<Expr, Expr> eqPair = Pair.of(warshallArraySubscriptToExpr.get(slotPair.second),
                    warshallArraySubscriptToExpr.get(slotPair.first));
            if (!slotEqSlotDeDuplication.contains(pair) && !slotEqSlotDeDuplication.contains(eqPair)) {
                slotEqSlotDeDuplication.add(pair);
                slotEqSlotExpr.add(new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        warshallArraySubscriptToExpr.get(slotPair.first),
                        warshallArraySubscriptToExpr.get(slotPair.second)));
                if (clauseType.isOnClause()) {
                    analyzer.registerOnSlotEqSlotDeDuplication(pair);
                    analyzer.registerOnSlotEqSlotExpr(new BinaryPredicate(BinaryPredicate.Operator.EQ,
                            warshallArraySubscriptToExpr.get(slotPair.first),
                            warshallArraySubscriptToExpr.get(slotPair.second)));
                }
            }
        }
    }

    /**
     * Traverse slotToLiteralExpr in turn to build new BinaryPredicate
     */
    private void inferSlotToLiteralPredicates(List<Expr> slotEqSlotExpr,
                                              Set<Pair<Expr, Expr>> slotToLiteralDeDuplication,
                                              List<Expr> slotToLiteralExpr,
                                              List<Pair<Expr, Boolean>> newExprWithState,
                                              Analyzer analyzer,
                                              ExprRewriter.ClauseType clauseType) {
        for (Expr slotToLiteral : slotToLiteralExpr) {
            buildNewBinaryPredicate(slotToLiteral, slotEqSlotExpr,
                    slotToLiteralDeDuplication, newExprWithState, analyzer, clauseType);
        }
    }

    /**
     * Traverse slotEqSlot to extract slots with equal expressions and construct a new slotToLiteral.
     */
    private void buildNewBinaryPredicate(Expr slotToLiteral,
                                         List<Expr> slotEqSlotExpr,
                                         Set<Pair<Expr, Expr>> slotToLiteralDeDuplication,
                                         List<Pair<Expr, Boolean>> newExprWithState,
                                         Analyzer analyzer,
                                         ExprRewriter.ClauseType clauseType) {
        SlotRef checkSlot = slotToLiteral.getChild(0).unwrapSlotRef();
        if (checkSlot != null) {
            for (Expr conjunct : slotEqSlotExpr) {
                SlotRef leftSlot = conjunct.getChild(0).unwrapSlotRef();
                SlotRef rightSlot = conjunct.getChild(1).unwrapSlotRef();

                if (leftSlot != null && rightSlot != null) {
                    if (checkSlot.equals(leftSlot)) {
                        addNewBinaryPredicate(genNewBinaryPredicate(slotToLiteral, rightSlot),
                                slotToLiteralDeDuplication, newExprWithState,
                                isNeedInfer(rightSlot, leftSlot, analyzer, clauseType),
                                analyzer, clauseType);
                    } else if (checkSlot.equals(rightSlot)) {
                        addNewBinaryPredicate(genNewBinaryPredicate(slotToLiteral, leftSlot),
                                slotToLiteralDeDuplication, newExprWithState,
                                isNeedInfer(leftSlot, rightSlot, analyzer, clauseType),
                                analyzer, clauseType);
                    }
                }
            }
        }
    }

    /**
     * To determine whether it needs to be extended.
     * eg:t1.id = t2.id and t2.id = 1;
     *
     * @param newSlot: t1.id
     * @param checkSlot: t2.id
     * @return needInfer.    True: needInfer. False: not needInfer
     */
    private boolean isNeedInfer(SlotRef newSlot, SlotRef checkSlot, Analyzer analyzer,
            ExprRewriter.ClauseType clauseType) {
        boolean ret = false;
        TupleId newTid = newSlot.getDesc().getParent().getRef().getId();
        TupleId checkTid = checkSlot.getDesc().getParent().getRef().getId();
        Pair<TupleId, TupleId> tids = Pair.of(newTid, checkTid);
        if (analyzer.isContainTupleIds(tids)) {
            JoinOperator joinOperator = analyzer.getAnyTwoTablesJoinOp(tids);
            ret = checkNeedInfer(joinOperator, false, clauseType);
        } else {
            Pair<TupleId, TupleId> changeTids = Pair.of(checkTid, newTid);
            if (analyzer.isContainTupleIds(changeTids)) {
                JoinOperator joinOperator = analyzer.getAnyTwoTablesJoinOp(changeTids);
                ret = checkNeedInfer(joinOperator, true, clauseType);
            }
        }
        return ret;
    }

    /**
     * Whether to derive the rules, on_clause and where_clause are discussed separately
     */
    private boolean checkNeedInfer(JoinOperator joinOperator, boolean needChange, ExprRewriter.ClauseType clauseType) {
        boolean ret = false;
        if (clauseType.isOnClause()) {
            if (joinOperator.isInnerJoin()
                    || (joinOperator == JoinOperator.LEFT_SEMI_JOIN)
                    || (!needChange && joinOperator == JoinOperator.RIGHT_OUTER_JOIN)
                    || (needChange && (joinOperator == JoinOperator.LEFT_OUTER_JOIN
                    || joinOperator == JoinOperator.LEFT_ANTI_JOIN
                    || joinOperator == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN))) {
                ret = true;
            }
        } else if (clauseType == ExprRewriter.ClauseType.WHERE_CLAUSE) {
            if (joinOperator.isInnerJoin()
                    || (joinOperator == JoinOperator.LEFT_SEMI_JOIN
                    || (needChange && joinOperator == JoinOperator.RIGHT_OUTER_JOIN))
                    || (!needChange && (joinOperator == JoinOperator.LEFT_OUTER_JOIN
                    || joinOperator == JoinOperator.LEFT_ANTI_JOIN
                    || joinOperator == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN))) {
                ret = true;
            }
        }
        return ret;
    }

    /**
     * generate a new BinaryPredicate.
     * @return new BinaryPredicate.
     */
    private Expr genNewBinaryPredicate(Expr oldExpr, Expr newSlot) {
        if (oldExpr instanceof BinaryPredicate) {
            BinaryPredicate oldBP = (BinaryPredicate) oldExpr;
            return new BinaryPredicate(oldBP.getOp(), newSlot, oldBP.getChild(1));
        }
        return oldExpr;
    }

    /**
     * add the new BinaryPredicate to slotToLiteralDeDuplication and newExprWithState and simultaneous deduplication
     */
    private void addNewBinaryPredicate(Expr expr,
                                       Set<Pair<Expr, Expr>> slotToLiteralDeDuplication,
                                       List<Pair<Expr, Boolean>> newExprWithState,
                                       boolean needAddnewExprWithState,
                                       Analyzer analyzer,
                                       ExprRewriter.ClauseType clauseType) {
        if (expr instanceof BinaryPredicate) {
            BinaryPredicate newBP = (BinaryPredicate) expr;
            Pair<Expr, Expr> pair = Pair.of(newBP.getChild(0), newBP.getChild(1));
            if (!slotToLiteralDeDuplication.contains(pair)) {
                slotToLiteralDeDuplication.add(pair);
                Pair<Expr, Boolean> newBPWithBool = Pair.of(newBP, needAddnewExprWithState);
                newExprWithState.add(newBPWithBool);
                if (clauseType.isOnClause()) {
                    analyzer.registerOnSlotToLiteralDeDuplication(pair);
                    analyzer.registerOnSlotToLiteralExpr(newBP);
                }
                if (needAddnewExprWithState) {
                    analyzer.registerGlobalSlotToLiteralDeDuplication(pair);
                }
            }
        }
    }

    /**
     * Traverse isNullExpr in turn to build new isNullPredicate
     */
    private void inferIsNotNullPredicates(List<Expr> slotEqSlotExpr,
                                          List<Expr> isNullExpr,
                                          Set<Expr> isNullDeDuplication,
                                          List<Pair<Expr, Boolean>> newExprWithState,
                                          Analyzer analyzer,
                                          ExprRewriter.ClauseType clauseType) {
        for (Expr isNullPredicate : isNullExpr) {
            buildNewIsNotNullPredicate(isNullPredicate, slotEqSlotExpr, isNullDeDuplication,
                    newExprWithState, analyzer, clauseType);
        }
    }

    /**
     * Traverse slotEqSlot to extract slots with equal expressions and construct a new IsNullPredicate.
     */
    private void buildNewIsNotNullPredicate(Expr expr,
                                            List<Expr> slotEqSlotExpr,
                                            Set<Expr> isNullDeDuplication,
                                            List<Pair<Expr, Boolean>> newExprWithState,
                                            Analyzer analyzer,
                                            ExprRewriter.ClauseType clauseType) {
        if (expr instanceof IsNullPredicate) {
            IsNullPredicate isNullPredicate = (IsNullPredicate) expr;
            SlotRef checkSlot = isNullPredicate.getChild(0).unwrapSlotRef();
            if (checkSlot != null) {
                for (Expr conjunct : slotEqSlotExpr) {
                    SlotRef leftSlot = conjunct.getChild(0).unwrapSlotRef();
                    SlotRef rightSlot = conjunct.getChild(1).unwrapSlotRef();

                    if (leftSlot != null && rightSlot != null) {
                        if (checkSlot.equals(leftSlot) && isNullPredicate.isNotNull()) {
                            addNewIsNotNullPredicate(genNewIsNotNullPredicate(isNullPredicate, rightSlot),
                                    isNullDeDuplication, newExprWithState, analyzer, clauseType);
                        } else if (checkSlot.equals(rightSlot)) {
                            addNewIsNotNullPredicate(genNewIsNotNullPredicate(isNullPredicate, leftSlot),
                                    isNullDeDuplication, newExprWithState, analyzer, clauseType);
                        }
                    }
                }
            }
        }
    }

    /**
     * generate a new IsNullPredicate.
     * @return new IsNullPredicate.
     */
    private Expr genNewIsNotNullPredicate(IsNullPredicate oldExpr, Expr newSlot) {
        return oldExpr != null ? new IsNullPredicate(newSlot, oldExpr.isNotNull()) : null;
    }

    /**
     * add the new IsNullPredicate to isNullDeDuplication and newExprWithState and simultaneous deduplication
     */
    private void addNewIsNotNullPredicate(Expr expr,
                                          Set<Expr> isNullDeDuplication,
                                          List<Pair<Expr, Boolean>> newExprWithState,
                                          Analyzer analyzer,
                                          ExprRewriter.ClauseType clauseType) {
        if (expr instanceof IsNullPredicate) {
            IsNullPredicate newExpr = (IsNullPredicate) expr;
            if (!isNullDeDuplication.contains(newExpr.getChild(0))) {
                isNullDeDuplication.add(newExpr.getChild(0));
                Pair<Expr, Boolean> newExprWithBoolean = Pair.of(newExpr, true);
                newExprWithState.add(newExprWithBoolean);
                if (clauseType.isOnClause()) {
                    analyzer.registerOnIsNullExpr(newExpr);
                    analyzer.registerOnIsNullDeDuplication(newExpr);
                }
            }
        }
    }

    /**
     * Traverse inExprs in turn to build new InPredicate
     */
    private void inferInPredicate(List<Expr> slotEqSlotExpr,
                                  Set<Expr> inDeDuplication,
                                  List<Expr> inExprs,
                                  List<Pair<Expr, Boolean>> newExprWithState,
                                  Analyzer analyzer,
                                  ExprRewriter.ClauseType clauseType) {
        for (Expr inExpr : inExprs) {
            buildNewInPredicate(inExpr, slotEqSlotExpr,
                    inDeDuplication, newExprWithState, analyzer, clauseType);
        }
    }

    /**
     Traverse slotEqSlot to extract slots with equal expressions and construct a new InPredicate.
     */
    private void buildNewInPredicate(Expr inExpr,
                                     List<Expr> slotEqSlotExpr,
                                     Set<Expr> inDeDuplication,
                                     List<Pair<Expr, Boolean>> newExprWithState,
                                     Analyzer analyzer,
                                     ExprRewriter.ClauseType clauseType) {
        if (inExpr instanceof InPredicate) {
            InPredicate inpredicate = (InPredicate) inExpr;
            SlotRef checkSlot = inpredicate.getChild(0).unwrapSlotRef();
            if (checkSlot != null) {
                for (Expr conjunct : slotEqSlotExpr) {
                    SlotRef leftSlot = conjunct.getChild(0).unwrapSlotRef();
                    SlotRef rightSlot = conjunct.getChild(1).unwrapSlotRef();

                    if (leftSlot != null && rightSlot != null) {
                        if (checkSlot.equals(leftSlot)) {
                            addNewInPredicate(genNewInPredicate(inpredicate, rightSlot),
                                    inDeDuplication, newExprWithState,
                                    isNeedInfer(rightSlot, leftSlot, analyzer, clauseType),
                                    analyzer, clauseType);
                        } else if (checkSlot.equals(rightSlot)) {
                            addNewInPredicate(genNewInPredicate(inpredicate, leftSlot),
                                    inDeDuplication, newExprWithState,
                                    isNeedInfer(leftSlot, rightSlot, analyzer, clauseType),
                                    analyzer, clauseType);
                        }
                    }
                }
            }
        }
    }

    /**
     * generate a new InPredicate.
     * @return new InPredicate.
     */
    private Expr genNewInPredicate(Expr oldExpr, Expr newSlot) {
        if (oldExpr instanceof InPredicate) {
            InPredicate oldBP = (InPredicate) oldExpr;
            return new InPredicate(newSlot, oldBP.getListChildren(), oldBP.isNotIn());
        }
        return oldExpr;
    }

    /**
     * add the new InPredicate to inDeDuplication and newExprWithState and simultaneous deduplication
     */
    private void addNewInPredicate(Expr expr,
                                   Set<Expr> inDeDuplication,
                                   List<Pair<Expr, Boolean>> newExprWithState,
                                   boolean needAddnewExprWithState,
                                   Analyzer analyzer,
                                   ExprRewriter.ClauseType clauseType) {
        if (expr instanceof InPredicate) {
            InPredicate newIP = (InPredicate) expr;
            if (!inDeDuplication.contains(newIP)) {
                inDeDuplication.add(newIP);
                Pair<Expr, Boolean> newBPWithBool = Pair.of(newIP, needAddnewExprWithState);
                newExprWithState.add(newBPWithBool);
                if (clauseType.isOnClause()) {
                    analyzer.registerInDeDuplication(newIP);
                    analyzer.registerInExpr(newIP);
                }
                if (needAddnewExprWithState) {
                    analyzer.registerGlobalInDeDuplication(newIP);
                }
            }
        }
    }
}
