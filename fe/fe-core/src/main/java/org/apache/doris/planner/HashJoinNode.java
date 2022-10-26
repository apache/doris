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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/HashJoinNode.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.analysis.TupleIsNullPredicate;
import org.apache.doris.catalog.ColumnStats;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CheckedMath;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.StatsRecursiveDerive;
import org.apache.doris.thrift.TEqJoinCondition;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.THashJoinNode;
import org.apache.doris.thrift.TNullSide;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Hash join between left child and right child.
 * The right child must be a leaf node, ie, can only materialize
 * a single input tuple.
 */
public class HashJoinNode extends PlanNode {
    private static final Logger LOG = LogManager.getLogger(HashJoinNode.class);

    private TableRef innerRef;
    private final JoinOperator joinOp;
    // predicates of the form 'a=b' or 'a<=>b'
    private List<BinaryPredicate> eqJoinConjuncts = Lists.newArrayList();
    // join conjuncts from the JOIN clause that aren't equi-join predicates
    private List<Expr> otherJoinConjuncts;
    // join conjunct from the JOIN clause that aren't equi-join predicates, only use in
    // vec exec engine
    private Expr votherJoinConjunct = null;

    private DistributionMode distrMode;
    private boolean isColocate = false; //the flag for colocate join
    private String colocateReason = ""; // if can not do colocate join, set reason here
    private boolean isBucketShuffle = false; // the flag for bucket shuffle join

    private List<SlotId> hashOutputSlotIds = new ArrayList<>(); //init for nereids
    private TupleDescriptor vOutputTupleDesc;
    private ExprSubstitutionMap vSrcToOutputSMap;
    private List<TupleDescriptor> vIntermediateTupleDescList;

    /**
     * Constructor of HashJoinNode.
     */
    public HashJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, TableRef innerRef, List<Expr> eqJoinConjuncts,
            List<Expr> otherJoinConjuncts) {
        super(id, "HASH JOIN", StatisticalType.HASH_JOIN_NODE);
        Preconditions.checkArgument(eqJoinConjuncts != null && !eqJoinConjuncts.isEmpty());
        Preconditions.checkArgument(otherJoinConjuncts != null);
        tblRefIds.addAll(outer.getTblRefIds());
        tblRefIds.addAll(inner.getTblRefIds());
        this.innerRef = innerRef;
        this.joinOp = innerRef.getJoinOp();

        // TODO: Support not vec exec engine cut unless tupleid in semi/anti join
        if (VectorizedUtil.isVectorized()) {
            if (joinOp.equals(JoinOperator.LEFT_ANTI_JOIN) || joinOp.equals(JoinOperator.LEFT_SEMI_JOIN)
                    || joinOp.equals(JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN)) {
                tupleIds.addAll(outer.getTupleIds());
            } else if (joinOp.equals(JoinOperator.RIGHT_ANTI_JOIN) || joinOp.equals(JoinOperator.RIGHT_SEMI_JOIN)) {
                tupleIds.addAll(inner.getTupleIds());
            } else {
                tupleIds.addAll(outer.getTupleIds());
                tupleIds.addAll(inner.getTupleIds());
            }
        } else {
            tupleIds.addAll(outer.getTupleIds());
            tupleIds.addAll(inner.getTupleIds());
        }

        for (Expr eqJoinPredicate : eqJoinConjuncts) {
            Preconditions.checkArgument(eqJoinPredicate instanceof BinaryPredicate);
            BinaryPredicate eqJoin = (BinaryPredicate) eqJoinPredicate;
            if (eqJoin.getOp().equals(BinaryPredicate.Operator.EQ_FOR_NULL)) {
                Preconditions.checkArgument(eqJoin.getChildren().size() == 2);
                if (!eqJoin.getChild(0).isNullable() || !eqJoin.getChild(1).isNullable()) {
                    eqJoin.setOp(BinaryPredicate.Operator.EQ);
                }
            }
            this.eqJoinConjuncts.add(eqJoin);
        }
        this.distrMode = DistributionMode.NONE;
        this.otherJoinConjuncts = otherJoinConjuncts;
        children.add(outer);
        children.add(inner);

        // Inherits all the nullable tuple from the children
        // Mark tuples that form the "nullable" side of the outer join as nullable.
        nullableTupleIds.addAll(inner.getNullableTupleIds());
        nullableTupleIds.addAll(outer.getNullableTupleIds());
        if (joinOp.equals(JoinOperator.FULL_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getTupleIds());
            nullableTupleIds.addAll(inner.getTupleIds());
        } else if (joinOp.equals(JoinOperator.LEFT_OUTER_JOIN)) {
            nullableTupleIds.addAll(inner.getTupleIds());
        } else if (joinOp.equals(JoinOperator.RIGHT_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getTupleIds());
        }
    }

    /**
     * This constructor is used by new optimizer.
     */
    public HashJoinNode(PlanNodeId id, PlanNode outer, PlanNode inner, JoinOperator joinOp,
            List<Expr> eqJoinConjuncts, List<Expr> otherJoinConjuncts, List<Expr> srcToOutputList,
            TupleDescriptor intermediateTuple, TupleDescriptor outputTuple) {
        super(id, "HASH JOIN", StatisticalType.HASH_JOIN_NODE);
        Preconditions.checkArgument(eqJoinConjuncts != null && !eqJoinConjuncts.isEmpty());
        Preconditions.checkArgument(otherJoinConjuncts != null);
        tblRefIds.addAll(outer.getTblRefIds());
        tblRefIds.addAll(inner.getTblRefIds());
        this.joinOp = joinOp;
        // TODO: Support not vec exec engine cut unless tupleid in semi/anti join
        if (VectorizedUtil.isVectorized()) {
            if (joinOp.equals(JoinOperator.LEFT_ANTI_JOIN) || joinOp.equals(JoinOperator.LEFT_SEMI_JOIN)
                    || joinOp.equals(JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN)) {
                tupleIds.addAll(outer.getTupleIds());
            } else if (joinOp.equals(JoinOperator.RIGHT_ANTI_JOIN) || joinOp.equals(JoinOperator.RIGHT_SEMI_JOIN)) {
                tupleIds.addAll(inner.getTupleIds());
            } else {
                tupleIds.addAll(outer.getTupleIds());
                tupleIds.addAll(inner.getTupleIds());
            }
        } else {
            tupleIds.addAll(outer.getTupleIds());
            tupleIds.addAll(inner.getTupleIds());
        }

        for (Expr eqJoinPredicate : eqJoinConjuncts) {
            Preconditions.checkArgument(eqJoinPredicate instanceof BinaryPredicate);
            BinaryPredicate eqJoin = (BinaryPredicate) eqJoinPredicate;
            if (eqJoin.getOp().equals(BinaryPredicate.Operator.EQ_FOR_NULL)) {
                Preconditions.checkArgument(eqJoin.getChildren().size() == 2);
                if (!eqJoin.getChild(0).isNullable() || !eqJoin.getChild(1).isNullable()) {
                    eqJoin.setOp(BinaryPredicate.Operator.EQ);
                }
            }
            this.eqJoinConjuncts.add(eqJoin);
        }
        this.distrMode = DistributionMode.NONE;
        this.otherJoinConjuncts = otherJoinConjuncts;
        children.add(outer);
        children.add(inner);

        // Inherits all the nullable tuple from the children
        // Mark tuples that form the "nullable" side of the outer join as nullable.
        nullableTupleIds.addAll(inner.getNullableTupleIds());
        nullableTupleIds.addAll(outer.getNullableTupleIds());
        if (joinOp.equals(JoinOperator.FULL_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getTupleIds());
            nullableTupleIds.addAll(inner.getTupleIds());
        } else if (joinOp.equals(JoinOperator.LEFT_OUTER_JOIN)) {
            nullableTupleIds.addAll(inner.getTupleIds());
        } else if (joinOp.equals(JoinOperator.RIGHT_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getTupleIds());
        }
        vIntermediateTupleDescList = Lists.newArrayList(intermediateTuple);
        vOutputTupleDesc = outputTuple;
        vSrcToOutputSMap = new ExprSubstitutionMap(srcToOutputList, Collections.emptyList());
    }

    public List<BinaryPredicate> getEqJoinConjuncts() {
        return eqJoinConjuncts;
    }

    public JoinOperator getJoinOp() {
        return joinOp;
    }

    public TableRef getInnerRef() {
        return innerRef;
    }

    public DistributionMode getDistributionMode() {
        return distrMode;
    }

    public void setDistributionMode(DistributionMode distrMode) {
        this.distrMode = distrMode;
    }

    public boolean isColocate() {
        return isColocate;
    }

    public boolean isBucketShuffle() {
        return distrMode.equals(DistributionMode.BUCKET_SHUFFLE);
    }

    public void setColocate(boolean colocate, String reason) {
        isColocate = colocate;
        colocateReason = reason;
    }

    /**
     * Calculate the slots output after going through the hash table in the hash join node.
     * The most essential difference between 'hashOutputSlots' and 'outputSlots' is that
     * it's output needs to contain other conjunct and conjunct columns.
     * hash output slots = output slots + conjunct slots + other conjunct slots
     * For example:
     * select b.k1 from test.t1 a right join test.t1 b on a.k1=b.k1 and b.k2>1 where a.k2>1;
     * output slots: b.k1
     * other conjuncts: a.k2>1
     * conjuncts: b.k2>1
     * hash output slots: a.k2, b.k2, b.k1
     * eq conjuncts: a.k1=b.k1
     *
     * @param slotIdList
     */
    private void initHashOutputSlotIds(List<SlotId> slotIdList, Analyzer analyzer) {
        Set<SlotId> hashOutputSlotIdSet = Sets.newHashSet();
        // step1: change output slot id to src slot id
        if (vSrcToOutputSMap != null) {
            for (SlotId slotId : slotIdList) {
                SlotRef slotRef = new SlotRef(analyzer.getDescTbl().getSlotDesc(slotId));
                Expr srcExpr = vSrcToOutputSMap.mappingForRhsExpr(slotRef);
                if (srcExpr == null) {
                    hashOutputSlotIdSet.add(slotId);
                } else {
                    List<SlotRef> srcSlotRefList = Lists.newArrayList();
                    srcExpr.collect(SlotRef.class, srcSlotRefList);
                    hashOutputSlotIdSet
                            .addAll(srcSlotRefList.stream().map(e -> e.getSlotId()).collect(Collectors.toList()));
                }
            }
        }

        // step2: add conjuncts required slots
        List<SlotId> otherAndConjunctSlotIds = Lists.newArrayList();
        Expr.getIds(otherJoinConjuncts, null, otherAndConjunctSlotIds);
        Expr.getIds(conjuncts, null, otherAndConjunctSlotIds);
        hashOutputSlotIdSet.addAll(otherAndConjunctSlotIds);
        hashOutputSlotIds = new ArrayList<>(hashOutputSlotIdSet);
    }

    @Override
    public void initOutputSlotIds(Set<SlotId> requiredSlotIdSet, Analyzer analyzer) {
        outputSlotIds = Lists.newArrayList();
        List<TupleDescriptor> outputTupleDescList = Lists.newArrayList();
        if (vOutputTupleDesc != null) {
            outputTupleDescList.add(vOutputTupleDesc);
        } else {
            for (TupleId tupleId : tupleIds) {
                outputTupleDescList.add(analyzer.getTupleDesc(tupleId));
            }
        }
        SlotId firstMaterializedSlotId = null;
        for (TupleDescriptor tupleDescriptor : outputTupleDescList) {
            for (SlotDescriptor slotDescriptor : tupleDescriptor.getSlots()) {
                if (slotDescriptor.isMaterialized()) {
                    if ((requiredSlotIdSet == null || requiredSlotIdSet.contains(slotDescriptor.getId()))) {
                        outputSlotIds.add(slotDescriptor.getId());
                    }
                    if (firstMaterializedSlotId == null) {
                        firstMaterializedSlotId = slotDescriptor.getId();
                    }
                }
            }
        }

        // be may be possible to output correct row number without any column data in future
        // but for now, in order to have correct output row number, should keep at least one slot.
        // use first materialized slot if outputSlotIds is empty.
        if (outputSlotIds.isEmpty() && firstMaterializedSlotId != null) {
            outputSlotIds.add(firstMaterializedSlotId);
        }
        initHashOutputSlotIds(outputSlotIds, analyzer);
    }

    @Override
    public void projectOutputTuple() throws NotImplementedException {
        if (vOutputTupleDesc == null) {
            return;
        }
        if (vOutputTupleDesc.getSlots().size() == outputSlotIds.size()) {
            return;
        }
        Iterator<SlotDescriptor> iterator = vOutputTupleDesc.getSlots().iterator();
        while (iterator.hasNext()) {
            SlotDescriptor slotDescriptor = iterator.next();
            boolean keep = false;
            for (SlotId outputSlotId : outputSlotIds) {
                if (slotDescriptor.getId().equals(outputSlotId)) {
                    keep = true;
                    break;
                }
            }
            if (!keep) {
                iterator.remove();
                SlotRef slotRef = new SlotRef(slotDescriptor);
                vSrcToOutputSMap.removeByRhsExpr(slotRef);
            }
        }
        vOutputTupleDesc.computeStatAndMemLayout();
    }

    // output slots + predicate slots = input slots
    @Override
    public Set<SlotId> computeInputSlotIds(Analyzer analyzer) throws NotImplementedException {
        Set<SlotId> result = Sets.newHashSet();
        Preconditions.checkState(outputSlotIds != null);
        // step1: change output slot id to src slot id
        if (vSrcToOutputSMap != null) {
            for (SlotId slotId : outputSlotIds) {
                SlotRef slotRef = new SlotRef(analyzer.getDescTbl().getSlotDesc(slotId));
                Expr srcExpr = vSrcToOutputSMap.mappingForRhsExpr(slotRef);
                if (srcExpr == null) {
                    result.add(slotId);
                } else {
                    List<SlotRef> srcSlotRefList = Lists.newArrayList();
                    srcExpr.collect(SlotRef.class, srcSlotRefList);
                    result.addAll(srcSlotRefList.stream().map(e -> e.getSlotId()).collect(Collectors.toList()));
                }
            }
        }
        // eq conjunct
        List<SlotId> eqConjunctSlotIds = Lists.newArrayList();
        Expr.getIds(eqJoinConjuncts, null, eqConjunctSlotIds);
        result.addAll(eqConjunctSlotIds);
        // other conjunct
        List<SlotId> otherConjunctSlotIds = Lists.newArrayList();
        Expr.getIds(otherJoinConjuncts, null, otherConjunctSlotIds);
        result.addAll(otherConjunctSlotIds);
        // conjunct
        List<SlotId> conjunctSlotIds = Lists.newArrayList();
        Expr.getIds(conjuncts, null, conjunctSlotIds);
        result.addAll(conjunctSlotIds);
        return result;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        assignedConjuncts = analyzer.getAssignedConjuncts();
        // outSmap replace in outer join may cause NULL be replace by literal
        // so need replace the outsmap in nullableTupleID
        replaceOutputSmapForOuterJoin();
        computeStats(analyzer);

        ExprSubstitutionMap combinedChildSmap = getCombinedChildWithoutTupleIsNullSmap();
        List<Expr> newEqJoinConjuncts = Expr.substituteList(eqJoinConjuncts, combinedChildSmap, analyzer, false);
        eqJoinConjuncts =
                newEqJoinConjuncts.stream().map(entity -> (BinaryPredicate) entity).collect(Collectors.toList());
        assignedConjuncts = analyzer.getAssignedConjuncts();
        otherJoinConjuncts = Expr.substituteList(otherJoinConjuncts, combinedChildSmap, analyzer, false);

        // Only for Vec: create new tuple for join result
        if (VectorizedUtil.isVectorized()) {
            computeOutputTuple(analyzer);
        }
    }

    private void computeOutputTuple(Analyzer analyzer) throws UserException {
        // 1. create new tuple
        vOutputTupleDesc = analyzer.getDescTbl().createTupleDescriptor();
        boolean copyLeft = false;
        boolean copyRight = false;
        boolean leftNullable = false;
        boolean rightNullable = false;
        switch (joinOp) {
            case INNER_JOIN:
            case CROSS_JOIN:
                copyLeft = true;
                copyRight = true;
                break;
            case LEFT_OUTER_JOIN:
                copyLeft = true;
                copyRight = true;
                rightNullable = true;
                break;
            case RIGHT_OUTER_JOIN:
                copyLeft = true;
                copyRight = true;
                leftNullable = true;
                break;
            case FULL_OUTER_JOIN:
                copyLeft = true;
                copyRight = true;
                leftNullable = true;
                rightNullable = true;
                break;
            case LEFT_ANTI_JOIN:
            case LEFT_SEMI_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
                copyLeft = true;
                break;
            case RIGHT_ANTI_JOIN:
            case RIGHT_SEMI_JOIN:
                copyRight = true;
                break;
            default:
                break;
        }
        ExprSubstitutionMap srcTblRefToOutputTupleSmap = new ExprSubstitutionMap();
        int leftNullableNumber = 0;
        int rightNullableNumber = 0;
        if (copyLeft) {
            //cross join do not have OutputTblRefIds
            List<TupleId> srcTupleIds = getChild(0) instanceof CrossJoinNode ? getChild(0).getOutputTupleIds()
                    : getChild(0).getOutputTblRefIds();
            for (TupleDescriptor leftTupleDesc : analyzer.getDescTbl().getTupleDesc(srcTupleIds)) {
                // if the child is cross join node, the only way to get the correct nullable info of its output slots
                // is to check if the output tuple ids are outer joined or not.
                // then pass this nullable info to hash join node will be correct.
                boolean needSetToNullable =
                        getChild(0) instanceof CrossJoinNode && analyzer.isOuterJoined(leftTupleDesc.getId());
                for (SlotDescriptor leftSlotDesc : leftTupleDesc.getSlots()) {
                    if (!isMaterailizedByChild(leftSlotDesc, getChild(0).getOutputSmap())) {
                        continue;
                    }
                    SlotDescriptor outputSlotDesc =
                            analyzer.getDescTbl().copySlotDescriptor(vOutputTupleDesc, leftSlotDesc);
                    if (leftNullable) {
                        outputSlotDesc.setIsNullable(true);
                        leftNullableNumber++;
                    }
                    if (needSetToNullable) {
                        outputSlotDesc.setIsNullable(true);
                    }
                    srcTblRefToOutputTupleSmap.put(new SlotRef(leftSlotDesc), new SlotRef(outputSlotDesc));
                }
            }
        }
        if (copyRight) {
            List<TupleId> srcTupleIds = getChild(1) instanceof CrossJoinNode ? getChild(1).getOutputTupleIds()
                    : getChild(1).getOutputTblRefIds();
            for (TupleDescriptor rightTupleDesc : analyzer.getDescTbl().getTupleDesc(srcTupleIds)) {
                boolean needSetToNullable =
                        getChild(1) instanceof CrossJoinNode && analyzer.isOuterJoined(rightTupleDesc.getId());
                for (SlotDescriptor rightSlotDesc : rightTupleDesc.getSlots()) {
                    if (!isMaterailizedByChild(rightSlotDesc, getChild(1).getOutputSmap())) {
                        continue;
                    }
                    SlotDescriptor outputSlotDesc =
                            analyzer.getDescTbl().copySlotDescriptor(vOutputTupleDesc, rightSlotDesc);
                    if (rightNullable) {
                        outputSlotDesc.setIsNullable(true);
                        rightNullableNumber++;
                    }
                    if (needSetToNullable) {
                        outputSlotDesc.setIsNullable(true);
                    }
                    srcTblRefToOutputTupleSmap.put(new SlotRef(rightSlotDesc), new SlotRef(outputSlotDesc));
                }
            }
        }
        // 2. compute srcToOutputMap
        vSrcToOutputSMap = ExprSubstitutionMap.subtraction(outputSmap, srcTblRefToOutputTupleSmap, analyzer);
        for (int i = 0; i < vSrcToOutputSMap.size(); i++) {
            Preconditions.checkState(vSrcToOutputSMap.getRhs().get(i) instanceof SlotRef);
            SlotRef rSlotRef = (SlotRef) vSrcToOutputSMap.getRhs().get(i);
            if (vSrcToOutputSMap.getLhs().get(i) instanceof SlotRef) {
                SlotRef lSlotRef = (SlotRef) vSrcToOutputSMap.getLhs().get(i);
                rSlotRef.getDesc().setIsMaterialized(lSlotRef.getDesc().isMaterialized());
            } else {
                rSlotRef.getDesc().setIsMaterialized(true);
            }
        }
        vOutputTupleDesc.computeStatAndMemLayout();
        // 3. add tupleisnull in null-side
        Preconditions.checkState(srcTblRefToOutputTupleSmap.getLhs().size() == vSrcToOutputSMap.getLhs().size());
        // Condition1: the left child is null-side
        // Condition2: the left child is a inline view
        // Then: add tuple is null in left child columns
        if (leftNullable && getChild(0).tblRefIds.size() == 1 && analyzer.isInlineView(getChild(0).tblRefIds.get(0))) {
            List<Expr> tupleIsNullLhs = TupleIsNullPredicate
                    .wrapExprs(vSrcToOutputSMap.getLhs().subList(0, leftNullableNumber), new ArrayList<>(),
                        TNullSide.LEFT, analyzer);
            tupleIsNullLhs
                    .addAll(vSrcToOutputSMap.getLhs().subList(leftNullableNumber, vSrcToOutputSMap.getLhs().size()));
            vSrcToOutputSMap.updateLhsExprs(tupleIsNullLhs);
        }
        // Condition1: the right child is null-side
        // Condition2: the right child is a inline view
        // Then: add tuple is null in right child columns
        if (rightNullable && getChild(1).tblRefIds.size() == 1 && analyzer.isInlineView(getChild(1).tblRefIds.get(0))) {
            if (rightNullableNumber != 0) {
                int rightBeginIndex = vSrcToOutputSMap.size() - rightNullableNumber;
                List<Expr> tupleIsNullLhs = TupleIsNullPredicate
                        .wrapExprs(vSrcToOutputSMap.getLhs().subList(rightBeginIndex, vSrcToOutputSMap.size()),
                                new ArrayList<>(), TNullSide.RIGHT, analyzer);
                List<Expr> newLhsList = Lists.newArrayList();
                if (rightBeginIndex > 0) {
                    newLhsList.addAll(vSrcToOutputSMap.getLhs().subList(0, rightBeginIndex));
                }
                newLhsList.addAll(tupleIsNullLhs);
                vSrcToOutputSMap.updateLhsExprs(newLhsList);
            }
        }
        // 4. change the outputSmap
        outputSmap = ExprSubstitutionMap.composeAndReplace(outputSmap, srcTblRefToOutputTupleSmap, analyzer);
    }

    private void replaceOutputSmapForOuterJoin() {
        if (joinOp.isOuterJoin() && !VectorizedUtil.isVectorized()) {
            List<Expr> lhs = new ArrayList<>();
            List<Expr> rhs = new ArrayList<>();

            for (int i = 0; i < outputSmap.size(); i++) {
                Expr expr = outputSmap.getLhs().get(i);
                boolean isInNullableTuple = false;
                for (TupleId tupleId : nullableTupleIds) {
                    if (expr.isBound(tupleId)) {
                        isInNullableTuple = true;
                        break;
                    }
                }

                if (!isInNullableTuple) {
                    lhs.add(outputSmap.getLhs().get(i));
                    rhs.add(outputSmap.getRhs().get(i));
                }
            }
            outputSmap = new ExprSubstitutionMap(lhs, rhs);
        }
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        super.finalize(analyzer);
        if (VectorizedUtil.isVectorized()) {
            computeIntermediateTuple(analyzer);
        }
    }

    private void computeIntermediateTuple(Analyzer analyzer) throws AnalysisException {
        // 1. create new tuple
        TupleDescriptor vIntermediateLeftTupleDesc = analyzer.getDescTbl().createTupleDescriptor();
        TupleDescriptor vIntermediateRightTupleDesc = analyzer.getDescTbl().createTupleDescriptor();
        vIntermediateTupleDescList = new ArrayList<>();
        vIntermediateTupleDescList.add(vIntermediateLeftTupleDesc);
        vIntermediateTupleDescList.add(vIntermediateRightTupleDesc);
        boolean leftNullable = false;
        boolean rightNullable = false;
        boolean copyleft = true;
        boolean copyRight = true;
        switch (joinOp) {
            case LEFT_OUTER_JOIN:
                rightNullable = true;
                break;
            case RIGHT_OUTER_JOIN:
                leftNullable = true;
                break;
            case FULL_OUTER_JOIN:
                leftNullable = true;
                rightNullable = true;
                break;
            case LEFT_ANTI_JOIN:
            case LEFT_SEMI_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
                if (otherJoinConjuncts == null || otherJoinConjuncts.isEmpty()) {
                    copyRight = false;
                }
                break;
            case RIGHT_SEMI_JOIN:
            case RIGHT_ANTI_JOIN:
                if (otherJoinConjuncts == null || otherJoinConjuncts.isEmpty()) {
                    copyleft = false;
                }
                break;
            default:
                break;
        }
        // 2. exprsmap: <originslot, intermediateslot>
        ExprSubstitutionMap originToIntermediateSmap = new ExprSubstitutionMap();
        Map<List<TupleId>, TupleId> originTidsToIntermediateTidMap = Maps.newHashMap();
        // left
        if (copyleft) {
            originTidsToIntermediateTidMap.put(getChild(0).getOutputTupleIds(), vIntermediateLeftTupleDesc.getId());
            for (TupleDescriptor tupleDescriptor : analyzer.getDescTbl()
                    .getTupleDesc(getChild(0).getOutputTupleIds())) {
                for (SlotDescriptor slotDescriptor : tupleDescriptor.getMaterializedSlots()) {
                    SlotDescriptor intermediateSlotDesc =
                            analyzer.getDescTbl().copySlotDescriptor(vIntermediateLeftTupleDesc, slotDescriptor);
                    if (leftNullable) {
                        intermediateSlotDesc.setIsNullable(true);
                    }
                    originToIntermediateSmap.put(new SlotRef(slotDescriptor), new SlotRef(intermediateSlotDesc));
                }
            }
        }
        vIntermediateLeftTupleDesc.computeMemLayout();
        // right
        if (copyRight) {
            originTidsToIntermediateTidMap.put(getChild(1).getOutputTupleIds(), vIntermediateRightTupleDesc.getId());
            for (TupleDescriptor tupleDescriptor : analyzer.getDescTbl()
                    .getTupleDesc(getChild(1).getOutputTupleIds())) {
                for (SlotDescriptor slotDescriptor : tupleDescriptor.getMaterializedSlots()) {
                    SlotDescriptor intermediateSlotDesc =
                            analyzer.getDescTbl().copySlotDescriptor(vIntermediateRightTupleDesc, slotDescriptor);
                    if (rightNullable) {
                        intermediateSlotDesc.setIsNullable(true);
                    }
                    originToIntermediateSmap.put(new SlotRef(slotDescriptor), new SlotRef(intermediateSlotDesc));
                }
            }
        }
        vIntermediateRightTupleDesc.computeMemLayout();
        // 3. replace srcExpr by intermediate tuple
        Preconditions.checkState(vSrcToOutputSMap != null);
        // Set `preserveRootTypes` to true because we should keep the consistent for types. See Issue-11314.
        vSrcToOutputSMap.substituteLhs(originToIntermediateSmap, analyzer, true);
        // 4. replace other conjuncts and conjuncts
        otherJoinConjuncts = Expr.substituteList(otherJoinConjuncts, originToIntermediateSmap, analyzer, false);
        if (votherJoinConjunct != null) {
            votherJoinConjunct =
                    Expr.substituteList(Arrays.asList(votherJoinConjunct), originToIntermediateSmap, analyzer, false)
                            .get(0);
        }
        conjuncts = Expr.substituteList(conjuncts, originToIntermediateSmap, analyzer, false);
        if (vconjunct != null) {
            vconjunct = Expr.substituteList(Arrays.asList(vconjunct), originToIntermediateSmap, analyzer, false).get(0);
        }
        // 5. replace tuple is null expr
        TupleIsNullPredicate.substitueListForTupleIsNull(vSrcToOutputSMap.getLhs(), originTidsToIntermediateTidMap);
    }

    /**
     * Holds the source scan slots of a <SlotRef> = <SlotRef> join predicate.
     * The underlying table and column on both sides have stats.
     */
    public static final class EqJoinConjunctScanSlots {
        private final Expr eqJoinConjunct;
        private final SlotDescriptor lhs;
        private final SlotDescriptor rhs;

        private EqJoinConjunctScanSlots(Expr eqJoinConjunct, SlotDescriptor lhs, SlotDescriptor rhs) {
            this.eqJoinConjunct = eqJoinConjunct;
            this.lhs = lhs;
            this.rhs = rhs;
        }

        // Convenience functions. They return double to avoid excessive casts in callers.
        public double lhsNdv() {
            // return the estimated number of rows in this partition (-1 if unknown)
            return Math.min(lhs.getStats().getNumDistinctValues(), lhsNumRows());
        }

        public double rhsNdv() {
            return Math.min(rhs.getStats().getNumDistinctValues(), rhsNumRows());
        }

        public double lhsNumRows() {
            TableIf table = lhs.getParent().getTable();
            Preconditions.checkState(table instanceof OlapTable);
            return table.getRowCount();
        }

        public double rhsNumRows() {
            TableIf table = rhs.getParent().getTable();
            Preconditions.checkState(table instanceof OlapTable);
            return table.getRowCount();
        }

        public TupleId lhsTid() {
            return lhs.getParent().getId();
        }

        public TupleId rhsTid() {
            return rhs.getParent().getId();
        }

        /**
         * Returns a new EqJoinConjunctScanSlots for the given equi-join conjunct or null if
         * the given conjunct is not of the form <SlotRef> = <SlotRef> or if the underlying
         * table/column of at least one side is missing stats.
         */
        public static EqJoinConjunctScanSlots create(Expr eqJoinConjunct) {
            if (!Expr.IS_EQ_BINARY_PREDICATE.apply(eqJoinConjunct)) {
                return null;
            }
            SlotDescriptor lhsScanSlot = eqJoinConjunct.getChild(0).findSrcScanSlot();
            if (lhsScanSlot == null || !hasNumRowsAndNdvStats(lhsScanSlot)) {
                return null;
            }
            SlotDescriptor rhsScanSlot = eqJoinConjunct.getChild(1).findSrcScanSlot();
            if (rhsScanSlot == null || !hasNumRowsAndNdvStats(rhsScanSlot)) {
                return null;
            }
            return new EqJoinConjunctScanSlots(eqJoinConjunct, lhsScanSlot, rhsScanSlot);
        }

        private static boolean hasNumRowsAndNdvStats(SlotDescriptor slotDesc) {
            if (slotDesc.getColumn() == null) {
                return false;
            }
            if (!slotDesc.getStats().hasNumDistinctValues()) {
                return false;
            }
            return true;
        }

        /**
         * Groups the given EqJoinConjunctScanSlots by the lhs/rhs tuple combination
         * and returns the result as a map.
         */
        public static Map<Pair<TupleId, TupleId>, List<EqJoinConjunctScanSlots>> groupByJoinedTupleIds(
                List<EqJoinConjunctScanSlots> eqJoinConjunctSlots) {
            Map<Pair<TupleId, TupleId>, List<EqJoinConjunctScanSlots>> scanSlotsByJoinedTids = new LinkedHashMap<>();
            for (EqJoinConjunctScanSlots slots : eqJoinConjunctSlots) {
                Pair<TupleId, TupleId> tids = Pair.of(slots.lhsTid(), slots.rhsTid());
                List<EqJoinConjunctScanSlots> scanSlots = scanSlotsByJoinedTids.get(tids);
                if (scanSlots == null) {
                    scanSlots = new ArrayList<>();
                    scanSlotsByJoinedTids.put(tids, scanSlots);
                }
                scanSlots.add(slots);
            }
            return scanSlotsByJoinedTids;
        }

        @Override
        public String toString() {
            return eqJoinConjunct.toSql();
        }
    }

    private long getJoinCardinality() {
        Preconditions.checkState(joinOp.isInnerJoin() || joinOp.isOuterJoin());

        long lhsCard = getChild(0).cardinality;
        long rhsCard = getChild(1).cardinality;
        if (lhsCard == -1 || rhsCard == -1) {
            return lhsCard;
        }

        // Collect join conjuncts that are eligible to participate in cardinality estimation.
        List<EqJoinConjunctScanSlots> eqJoinConjunctSlots = new ArrayList<>();
        for (Expr eqJoinConjunct : eqJoinConjuncts) {
            EqJoinConjunctScanSlots slots = EqJoinConjunctScanSlots.create(eqJoinConjunct);
            if (slots != null) {
                eqJoinConjunctSlots.add(slots);
            }
        }

        if (eqJoinConjunctSlots.isEmpty()) {
            // There are no eligible equi-join conjuncts.
            return lhsCard;
        }

        return getGenericJoinCardinality(eqJoinConjunctSlots, lhsCard, rhsCard);
    }

    /**
     * Returns the estimated join cardinality of a generic N:M inner or outer join based
     * on the given list of equi-join conjunct slots and the join input cardinalities.
     * The returned result is >= 0.
     * The list of join conjuncts must be non-empty and the cardinalities must be >= 0.
     * <p>
     * Generic estimation:
     * cardinality = |child(0)| * |child(1)| / max(NDV(L.c), NDV(R.d))
     * - case A: NDV(L.c) <= NDV(R.d)
     * every row from child(0) joins with |child(1)| / NDV(R.d) rows
     * - case B: NDV(L.c) > NDV(R.d)
     * every row from child(1) joins with |child(0)| / NDV(L.c) rows
     * - we adjust the NDVs from both sides to account for predicates that may
     * might have reduce the cardinality and NDVs
     */
    private long getGenericJoinCardinality(List<EqJoinConjunctScanSlots> eqJoinConjunctSlots,
            long lhsCard, long rhsCard) {
        Preconditions.checkState(joinOp.isInnerJoin() || joinOp.isOuterJoin());
        Preconditions.checkState(!eqJoinConjunctSlots.isEmpty());
        Preconditions.checkState(lhsCard >= 0 && rhsCard >= 0);

        long result = -1;
        for (EqJoinConjunctScanSlots slots : eqJoinConjunctSlots) {
            // Adjust the NDVs on both sides to account for predicates. Intuitively, the NDVs
            // should only decrease. We ignore adjustments that would lead to an increase.
            double lhsAdjNdv = slots.lhsNdv();
            if (slots.lhsNumRows() > lhsCard) {
                lhsAdjNdv *= lhsCard / slots.lhsNumRows();
            }
            double rhsAdjNdv = slots.rhsNdv();
            if (slots.rhsNumRows() > rhsCard) {
                rhsAdjNdv *= rhsCard / slots.rhsNumRows();
            }
            // A lower limit of 1 on the max Adjusted Ndv ensures we don't estimate
            // cardinality more than the max possible.
            long joinCard = CheckedMath.checkedMultiply(
                    Math.round((lhsCard / Math.max(1, Math.max(lhsAdjNdv, rhsAdjNdv)))), rhsCard);
            if (result == -1) {
                result = joinCard;
            } else {
                result = Math.min(result, joinCard);
            }
        }
        Preconditions.checkState(result >= 0);
        return result;
    }


    @Override
    public void computeStats(Analyzer analyzer) throws UserException {
        super.computeStats(analyzer);

        if (!analyzer.safeIsEnableJoinReorderBasedCost()) {
            return;
        }

        StatsRecursiveDerive.getStatsRecursiveDerive().statsRecursiveDerive(this);
        cardinality = (long) statsDeriveResult.getRowCount();

        if (LOG.isDebugEnabled()) {
            LOG.debug("stats HashJoin:" + id + ", cardinality: " + cardinality);
        }
    }

    @Override
    protected void computeOldCardinality() {
        // For a join between child(0) and child(1), we look for join conditions "L.c = R.d"
        // (with L being from child(0) and R from child(1)) and use as the cardinality
        // estimate the maximum of
        //   child(0).cardinality * R.cardinality / # distinct values for R.d
        //     * child(1).cardinality / R.cardinality
        // across all suitable join conditions, which simplifies to
        //   child(0).cardinality * child(1).cardinality / # distinct values for R.d
        // The reasoning is that
        // - each row in child(0) joins with R.cardinality/#DV_R.d rows in R
        // - each row in R is 'present' in child(1).cardinality / R.cardinality rows in
        //   child(1)
        //
        // This handles the very frequent case of a fact table/dimension table join
        // (aka foreign key/primary key join) if the primary key is a single column, with
        // possible additional predicates against the dimension table. An example:
        // FROM FactTbl F JOIN Customers C D ON (F.cust_id = C.id) ... WHERE C.region = 'US'
        // - if there are 5 regions, the selectivity of "C.region = 'US'" would be 0.2
        //   and the output cardinality of the Customers scan would be 0.2 * # rows in
        //   Customers
        // - # rows in Customers == # of distinct values for Customers.id
        // - the output cardinality of the join would be F.cardinality * 0.2

        long maxNumDistinct = 0;
        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            Expr lhsJoinExpr = eqJoinPredicate.getChild(0);
            Expr rhsJoinExpr = eqJoinPredicate.getChild(1);
            if (lhsJoinExpr.unwrapSlotRef() == null) {
                continue;
            }
            SlotRef rhsSlotRef = rhsJoinExpr.unwrapSlotRef();
            if (rhsSlotRef == null) {
                continue;
            }
            SlotDescriptor slotDesc = rhsSlotRef.getDesc();
            if (slotDesc == null) {
                continue;
            }
            ColumnStats stats = slotDesc.getStats();
            if (!stats.hasNumDistinctValues()) {
                continue;
            }
            long numDistinct = stats.getNumDistinctValues();
            // TODO rownum
            //Table rhsTbl = slotDesc.getParent().getTableFamilyGroup().getBaseTable();
            // if (rhsTbl != null && rhsTbl.getNumRows() != -1) {
            // we can't have more distinct values than rows in the table, even though
            // the metastore stats may think so
            // LOG.info(
            //   "#distinct=" + numDistinct + " #rows=" + Long.toString(rhsTbl.getNumRows()));
            // numDistinct = Math.min(numDistinct, rhsTbl.getNumRows());
            // }
            maxNumDistinct = Math.max(maxNumDistinct, numDistinct);
            LOG.debug("min slotref: {}, #distinct: {}", rhsSlotRef.toSql(), numDistinct);
        }

        if (maxNumDistinct == 0) {
            // if we didn't find any suitable join predicates or don't have stats
            // on the relevant columns, we very optimistically assume we're doing an
            // FK/PK join (which doesn't alter the cardinality of the left-hand side)
            cardinality = getChild(0).cardinality;
        } else {
            cardinality = Math.round(
                    (double) getChild(0).cardinality * (double) getChild(1).cardinality / (double) maxNumDistinct);
            LOG.debug("lhs card: {}, rhs card: {}", getChild(0).cardinality, getChild(1).cardinality);
        }
        LOG.debug("stats HashJoin: cardinality {}", cardinality);
    }

    /**
     * Unwraps the SlotRef in expr and returns the NDVs of it.
     * Returns -1 if the NDVs are unknown or if expr is not a SlotRef.
     */
    private long getNdv(Expr expr) {
        SlotRef slotRef = expr.unwrapSlotRef(false);
        if (slotRef == null) {
            return -1;
        }
        SlotDescriptor slotDesc = slotRef.getDesc();
        if (slotDesc == null) {
            return -1;
        }
        ColumnStats stats = slotDesc.getStats();
        if (!stats.hasNumDistinctValues()) {
            return -1;
        }
        return stats.getNumDistinctValues();
    }

    /**
     * Returns the estimated cardinality of a semi join node.
     * For a left semi join between child(0) and child(1), we look for equality join
     * conditions "L.c = R.d" (with L being from child(0) and R from child(1)) and use as
     * the cardinality estimate the minimum of
     * |child(0)| * Min(NDV(L.c), NDV(R.d)) / NDV(L.c)
     * over all suitable join conditions. The reasoning is that:
     * - each row in child(0) is returned at most once
     * - the probability of a row in child(0) having a match in R is
     * Min(NDV(L.c), NDV(R.d)) / NDV(L.c)
     * <p>
     * For a left anti join we estimate the cardinality as the minimum of:
     * |L| * Max(NDV(L.c) - NDV(R.d), NDV(L.c)) / NDV(L.c)
     * over all suitable join conditions. The reasoning is that:
     * - each row in child(0) is returned at most once
     * - if NDV(L.c) > NDV(R.d) then the probability of row in L having a match
     * in child(1) is (NDV(L.c) - NDV(R.d)) / NDV(L.c)
     * - otherwise, we conservatively use |L| to avoid underestimation
     * <p>
     * We analogously estimate the cardinality for right semi/anti joins, and treat the
     * null-aware anti join like a regular anti join
     */
    private long getSemiJoinCardinality() {
        Preconditions.checkState(joinOp.isSemiJoin());

        // Return -1 if the cardinality of the returned side is unknown.
        double cardinality;
        if (joinOp == JoinOperator.RIGHT_SEMI_JOIN || joinOp == JoinOperator.RIGHT_ANTI_JOIN) {
            if (getChild(1).cardinality == -1) {
                return -1;
            }
            cardinality = getChild(1).cardinality;
        } else {
            if (getChild(0).cardinality == -1) {
                return -1;
            }
            cardinality = getChild(0).cardinality;
        }
        double minSelectivity = 1.0;
        for (Expr eqJoinPredicate : eqJoinConjuncts) {
            long lhsNdv = getNdv(eqJoinPredicate.getChild(0));
            lhsNdv = Math.min(lhsNdv, getChild(0).cardinality);
            long rhsNdv = getNdv(eqJoinPredicate.getChild(1));
            rhsNdv = Math.min(rhsNdv, getChild(1).cardinality);

            // Skip conjuncts with unknown NDV on either side.
            if (lhsNdv == -1 || rhsNdv == -1) {
                continue;
            }

            double selectivity = 1.0;
            switch (joinOp) {
                case LEFT_SEMI_JOIN: {
                    selectivity = (double) Math.min(lhsNdv, rhsNdv) / (double) (lhsNdv);
                    break;
                }
                case RIGHT_SEMI_JOIN: {
                    selectivity = (double) Math.min(lhsNdv, rhsNdv) / (double) (rhsNdv);
                    break;
                }
                case LEFT_ANTI_JOIN:
                case NULL_AWARE_LEFT_ANTI_JOIN: {
                    selectivity = (double) (lhsNdv > rhsNdv ? (lhsNdv - rhsNdv) : lhsNdv) / (double) lhsNdv;
                    break;
                }
                case RIGHT_ANTI_JOIN: {
                    selectivity = (double) (rhsNdv > lhsNdv ? (rhsNdv - lhsNdv) : rhsNdv) / (double) rhsNdv;
                    break;
                }
                default:
                    Preconditions.checkState(false);
            }
            minSelectivity = Math.min(minSelectivity, selectivity);
        }

        Preconditions.checkState(cardinality != -1);
        return Math.round(cardinality * minSelectivity);
    }

    @Override
    protected String debugString() {
        return MoreObjects.toStringHelper(this).add("eqJoinConjuncts", eqJoinConjunctsDebugString())
                .addValue(super.debugString()).toString();
    }

    private String eqJoinConjunctsDebugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        for (BinaryPredicate expr : eqJoinConjuncts) {
            helper.add("lhs", expr.getChild(0)).add("rhs", expr.getChild(1));
        }
        return helper.toString();
    }

    @Override
    public void getMaterializedIds(Analyzer analyzer, List<SlotId> ids) {
        super.getMaterializedIds(analyzer, ids);
        // we also need to materialize everything referenced by eqJoinConjuncts
        // and otherJoinConjuncts
        for (Expr eqJoinPredicate : eqJoinConjuncts) {
            eqJoinPredicate.getIds(null, ids);
        }
        for (Expr e : otherJoinConjuncts) {
            e.getIds(null, ids);
        }
    }

    //nereids only
    public void addSlotIdToHashOutputSlotIds(SlotId slotId) {
        hashOutputSlotIds.add(slotId);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HASH_JOIN_NODE;
        msg.hash_join_node = new THashJoinNode();
        msg.hash_join_node.join_op = joinOp.toThrift();
        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            TEqJoinCondition eqJoinCondition = new TEqJoinCondition(eqJoinPredicate.getChild(0).treeToThrift(),
                    eqJoinPredicate.getChild(1).treeToThrift());
            eqJoinCondition.setOpcode(eqJoinPredicate.getOp().getOpcode());
            msg.hash_join_node.addToEqJoinConjuncts(eqJoinCondition);
        }
        for (Expr e : otherJoinConjuncts) {
            msg.hash_join_node.addToOtherJoinConjuncts(e.treeToThrift());
        }

        // use in vec exec engine to replace otherJoinConjuncts
        if (votherJoinConjunct != null) {
            msg.hash_join_node.setVotherJoinConjunct(votherJoinConjunct.treeToThrift());
        }
        if (hashOutputSlotIds != null) {
            for (SlotId slotId : hashOutputSlotIds) {
                msg.hash_join_node.addToHashOutputSlotIds(slotId.asInt());
            }
        }
        if (vSrcToOutputSMap != null) {
            for (int i = 0; i < vSrcToOutputSMap.size(); i++) {
                // TODO: Enable it after we support new optimizers
                // if (ConnectContext.get().getSessionVariable().isEnableNereidsPlanner()) {
                //     msg.addToProjections(vSrcToOutputSMap.getLhs().get(i).treeToThrift());
                // } else
                msg.hash_join_node.addToSrcExprList(vSrcToOutputSMap.getLhs().get(i).treeToThrift());
            }
        }
        if (vOutputTupleDesc != null) {
            msg.hash_join_node.setVoutputTupleId(vOutputTupleDesc.getId().asInt());
            // TODO Enable it after we support new optimizers
            // msg.setOutputTupleId(vOutputTupleDesc.getId().asInt());
        }
        if (vIntermediateTupleDescList != null) {
            for (TupleDescriptor tupleDescriptor : vIntermediateTupleDescList) {
                msg.hash_join_node.addToVintermediateTupleIdList(tupleDescriptor.getId().asInt());
            }
        }
    }

    @Override
    public String getNodeExplainString(String detailPrefix, TExplainLevel detailLevel) {
        String distrModeStr = "";
        if (isColocate) {
            distrModeStr = "COLOCATE[" + colocateReason + "]";
        } else {
            distrModeStr = distrMode.toString();
        }
        StringBuilder output =
                new StringBuilder().append(detailPrefix).append("join op: ").append(joinOp.toString()).append("(")
                        .append(distrModeStr).append(")").append("[").append(colocateReason).append("]\n");

        if (detailLevel == TExplainLevel.BRIEF) {
            return output.toString();
        }

        for (BinaryPredicate eqJoinPredicate : eqJoinConjuncts) {
            output.append(detailPrefix).append("equal join conjunct: ").append(eqJoinPredicate.toSql()).append("\n");
        }
        if (!otherJoinConjuncts.isEmpty()) {
            output.append(detailPrefix).append("other join predicates: ")
                    .append(getExplainString(otherJoinConjuncts)).append("\n");
        }
        if (!conjuncts.isEmpty()) {
            output.append(detailPrefix).append("other predicates: ").append(getExplainString(conjuncts)).append("\n");
        }
        if (!runtimeFilters.isEmpty()) {
            output.append(detailPrefix).append("runtime filters: ");
            output.append(getRuntimeFilterExplainString(true));
        }
        output.append(detailPrefix).append(String.format("cardinality=%s", cardinality)).append("\n");
        // todo unify in plan node
        if (vOutputTupleDesc != null) {
            output.append(detailPrefix).append("vec output tuple id: ").append(vOutputTupleDesc.getId()).append("\n");
        }
        if (vIntermediateTupleDescList != null) {
            output.append(detailPrefix).append("vIntermediate tuple ids: ");
            for (TupleDescriptor tupleDescriptor : vIntermediateTupleDescList) {
                output.append(tupleDescriptor.getId()).append(" ");
            }
            output.append("\n");
        }
        if (outputSlotIds != null) {
            output.append(detailPrefix).append("output slot ids: ");
            for (SlotId slotId : outputSlotIds) {
                output.append(slotId).append(" ");
            }
            output.append("\n");
        }
        if (hashOutputSlotIds != null) {
            output.append(detailPrefix).append("hash output slot ids: ");
            for (SlotId slotId : hashOutputSlotIds) {
                output.append(slotId).append(" ");
            }
            output.append("\n");
        }
        return output.toString();
    }

    @Override
    public int getNumInstances() {
        return Math.max(children.get(0).getNumInstances(), children.get(1).getNumInstances());
    }

    public boolean isShuffleJoin() {
        return distrMode == DistributionMode.PARTITIONED;
    }

    public enum DistributionMode {
        NONE("NONE"), BROADCAST("BROADCAST"), PARTITIONED("PARTITIONED"), BUCKET_SHUFFLE("BUCKET_SHUFFLE");

        private final String description;

        private DistributionMode(String descr) {
            this.description = descr;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    @Override
    public void convertToVectoriezd() {
        if (!otherJoinConjuncts.isEmpty()) {
            votherJoinConjunct = convertConjunctsToAndCompoundPredicate(otherJoinConjuncts);
            initCompoundPredicate(votherJoinConjunct);
        }
        super.convertToVectoriezd();
    }

    /**
     * If parent wants to get hash join node tupleids,
     * it will call this function instead of read properties directly.
     * The reason is that the tuple id of vOutputTupleDesc the real output tuple id for hash join node.
     * <p>
     * If you read the properties of @tupleids directly instead of this function,
     * it reads the input id of the current node.
     */
    @Override
    public ArrayList<TupleId> getTupleIds() {
        Preconditions.checkState(tupleIds != null);
        if (vOutputTupleDesc != null) {
            return Lists.newArrayList(vOutputTupleDesc.getId());
        }
        return tupleIds;
    }

    @Override
    public ArrayList<TupleId> getOutputTblRefIds() {
        if (vOutputTupleDesc != null) {
            return Lists.newArrayList(vOutputTupleDesc.getId());
        }
        switch (joinOp) {
            case LEFT_SEMI_JOIN:
            case LEFT_ANTI_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
                return getChild(0).getOutputTblRefIds();
            case RIGHT_SEMI_JOIN:
            case RIGHT_ANTI_JOIN:
                return getChild(1).getOutputTblRefIds();
            default:
                return getTblRefIds();
        }
    }

    @Override
    public List<TupleId> getOutputTupleIds() {
        if (vOutputTupleDesc != null) {
            return Lists.newArrayList(vOutputTupleDesc.getId());
        }
        switch (joinOp) {
            case LEFT_SEMI_JOIN:
            case LEFT_ANTI_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
                return getChild(0).getOutputTupleIds();
            case RIGHT_SEMI_JOIN:
            case RIGHT_ANTI_JOIN:
                return getChild(1).getOutputTupleIds();
            default:
                return tupleIds;
        }
    }

    private boolean isMaterailizedByChild(SlotDescriptor slotDesc, ExprSubstitutionMap smap) {
        if (slotDesc.isMaterialized()) {
            return true;
        }
        Expr child = smap.get(new SlotRef(slotDesc));
        if (child == null) {
            return false;
        }
        List<SlotRef> slotRefList = Lists.newArrayList();
        child.collect(SlotRef.class, slotRefList);
        for (SlotRef slotRef : slotRefList) {
            if (slotRef.getDesc() != null && !slotRef.getDesc().isMaterialized()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Used by nereids.
     */
    public void setOtherJoinConjuncts(List<Expr> otherJoinConjuncts) {
        this.otherJoinConjuncts = otherJoinConjuncts;
    }

    /**
     * Used by nereids.
     */
    public void setvOutputTupleDesc(TupleDescriptor vOutputTupleDesc) {
        this.vOutputTupleDesc = vOutputTupleDesc;
    }

    /**
     * Used by nereids.
     */
    public void setvIntermediateTupleDescList(List<TupleDescriptor> vIntermediateTupleDescList) {
        this.vIntermediateTupleDescList = vIntermediateTupleDescList;
    }

    /**
     * Used by nereids.
     */
    public void setvSrcToOutputSMap(List<Expr> lhs) {
        this.vSrcToOutputSMap = new ExprSubstitutionMap(lhs, Collections.emptyList());
    }
}
