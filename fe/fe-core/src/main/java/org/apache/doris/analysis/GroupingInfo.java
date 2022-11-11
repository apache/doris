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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.stream.Collectors;

public class GroupingInfo {
    public static final String COL_GROUPING_ID = "GROUPING_ID";
    public static final String GROUPING_PREFIX = "GROUPING_PREFIX_";
    private VirtualSlotRef groupingIDSlot;
    private TupleDescriptor virtualTuple;
    private TupleDescriptor outputTupleDesc;
    private ExprSubstitutionMap outputTupleSmap;
    private List<SlotDescriptor> groupingSlotDescList;
    private Set<VirtualSlotRef> virtualSlotRefs;
    private List<BitSet> groupingIdList;
    private GroupByClause.GroupingType groupingType;
    private BitSet bitSetAll;

    private List<Expr> preRepeatExprs;

    public GroupingInfo(Analyzer analyzer, GroupByClause groupByClause) throws AnalysisException {
        this.groupingType = groupByClause.getGroupingType();
        virtualSlotRefs = new LinkedHashSet<>();
        virtualTuple = analyzer.getDescTbl().createTupleDescriptor("VIRTUAL_TUPLE");
        groupingIDSlot = new VirtualSlotRef(COL_GROUPING_ID, Type.BIGINT, virtualTuple, new ArrayList<>());
        groupingIDSlot.analyze(analyzer);
        virtualSlotRefs.add(groupingIDSlot);

        outputTupleDesc = analyzer.getDescTbl().createTupleDescriptor("repeat-tuple");
        outputTupleSmap = new ExprSubstitutionMap();
        groupingSlotDescList = Lists.newArrayList();
        preRepeatExprs = Lists.newArrayList();
    }

    public Set<VirtualSlotRef> getVirtualSlotRefs() {
        return virtualSlotRefs;
    }

    public TupleDescriptor getVirtualTuple() {
        return virtualTuple;
    }

    public TupleDescriptor getOutputTupleDesc() {
        return outputTupleDesc;
    }

    public ExprSubstitutionMap getOutputTupleSmap() {
        return outputTupleSmap;
    }

    public List<SlotDescriptor> getGroupingSlotDescList() {
        return groupingSlotDescList;
    }

    public List<BitSet> getGroupingIdList() {
        return groupingIdList;
    }

    public List<Expr> getPreRepeatExprs() {
        return preRepeatExprs;
    }

    public void substitutePreRepeatExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
        ArrayList<Expr> originalPreRepeatExprs = new ArrayList<>(preRepeatExprs);
        preRepeatExprs = Expr.substituteList(preRepeatExprs, smap, analyzer, true);

        // remove unmaterialized slotRef from preRepeatExprs
        ArrayList<Expr> materializedPreRepeatExprs = new ArrayList<>();
        ArrayList<Expr> unMaterializedSlotRefs = new ArrayList<>();
        for (int i = 0; i < preRepeatExprs.size(); ++i) {
            Expr expr = preRepeatExprs.get(i);
            if (expr instanceof SlotRef && !((SlotRef) expr).getDesc().isMaterialized()) {
                unMaterializedSlotRefs.add(originalPreRepeatExprs.get(i));
            } else {
                materializedPreRepeatExprs.add(expr);
            }
        }
        preRepeatExprs = materializedPreRepeatExprs;

        // set slotRef unmaterialized in outputTupleSmap
        for (Expr expr : unMaterializedSlotRefs) {
            Expr rExpr = outputTupleSmap.get(expr);
            if (rExpr instanceof SlotRef) {
                ((SlotRef) rExpr).getDesc().setIsMaterialized(false);
            }
        }
    }

    // generate virtual slots for grouping or grouping_id functions
    public VirtualSlotRef addGroupingSlots(List<Expr> realSlots, Analyzer analyzer) throws AnalysisException {
        String colName = realSlots.stream().map(expr -> expr.toSql()).collect(Collectors.joining("_"));
        colName = GROUPING_PREFIX + colName;
        VirtualSlotRef virtualSlot = new VirtualSlotRef(colName, Type.BIGINT, virtualTuple, realSlots);
        virtualSlot.analyze(analyzer);
        if (virtualSlotRefs.contains(virtualSlot)) {
            for (VirtualSlotRef vs : virtualSlotRefs) {
                if (vs.equals(virtualSlot)) {
                    return vs;
                }
            }
        }
        virtualSlotRefs.add(virtualSlot);
        return virtualSlot;
    }

    // generate the bitmap that repeated node will repeat rows according to
    public void buildRepeat(ArrayList<Expr> groupingExprs, List<ArrayList<Expr>> groupingSetList) {
        groupingIdList = new ArrayList<>();
        bitSetAll = new BitSet();
        bitSetAll.set(0, groupingExprs.size(), true);
        switch (groupingType) {
            case CUBE:
                // it will generate the full permutation bitmap of cube item
                // e.g. cube (k1,k2,k3) the result is ["{}", "{1}", "{0}", "{0, 1}", "{2}", "{1, 2}", "{0, 1, 2}",
                // "{0, 2}"]
                for (int i = 0; i < (1 << groupingExprs.size()); i++) {
                    BitSet bitSet = new BitSet();
                    for (int j = 0; j < groupingExprs.size(); j++) {
                        if ((i & (1 << j)) > 0) {
                            bitSet.set(j, true);
                        }
                    }
                    groupingIdList.add(bitSet);
                }
                break;

            case ROLLUP:
                for (int i = 0; i <= groupingExprs.size(); i++) {
                    BitSet bitSet = new BitSet();
                    bitSet.set(0, i);
                    groupingIdList.add(bitSet);
                }
                break;

            case GROUPING_SETS:
                for (ArrayList<Expr> list : groupingSetList) {
                    BitSet bitSet = new BitSet();
                    for (int i = 0; i < groupingExprs.size(); i++) {
                        bitSet.set(i, list.contains(groupingExprs.get(i)));
                    }
                    if (!groupingIdList.contains(bitSet)) {
                        groupingIdList.add(bitSet);
                    }
                }
                break;

            default:
                Preconditions.checkState(false);
        }
        groupingExprs.addAll(virtualSlotRefs);
    }

    // generate grouping function's value
    public List<List<Long>> genGroupingList(ArrayList<Expr> groupingExprs) throws AnalysisException {
        List<List<Long>> groupingList = new ArrayList<>();
        for (SlotRef slot : virtualSlotRefs) {
            List<Long> glist = new ArrayList<>();
            for (BitSet bitSet : groupingIdList) {
                long l = 0L;
                // for all column, using for group by
                if (slot.getColumnName().equalsIgnoreCase(COL_GROUPING_ID)) {
                    BitSet newBitSet = new BitSet();
                    for (int i = 0; i < bitSetAll.length(); ++i) {
                        newBitSet.set(i, bitSet.get(bitSetAll.length() - i - 1));
                    }
                    newBitSet.flip(0, bitSetAll.length());
                    newBitSet.and(bitSetAll);
                    for (int i = 0; i < newBitSet.length(); ++i) {
                        l += newBitSet.get(i) ? (1L << i) : 0L;
                    }
                } else {
                    // for grouping[_id] functions
                    int slotSize = ((VirtualSlotRef) slot).getRealSlots().size();
                    for (int i = 0; i < slotSize; ++i) {
                        int j = groupingExprs.indexOf(((VirtualSlotRef) slot).getRealSlots().get(i));
                        if (j < 0 || j >= bitSet.size()) {
                            throw new AnalysisException("Column " + ((VirtualSlotRef) slot).getRealColumnName()
                                    + " in GROUP_ID() does not exist in GROUP BY clause.");
                        }
                        l += bitSet.get(j) ? 0L : (1L << (slotSize - i - 1));
                    }
                }
                glist.add(l);
            }
            groupingList.add(glist);
        }
        return groupingList;
    }

    public void genOutputTupleDescAndSMap(Analyzer analyzer, ArrayList<Expr> groupingAndVirtualSlotExprs,
            List<FunctionCallExpr> aggExprs) {
        List<Expr> groupingExprs = Lists.newArrayList();
        List<Expr> virtualSlotExprs = Lists.newArrayList();
        for (Expr expr : groupingAndVirtualSlotExprs) {
            if (expr instanceof VirtualSlotRef) {
                virtualSlotExprs.add(expr);
            } else {
                groupingExprs.add(expr);
            }
        }
        for (Expr expr : groupingExprs) {
            SlotDescriptor slotDesc = addSlot(analyzer, expr);
            slotDesc.setIsNullable(true);
            groupingSlotDescList.add(slotDesc);
            preRepeatExprs.add(expr);
            // register equivalence between grouping slot and grouping expr;
            // do this only when the grouping expr isn't a constant, otherwise
            // it'll simply show up as a gratuitous HAVING predicate
            // (which would actually be incorrect if the constant happens to be NULL)
            if (!expr.isConstant()) {
                analyzer.createAuxEquivPredicate(new SlotRef(slotDesc), expr.clone());
            }
        }
        List<SlotRef> aggSlot = Lists.newArrayList();
        aggExprs.forEach(expr -> aggSlot.addAll(getSlotRefChildren(expr)));
        for (SlotRef slotRef : aggSlot) {
            addSlot(analyzer, slotRef);
            preRepeatExprs.add(slotRef);
        }
        for (Expr expr : virtualSlotExprs) {
            addSlot(analyzer, expr);
        }
    }

    private SlotDescriptor addSlot(Analyzer analyzer, Expr expr) {
        SlotDescriptor slotDesc = analyzer.addSlotDescriptor(outputTupleDesc);
        slotDesc.initFromExpr(expr);
        slotDesc.setIsMaterialized(true);
        if (expr instanceof SlotRef) {
            slotDesc.setColumn(((SlotRef) expr).getColumn());
        }
        if (expr instanceof VirtualSlotRef) {
            outputTupleSmap.put(expr.clone(), new VirtualSlotRef(slotDesc));
        } else {
            outputTupleSmap.put(expr.clone(), new SlotRef(slotDesc));
        }
        return slotDesc;
    }

    private List<SlotRef> getSlotRefChildren(Expr root) {
        List<SlotRef> result = new ArrayList<>();
        for (Expr child : root.getChildren()) {
            if (child instanceof SlotRef) {
                result.add((SlotRef) child);
            } else {
                result.addAll(getSlotRefChildren(child));
            }
        }
        return result;
    }

    public void substituteGroupingFn(List<Expr> exprs, Analyzer analyzer) throws AnalysisException {
        if (groupingType == GroupByClause.GroupingType.GROUP_BY) {
            throw new AnalysisException("cannot use GROUPING functions without [grouping sets|rollup|cube] a"
                    + "clause or grouping sets only have one element.");
        }
        ListIterator<Expr> i = exprs.listIterator();
        while (i.hasNext()) {
            Expr expr = i.next();
            substituteGroupingFn(expr, analyzer);
        }
    }

    public void substituteGroupingFn(Expr expr, Analyzer analyzer) throws AnalysisException {
        if (expr instanceof GroupingFunctionCallExpr) {
            // if is substituted skip
            if (expr.getChildren().size() == 1 && expr.getChild(0) instanceof VirtualSlotRef) {
                return;
            }
            VirtualSlotRef vSlot = addGroupingSlots(((GroupingFunctionCallExpr) expr).getRealSlot(), analyzer);
            ((GroupingFunctionCallExpr) expr).resetChild(vSlot);
            expr.analyze(analyzer);
        } else if (expr.getChildren() != null && expr.getChildren().size() > 0) {
            for (Expr child : expr.getChildren()) {
                substituteGroupingFn(child, analyzer);
            }
        }
    }
}
