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

import org.apache.doris.analysis.AnalyticExpr;
import org.apache.doris.analysis.AnalyticInfo;
import org.apache.doris.analysis.AnalyticWindow;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.OrderByElement;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.analysis.TupleIsNullPredicate;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TPartitionType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * The analytic planner adds plan nodes to an existing plan tree in order to
 * implement the AnalyticInfo of a given query stmt. The resulting plan reflects
 * similarities among analytic exprs with respect to partitioning, ordering and
 * windowing to reduce data exchanges and sorts (the exchanges and sorts are currently
 * not minimal). The generated plan has the following structure:
 * ...
 * (
 *  (
 *    (
 *      analytic node  <-- group of analytic exprs with compatible window
 *    )+               <-- group of analytic exprs with compatible ordering
 *    sort node?
 *  )+                 <-- group of analytic exprs with compatible partitioning
 *  hash exchange?
 * )*                  <-- group of analytic exprs that have different partitioning
 * input plan node
 * ...
 */
public class AnalyticPlanner {
    private final static Logger LOG = LoggerFactory.getLogger(AnalyticPlanner.class);

    private final AnalyticInfo analyticInfo;
    private final Analyzer analyzer;

    private final PlannerContext ctx_;

    public AnalyticPlanner(AnalyticInfo analyticInfo, Analyzer analyzer, PlannerContext ctx) {
        this.analyticInfo = analyticInfo;
        this.analyzer = analyzer;
        this.ctx_ = ctx;
    }

    /**
     * Return plan tree that augments 'root' with plan nodes that implement single-node
     * evaluation of the AnalyticExprs in analyticInfo.
     * This plan takes into account a possible hash partition of its input on
     * 'groupingExprs'; if this is non-null, it returns in 'inputPartitionExprs'
     * a subset of the grouping exprs which should be used for the aggregate
     * hash partitioning during the parallelization of 'root'.
     * TODO: when generating sort orders for the sort groups, optimize the ordering
     * of the partition exprs (so that subsequent sort operations see the input sorted
     * on a prefix of their required sort exprs)
     * TODO: when merging sort groups, recognize equivalent exprs
     * (using the equivalence classes) rather than looking for expr equality
     * @throws AnalysisException
     * @throws UserException
     */
    public PlanNode createSingleNodePlan(PlanNode root,
            List<Expr> groupingExprs, List<Expr> inputPartitionExprs) throws AnalysisException,
            UserException {
        List<WindowGroup> windowGroups = collectWindowGroups();

        for (int i = 0; i < windowGroups.size(); ++i) {
            windowGroups.get(i).init(analyzer, "wg-" + i);
        }

        List<SortGroup> sortGroups = collectSortGroups(windowGroups);
        mergeSortGroups(sortGroups);

        for (SortGroup g : sortGroups) {
            g.init();
        }

        List<PartitionGroup> partitionGroups = collectPartitionGroups(sortGroups);
        mergePartitionGroups(partitionGroups, root.getNumNodes());
        orderGroups(partitionGroups);

        if (groupingExprs != null) {
            Preconditions.checkNotNull(inputPartitionExprs);
            computeInputPartitionExprs(
                partitionGroups, groupingExprs, root.getNumNodes(), inputPartitionExprs);
        }

        PlanNode newRoot = root;
        for (PartitionGroup partitionGroup : partitionGroups) {
            for (int i = 0; i < partitionGroup.sortGroups.size(); ++i) {
                newRoot = createSortGroupPlan(newRoot, partitionGroup.sortGroups.get(i),
                                           i == 0 ? partitionGroup.partitionByExprs : null);
            }
        }

        // create equiv classes for newly added slots
        //    analyzer_.createIdentityEquivClasses();
        return newRoot;
    }

    /**
     * Coalesce sort groups that have compatible partition-by exprs and
     * have a prefix relationship.
     */
    private void mergeSortGroups(List<SortGroup> sortGroups) {
        boolean hasMerged = false;

        do {
            hasMerged = false;

            for (SortGroup sg1 : sortGroups) {
                for (SortGroup sg2 : sortGroups) {
                    if (sg1 != sg2 && sg1.isPrefixOf(sg2)) {
                        sg1.absorb(sg2);
                        sortGroups.remove(sg2);
                        hasMerged = true;
                        break;
                    }
                }

                if (hasMerged) {
                    break;
                }
            }
        } while (hasMerged);
    }

    /**
     * Coalesce partition groups for which the intersection of their
     * partition exprs has ndv estimate > numNodes, so that the resulting plan
     * still parallelizes across all nodes.
     */
    private void mergePartitionGroups(
        List<PartitionGroup> partitionGroups, int numNodes) {
        boolean hasMerged = false;

        do {
            hasMerged = false;

            for (PartitionGroup pg1 : partitionGroups) {
                for (PartitionGroup pg2 : partitionGroups) {
                    if (pg1 != pg2) {
                        long ndv = Expr.getNumDistinctValues(
                                       Expr.intersect(pg1.partitionByExprs, pg2.partitionByExprs));

                        if (ndv == -1 || ndv < 1 || ndv < numNodes) {
                            // didn't get a usable value or the number of partitions is too small
                            continue;
                        }

                        pg1.merge(pg2);
                        partitionGroups.remove(pg2);
                        hasMerged = true;
                        break;
                    }
                }

                if (hasMerged) {
                    break;
                }
            }
        } while (hasMerged);
    }

    /**
     * Determine the partition group that has the maximum intersection in terms
     * of the estimated ndv of the partition exprs with groupingExprs.
     * That partition group is placed at the front of partitionGroups, with its
     * partition exprs reduced to the intersection, and the intersecting groupingExprs
     * are returned in inputPartitionExprs.
     * @throws AnalysisException
     */
    private void computeInputPartitionExprs(List<PartitionGroup> partitionGroups,
            List<Expr> groupingExprs, int numNodes, List<Expr> inputPartitionExprs) throws AnalysisException {
        inputPartitionExprs.clear();
        // find partition group with maximum intersection
        long maxNdv = 0;
        PartitionGroup maxPg = null;
        List<Expr> maxGroupingExprs = null;

        for (PartitionGroup pg : partitionGroups) {
            List<Expr> l1 = Lists.newArrayList();
            List<Expr> l2 = Lists.newArrayList();

            //      Expr.intersect(analyzer_, pg.partitionByExprs, groupingExprs,
            //          analyzer_.getEquivClassSmap(), l1, l2);
            Expr.intersect(analyzer, pg.partitionByExprs, groupingExprs,
                           null, l1, l2);

            // TODO: also look at l2 and take the max?
            long ndv = Expr.getNumDistinctValues(l1);

            if (ndv < 1 || ndv < numNodes || ndv < maxNdv) {
                continue;
            }

            // found a better partition group
            maxPg = pg;
            maxPg.partitionByExprs = l1;
            maxGroupingExprs = l2;
            maxNdv = ndv;
        }

        if (maxNdv > numNodes) {
            Preconditions.checkNotNull(maxPg);
            // we found a partition group that gives us enough parallelism;
            // move it to the front
            partitionGroups.remove(maxPg);
            partitionGroups.add(0, maxPg);
            inputPartitionExprs.addAll(maxGroupingExprs);
        }
    }

    /**
     * Order partition groups (and the sort groups within them) by increasing
     * totalOutputTupleSize. This minimizes the total volume of data that needs to be
     * repartitioned and sorted.
     * Also move the non-partitioning partition group to the end.
     */
    private void orderGroups(List<PartitionGroup> partitionGroups) {
        // remove the non-partitioning group from partitionGroups
        PartitionGroup nonPartitioning = null;

        for (PartitionGroup pg : partitionGroups) {
            if (pg.partitionByExprs.isEmpty()) {
                nonPartitioning = pg;
                break;
            }
        }

        if (nonPartitioning != null) {
            partitionGroups.remove(nonPartitioning);
        }

        // order by ascending combined output tuple size
        Collections.sort(partitionGroups,
        new Comparator<PartitionGroup>() {
            public int compare(PartitionGroup pg1, PartitionGroup pg2) {
                Preconditions.checkState(pg1.totalOutputTupleSize > 0);
                Preconditions.checkState(pg2.totalOutputTupleSize > 0);
                int diff = pg1.totalOutputTupleSize - pg2.totalOutputTupleSize;
                return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
            }
        });

        if (nonPartitioning != null) {
            partitionGroups.add(nonPartitioning);
        }

        for (PartitionGroup pg : partitionGroups) {
            pg.orderSortGroups();
        }
    }

    /**
     * Create SortInfo, including sort tuple, to sort entire input row
     * on sortExprs.
     * @throws AnalysisException
     */
    private SortInfo createSortInfo(
        PlanNode input, List<Expr> sortExprs, List<Boolean> isAsc,
        List<Boolean> nullsFirst) throws AnalysisException {
        // create tuple for sort output = the entire materialized input in a single tuple
        TupleDescriptor sortTupleDesc =
                analyzer.getDescTbl().createTupleDescriptor("sort-tuple");
        ExprSubstitutionMap sortSmap = new ExprSubstitutionMap();
        List<Expr> sortSlotExprs = Lists.newArrayList();
        sortTupleDesc.setIsMaterialized(true);
        for (TupleId tid : input.getTupleIds()) {
            TupleDescriptor tupleDesc = analyzer.getTupleDesc(tid);
            for (SlotDescriptor inputSlotDesc : tupleDesc.getSlots()) {
                if (!inputSlotDesc.isMaterialized()) {
                    continue;
                }
                // TODO(zc)
                // SlotDescriptor sortSlotDesc =
                //         analyzer.copySlotDescriptor(inputSlotDesc, sortTupleDesc);
                SlotDescriptor sortSlotDesc = analyzer.getDescTbl().addSlotDescriptor(sortTupleDesc);
                if (inputSlotDesc.getColumn() != null) {
                    sortSlotDesc.setColumn(inputSlotDesc.getColumn());
                } else {
                    sortSlotDesc.setType(inputSlotDesc.getType());
                }
                // all output slots need to be materialized
                sortSlotDesc.setIsMaterialized(true);
                sortSlotDesc.setIsNullable(inputSlotDesc.getIsNullable());
                sortSmap.put(new SlotRef(inputSlotDesc), new SlotRef(sortSlotDesc));
                sortSlotExprs.add(new SlotRef(inputSlotDesc));
            }
        }

        // Lhs exprs to be substituted in ancestor plan nodes could have a rhs that contains
        // TupleIsNullPredicates. TupleIsNullPredicates require specific tuple ids for
        // evaluation. Since this sort materializes a new tuple, it's impossible to evaluate
        // TupleIsNullPredicates referring to this sort's input after this sort,
        // To preserve the information whether an input tuple was null or not this sort node,
        // we materialize those rhs TupleIsNullPredicates, which are then substituted
        // by a SlotRef into the sort's tuple in ancestor nodes (IMPALA-1519).
        ExprSubstitutionMap inputSmap = input.getOutputSmap();
        if (inputSmap != null) {
            List<Expr> tupleIsNullPredsToMaterialize = Lists.newArrayList();
            for (int i = 0; i < inputSmap.size(); ++i) {
                Expr rhsExpr = inputSmap.getRhs().get(i);
                // Ignore substitutions that are irrelevant at this plan node and its ancestors.
                if (!rhsExpr.isBoundByTupleIds(input.getTupleIds())) continue;
                rhsExpr.collect(TupleIsNullPredicate.class, tupleIsNullPredsToMaterialize);
            }
            Expr.removeDuplicates(tupleIsNullPredsToMaterialize);

            // Materialize relevant unique TupleIsNullPredicates.
            for (Expr tupleIsNullPred: tupleIsNullPredsToMaterialize) {
                SlotDescriptor sortSlotDesc = analyzer.addSlotDescriptor(sortTupleDesc);
                sortSlotDesc.setType(tupleIsNullPred.getType());
                sortSlotDesc.setIsMaterialized(true);
                sortSlotDesc.setSourceExpr(tupleIsNullPred);
                sortSlotDesc.setLabel(tupleIsNullPred.toSql());
                sortSlotExprs.add(tupleIsNullPred.clone());
            }
        }

        // SortInfo sortInfo = new SortInfo(Expr.trySubstituteList(sortExprs, sortSmap, analyzer, false),
        //                                  isAsc, nullsFirst);
        // LOG.trace("sortinfo exprs: " + Expr.debugString(sortInfo.getOrderingExprs()));
        // sortInfo.setMaterializedTupleInfo(sortTupleDesc, sortSlotExprs);

        SortInfo sortInfo = new SortInfo(sortExprs, isAsc, nullsFirst);
        ExprSubstitutionMap smap =
                sortInfo.createMaterializedOrderExprs(sortTupleDesc, analyzer);
        sortSlotExprs.addAll(smap.getLhs());
        sortSmap = ExprSubstitutionMap.combine(sortSmap, smap);
        sortInfo.substituteOrderingExprs(sortSmap, analyzer);
        if (LOG.isDebugEnabled()) {
            LOG.debug("sortinfo exprs: " + Expr.debugString(sortInfo.getOrderingExprs()));
        }
        sortInfo.setMaterializedTupleInfo(sortTupleDesc, sortSlotExprs);
        return sortInfo;
    }

    /**
     * Create plan tree for the entire sort group, including all contained window groups.
     * Marks the SortNode as requiring its input to be partitioned if partitionExprs
     * is not null (partitionExprs represent the data partition of the entire partition
     * group of which this sort group is a part).
     * @throws AnalysisException
     * @throws UserException
     */
    private PlanNode createSortGroupPlan(PlanNode root, SortGroup sortGroup,
                                         List<Expr> partitionExprs) throws AnalysisException, UserException {
        List<Expr> partitionByExprs = sortGroup.partitionByExprs;
        List<OrderByElement> orderByElements = sortGroup.orderByElements;
        ExprSubstitutionMap sortSmap = null;
        TupleId sortTupleId = null;
        TupleDescriptor bufferedTupleDesc = null;
        // map from input to buffered tuple
        ExprSubstitutionMap bufferedSmap = new ExprSubstitutionMap();

        PlanNode newRoot = root;
        // sort on partition by (pb) + order by (ob) exprs and create pb/ob predicates
        if (!partitionByExprs.isEmpty() || !orderByElements.isEmpty()) {
            // first sort on partitionExprs (direction doesn't matter)
            List<Expr> sortExprs = Lists.newArrayList(partitionByExprs);
            List<Boolean> isAsc =
                    Lists.newArrayList(Collections.nCopies(sortExprs.size(), Boolean.TRUE));
            // TODO: utilize a direction and nulls/first last that has benefit
            // for subsequent sort groups
            List<Boolean> nullsFirst =
                    Lists.newArrayList(Collections.nCopies(sortExprs.size(), Boolean.FALSE));

            // then sort on orderByExprs
            for (OrderByElement orderByElement : sortGroup.orderByElements) {
                sortExprs.add(orderByElement.getExpr());
                isAsc.add(orderByElement.getIsAsc());
                nullsFirst.add(orderByElement.getNullsFirstParam());
            }

            SortInfo sortInfo = createSortInfo(newRoot, sortExprs, isAsc, nullsFirst);
            SortNode sortNode = new SortNode(ctx_.getNextNodeId(), newRoot, sortInfo, false, false, 0);

            // if this sort group does not have partitioning exprs, we want the sort
            // to be executed like a regular distributed sort
            if (!partitionByExprs.isEmpty()) {
                sortNode.setIsAnalyticSort(true);
            }

            if (partitionExprs != null) {
                // create required input partition
                DataPartition inputPartition = DataPartition.UNPARTITIONED;

                if (!partitionExprs.isEmpty()) {
                    inputPartition =
                        new DataPartition(TPartitionType.HASH_PARTITIONED, partitionExprs);
                }

                sortNode.setInputPartition(inputPartition);
            }

            sortNode.init(analyzer);
            newRoot = sortNode;
            sortSmap = sortNode.getOutputSmap();

            // create bufferedTupleDesc and bufferedSmap
            sortTupleId = sortNode.tupleIds.get(0);
            bufferedTupleDesc =
                analyzer.getDescTbl().copyTupleDescriptor(sortTupleId, "buffered-tuple");
            LOG.trace("desctbl: " + analyzer.getDescTbl().debugString());

            List<SlotDescriptor> inputSlots = analyzer.getTupleDesc(sortTupleId).getSlots();
            List<SlotDescriptor> bufferedSlots = bufferedTupleDesc.getSlots();

            for (int i = 0; i < inputSlots.size(); ++i) {
                bufferedSmap.put(new SlotRef(inputSlots.get(i)), new SlotRef(bufferedSlots.get(i)));
            }
        }

        // create one AnalyticEvalNode per window group
        for (WindowGroup windowGroup : sortGroup.windowGroups) {
            // Create partition-by (pb) and order-by (ob) less-than predicates between the
            // input tuple (the output of the preceding sort) and a buffered tuple that is
            // identical to the input tuple. We need a different tuple descriptor for the
            // buffered tuple because the generated predicates should compare two different
            // tuple instances from the same input stream (i.e., the predicates should be
            // evaluated over a row that is composed of the input and the buffered tuple).

            // we need to remap the pb/ob exprs to a) the sort output, b) our buffer of the
            // sort input
            Expr partitionByEq = null;

            if (!windowGroup.partitionByExprs.isEmpty()) {
                partitionByEq = createNullMatchingEquals(
                                    Expr.trySubstituteList(windowGroup.partitionByExprs, sortSmap, analyzer, false),
                                    sortTupleId, bufferedSmap);
                LOG.trace("partitionByEq: " + partitionByEq.debugString());
            }

            Expr orderByEq = null;

            if (!windowGroup.orderByElements.isEmpty()) {
                orderByEq = createNullMatchingEquals(
                                OrderByElement.getOrderByExprs(OrderByElement.substitute(
                                            windowGroup.orderByElements, sortSmap, analyzer)),
                                sortTupleId, bufferedSmap);
                LOG.trace("orderByEq: " + orderByEq.debugString());
            }

            AnalyticEvalNode node = new AnalyticEvalNode(ctx_.getNextNodeId(), newRoot,
                    windowGroup.analyticFnCalls, windowGroup.partitionByExprs,
                    windowGroup.orderByElements,
                    windowGroup.window,
                    windowGroup.physicalIntermediateTuple,
                    windowGroup.physicalOutputTuple, windowGroup.logicalToPhysicalSmap,
                    partitionByEq, orderByEq, bufferedTupleDesc);
            node.init(analyzer);
            newRoot = node;
        }

        return newRoot;
    }

    /**
     * Create a predicate that checks if all exprs are equal or both sides are null.
     * @throws AnalysisException
     */
    private Expr createNullMatchingEquals(List<Expr> exprs, TupleId inputTid,
                                          ExprSubstitutionMap bufferedSmap) throws AnalysisException {
        Preconditions.checkState(!exprs.isEmpty());
        Expr result = createNullMatchingEqualsAux(exprs, 0, inputTid, bufferedSmap);
        result.analyze(analyzer);
        return result;
    }

    /**
     * Create an unanalyzed predicate that checks if elements >= i are equal or
     * both sides are null.
     *
     * The predicate has the form
     * ((lhs[i] is null && rhs[i] is null) || (
     *   lhs[i] is not null && rhs[i] is not null && lhs[i] = rhs[i]))
     * && <createEqualsAux(i + 1)>
     * @throws AnalysisException
     */
    private Expr createNullMatchingEqualsAux(List<Expr> elements, int i,
            TupleId inputTid, ExprSubstitutionMap bufferedSmap) throws AnalysisException {
        if (i > elements.size() - 1) {
            return new BoolLiteral(true);
        }

        // compare elements[i]
        Expr lhs = elements.get(i);
        Preconditions.checkState(lhs.isBound(inputTid));
        Expr rhs = lhs.trySubstitute(bufferedSmap, analyzer, false);

        Expr bothNull = new CompoundPredicate(Operator.AND,
                                              new IsNullPredicate(lhs, false), new IsNullPredicate(rhs, false));
        Expr lhsEqRhsNotNull = new CompoundPredicate(Operator.AND,
                new CompoundPredicate(Operator.AND,
                                      new IsNullPredicate(lhs, true), new IsNullPredicate(rhs, true)),
                new BinaryPredicate(BinaryPredicate.Operator.EQ, lhs, rhs));
        Expr remainder = createNullMatchingEqualsAux(elements, i + 1, inputTid, bufferedSmap);
        return new CompoundPredicate(CompoundPredicate.Operator.AND,
                                     new CompoundPredicate(Operator.OR, bothNull, lhsEqRhsNotNull),
                                     remainder);
    }

    /**
     * Collection of AnalyticExprs that share the same partition-by/order-by/window
     * specification. The AnalyticExprs are stored broken up into their constituent parts.
     */
    private static class WindowGroup {
        public final List<Expr> partitionByExprs;
        public final List<OrderByElement> orderByElements;
        public final AnalyticWindow window; // not null

        // Analytic exprs belonging to this window group and their corresponding logical
        // intermediate and output slots from AnalyticInfo.intermediateTupleDesc_
        // and AnalyticInfo.outputTupleDesc_.
        public final List<AnalyticExpr> analyticExprs = Lists.newArrayList();
        // Result of getFnCall() for every analytic expr.
        public final List<Expr> analyticFnCalls = Lists.newArrayList();
        public final List<SlotDescriptor> logicalOutputSlots = Lists.newArrayList();
        public final List<SlotDescriptor> logicalIntermediateSlots = Lists.newArrayList();

        // Physical output and intermediate tuples as well as an smap that maps the
        // corresponding logical output slots to their physical slots in physicalOutputTuple.
        // Set in init().
        public TupleDescriptor physicalOutputTuple;
        public TupleDescriptor physicalIntermediateTuple;
        public final ExprSubstitutionMap logicalToPhysicalSmap = new ExprSubstitutionMap();

        public WindowGroup(AnalyticExpr analyticExpr, SlotDescriptor logicalOutputSlot,
                           SlotDescriptor logicalIntermediateSlot) {
            partitionByExprs = analyticExpr.getPartitionExprs();
            orderByElements = analyticExpr.getOrderByElements();
            window = analyticExpr.getWindow();
            analyticExprs.add(analyticExpr);
            analyticFnCalls.add(analyticExpr.getFnCall());
            logicalOutputSlots.add(logicalOutputSlot);
            logicalIntermediateSlots.add(logicalIntermediateSlot);
        }

        /**
         * True if this analytic function must be evaluated in its own WindowGroup.
         */
        private static boolean requiresIndependentEval(AnalyticExpr analyticExpr) {
            return analyticExpr.getFnCall().getFnName().getFunction().equals(
                       AnalyticExpr.FIRST_VALUE_REWRITE);
        }

        /**
         * True if the partition exprs and ordering elements and the window of analyticExpr
         * match ours.
         */
        public boolean isCompatible(AnalyticExpr analyticExpr) {
            if (requiresIndependentEval(analyticExprs.get(0)) ||
                    requiresIndependentEval(analyticExpr)) {
                return false;
            }

            if (!Expr.equalSets(analyticExpr.getPartitionExprs(), partitionByExprs)) {
                return false;
            }

            if (!analyticExpr.getOrderByElements().equals(orderByElements)) {
                return false;
            }

            if ((window == null) != (analyticExpr.getWindow() == null)) {
                return false;
            }

            if (window == null) {
                return true;
            }

            return analyticExpr.getWindow().equals(window);
        }

        /**
         * Adds the given analytic expr and its logical slots to this window group.
         * Assumes the corresponding analyticExpr is compatible with 'this'.
         */
        public void add(AnalyticExpr analyticExpr, SlotDescriptor logicalOutputSlot,
                        SlotDescriptor logicalIntermediateSlot) {
            Preconditions.checkState(isCompatible(analyticExpr));
            analyticExprs.add(analyticExpr);
            analyticFnCalls.add(analyticExpr.getFnCall());
            logicalOutputSlots.add(logicalOutputSlot);
            logicalIntermediateSlots.add(logicalIntermediateSlot);
        }

        /**
         * Creates the physical output and intermediate tuples as well as the logical to
         * physical smap for this window group. Computes the mem layout for the tuple
         * descriptors.
         */
        public void init(Analyzer analyzer, String tupleName) {
            Preconditions.checkState(physicalOutputTuple == null);
            Preconditions.checkState(physicalIntermediateTuple == null);
            Preconditions.checkState(analyticFnCalls.size() == analyticExprs.size());

            //          If needed, create the intermediate tuple first to maintain
            //          intermediateTupleId < outputTupleId for debugging purposes and consistency with
            //          tuple creation for aggregations.
            boolean requiresIntermediateTuple =
                AnalyticInfo.requiresIntermediateTuple(analyticFnCalls);

            if (requiresIntermediateTuple) {
                physicalIntermediateTuple =
                    analyzer.getDescTbl().createTupleDescriptor();
                physicalOutputTuple =
                    analyzer.getDescTbl().createTupleDescriptor();
            } else {
                physicalOutputTuple =
                    analyzer.getDescTbl().createTupleDescriptor();
                physicalIntermediateTuple = physicalOutputTuple;
            }

            Preconditions.checkState(analyticExprs.size() == logicalIntermediateSlots.size());
            Preconditions.checkState(analyticExprs.size() == logicalOutputSlots.size());

            for (int i = 0; i < analyticExprs.size(); ++i) {
                SlotDescriptor logicalOutputSlot = logicalOutputSlots.get(i);

                //        SlotDescriptor physicalOutputSlot =analyzer.getDescTbl().copySlotDescriptor(logicalOutputSlot, physicalOutputTuple);
                SlotDescriptor physicalOutputSlot = analyzer.getDescTbl().copySlotDescriptor(physicalOutputTuple,
                                                    logicalOutputSlot);
                physicalOutputSlot.setIsMaterialized(true);
                //        in impala setIntermediateType only used in uda

                if (requiresIntermediateTuple) {
                    SlotDescriptor logicalIntermediateSlot = logicalIntermediateSlots.get(i);
                    SlotDescriptor physicalIntermediateSlot = analyzer.getDescTbl().copySlotDescriptor(
                                physicalIntermediateTuple, logicalIntermediateSlot);
                    physicalIntermediateSlot.setIsMaterialized(true);
                }

                logicalToPhysicalSmap.put(new SlotRef(logicalOutputSlot), new SlotRef(physicalOutputSlot));
            }

            physicalOutputTuple.computeStatAndMemLayout();
        }
    }

    /**
     * Extract a minimal set of WindowGroups from analyticExprs.
     */
    private List<WindowGroup> collectWindowGroups() {
        List<Expr> analyticExprs = analyticInfo.getAnalyticExprs();
        List<WindowGroup> groups = Lists.newArrayList();
        for (int i = 0; i < analyticExprs.size(); ++i) {
            AnalyticExpr analyticExpr = (AnalyticExpr) analyticExprs.get(i);
            // Do not generate the plan for non-materialized analytic exprs.
            if (!analyticInfo.getOutputTupleDesc().getSlots().get(i).isMaterialized()) {
                continue;
            }
            boolean match = false;
            for (WindowGroup group : groups) {
                if (group.isCompatible(analyticExpr)) {
                    group.add((AnalyticExpr) analyticInfo.getAnalyticExprs().get(i),
                              analyticInfo.getOutputTupleDesc().getSlots().get(i),
                              analyticInfo.getIntermediateTupleDesc().getSlots().get(i));
                    match = true;
                    break;
                }
            }
            if (!match) {
                groups.add(new WindowGroup(
                               (AnalyticExpr) analyticInfo.getAnalyticExprs().get(i),
                               analyticInfo.getOutputTupleDesc().getSlots().get(i),
                               analyticInfo.getIntermediateTupleDesc().getSlots().get(i)));
            }
        }

        return groups;
    }

    /**
     * Collection of WindowGroups that share the same partition-by/order-by
     * specification.
     */
    private static class SortGroup {
        public List<Expr> partitionByExprs;
        public List<OrderByElement> orderByElements;
        public List<WindowGroup> windowGroups = Lists.newArrayList();

        // sum of windowGroups.physicalOutputTuple.getByteSize()
        public int totalOutputTupleSize = -1;

        public SortGroup(WindowGroup windowGroup) {
            partitionByExprs = windowGroup.partitionByExprs;
            orderByElements = windowGroup.orderByElements;
            windowGroups.add(windowGroup);
        }

        /**
         * True if the partition and ordering exprs of windowGroup match ours.
         */
        public boolean isCompatible(WindowGroup windowGroup) {
            return Expr.equalSets(windowGroup.partitionByExprs, partitionByExprs)
                   && windowGroup.orderByElements.equals(orderByElements);
        }

        public void add(WindowGroup windowGroup) {
            Preconditions.checkState(isCompatible(windowGroup));
            windowGroups.add(windowGroup);
        }

        /**
         * Return true if 'this' and other have compatible partition exprs and
         * our orderByElements are a prefix of other's.
         */
        public boolean isPrefixOf(SortGroup other) {
            if (other.orderByElements.size() > orderByElements.size()) {
                return false;
            }

            if (!Expr.equalSets(partitionByExprs, other.partitionByExprs)) {
                return false;
            }

            for (int i = 0; i < other.orderByElements.size(); ++i) {
                OrderByElement ob = orderByElements.get(i);
                OrderByElement otherOb = other.orderByElements.get(i);

                // TODO: compare equiv classes by comparing each equiv class's placeholder
                // slotref
                if (!ob.getExpr().equals(otherOb.getExpr())) {
                    return false;
                }

                if (ob.getIsAsc() != otherOb.getIsAsc()) {
                    return false;
                }

                //        if (ob.nullsFirst() != otherOb.nullsFirst()) return false;
            }

            return true;
        }

        /**
         * Adds other's window groups to ours, assuming that we're a prefix of other.
         */
        public void absorb(SortGroup other) {
            Preconditions.checkState(isPrefixOf(other));
            windowGroups.addAll(other.windowGroups);
        }

        /**
         * Compute totalOutputTupleSize.
         */
        public void init() {
            totalOutputTupleSize = 0;

            for (WindowGroup g : windowGroups) {
                TupleDescriptor outputTuple = g.physicalOutputTuple;
                Preconditions.checkState(outputTuple.getIsMaterialized());
                Preconditions.checkState(outputTuple.getByteSize() != -1);
                totalOutputTupleSize += outputTuple.getByteSize();
            }
        }

        private static class SizeLt implements Comparator<WindowGroup> {
            public int compare(WindowGroup wg1, WindowGroup wg2) {
                Preconditions.checkState(wg1.physicalOutputTuple != null
                                         && wg1.physicalOutputTuple.getByteSize() != -1);
                Preconditions.checkState(wg2.physicalOutputTuple != null
                                         && wg2.physicalOutputTuple.getByteSize() != -1);
                int diff = wg1.physicalOutputTuple.getByteSize()
                           - wg2.physicalOutputTuple.getByteSize();
                return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
            }
        }

        private static final SizeLt SIZE_LT;
        static {
            SIZE_LT = new SizeLt();
        }

        /**
         * Order window groups by increasing size of the output tuple. This minimizes
         * the total volume of data that needs to be buffered.
         */
        public void orderWindowGroups() {
            Collections.sort(windowGroups, SIZE_LT);
        }
    }

    /**
     * Partitions the windowGroups into SortGroups based on compatible order by exprs.
     */
    private List<SortGroup> collectSortGroups(List<WindowGroup> windowGroups) {
        List<SortGroup> sortGroups = Lists.newArrayList();

        for (WindowGroup windowGroup : windowGroups) {
            boolean match = false;

            for (SortGroup sortGroup : sortGroups) {
                if (sortGroup.isCompatible(windowGroup)) {
                    sortGroup.add(windowGroup);
                    match = true;
                    break;
                }
            }

            if (!match) {
                sortGroups.add(new SortGroup(windowGroup));
            }
        }

        return sortGroups;
    }

    /**
     * Collection of SortGroups that have compatible partition-by specifications.
     */
    private static class PartitionGroup {
        public List<Expr> partitionByExprs;
        public List<SortGroup> sortGroups = Lists.newArrayList();

        // sum of sortGroups.windowGroups.physicalOutputTuple.getByteSize()
        public int totalOutputTupleSize = -1;

        public PartitionGroup(SortGroup sortGroup) {
            partitionByExprs = sortGroup.partitionByExprs;
            sortGroups.add(sortGroup);
            totalOutputTupleSize = sortGroup.totalOutputTupleSize;
        }

        /**
         * True if the partition exprs of sortGroup are compatible with ours.
         * For now that means equality.
         */
        public boolean isCompatible(SortGroup sortGroup) {
            return Expr.equalSets(sortGroup.partitionByExprs, partitionByExprs);
        }

        public void add(SortGroup sortGroup) {
            Preconditions.checkState(isCompatible(sortGroup));
            sortGroups.add(sortGroup);
            totalOutputTupleSize += sortGroup.totalOutputTupleSize;
        }

        /**
         * Merge 'other' into 'this'
         * - partitionByExprs is the intersection of the two
         * - sortGroups becomes the union
         */
        public void merge(PartitionGroup other) {
            partitionByExprs = Expr.intersect(partitionByExprs, other.partitionByExprs);
            Preconditions.checkState(Expr.getNumDistinctValues(partitionByExprs) >= 0);
            sortGroups.addAll(other.sortGroups);
        }

        /**
         * Order sort groups by increasing totalOutputTupleSize. This minimizes the total
         * volume of data that needs to be sorted.
         */
        public void orderSortGroups() {
            Collections.sort(sortGroups,
            new Comparator<SortGroup>() {
                public int compare(SortGroup sg1, SortGroup sg2) {
                    Preconditions.checkState(sg1.totalOutputTupleSize > 0);
                    Preconditions.checkState(sg2.totalOutputTupleSize > 0);
                    int diff = sg1.totalOutputTupleSize - sg2.totalOutputTupleSize;
                    return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
                }
            });

            for (SortGroup sortGroup : sortGroups) {
                sortGroup.orderWindowGroups();
            }
        }
    }

    /**
     * Extract a minimal set of PartitionGroups from sortGroups.
     */
    private List<PartitionGroup> collectPartitionGroups(List<SortGroup> sortGroups) {
        List<PartitionGroup> partitionGroups = Lists.newArrayList();

        for (SortGroup sortGroup : sortGroups) {
            boolean match = false;

            for (PartitionGroup partitionGroup : partitionGroups) {
                if (partitionGroup.isCompatible(sortGroup)) {
                    partitionGroup.add(sortGroup);
                    match = true;
                    break;
                }
            }

            if (!match) {
                partitionGroups.add(new PartitionGroup(sortGroup));
            }
        }

        return partitionGroups;
    }
}
