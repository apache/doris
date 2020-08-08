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

import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.thrift.TPartitionType;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates all the information needed to compute the aggregate functions of a single
 * Select block, including a possible 2nd phase aggregation step for DISTINCT aggregate
 * functions and merge aggregation steps needed for distributed execution.
 *
 * The latter requires a tree structure of AggregateInfo objects which express the
 * original aggregate computations as well as the necessary merging aggregate
 * computations.
 * TODO: get rid of this by transforming
 *   SELECT COUNT(DISTINCT a, b, ..) GROUP BY x, y, ...
 * into an equivalent query with a inline view:
 *   SELECT COUNT(*) FROM (SELECT DISTINCT a, b, ..., x, y, ...) GROUP BY x, y, ...
 *
 * The tree structure looks as follows:
 * - for non-distinct aggregation:
 *   - aggInfo: contains the original aggregation functions and grouping exprs
 *   - aggInfo.mergeAggInfo: contains the merging aggregation functions (grouping
 *     exprs are identical)
 * - for distinct aggregation (for an explanation of the phases, see
 *   SelectStmt.createDistinctAggInfo()):
 *   - aggInfo: contains the phase 1 aggregate functions and grouping exprs
 *   - aggInfo.2ndPhaseDistinctAggInfo: contains the phase 2 aggregate functions and
 *     grouping exprs
 *   - aggInfo.mergeAggInfo: contains the merging aggregate functions for the phase 1
 *     computation (grouping exprs are identical)
 *   - aggInfo.2ndPhaseDistinctAggInfo.mergeAggInfo: contains the merging aggregate
 *     functions for the phase 2 computation (grouping exprs are identical)
 *
 * In general, merging aggregate computations are idempotent; in other words,
 * aggInfo.mergeAggInfo == aggInfo.mergeAggInfo.mergeAggInfo.
 *
 * TODO: move the merge construction logic from SelectStmt into AggregateInfo
 * TODO: Add query tests for aggregation with intermediate tuples with num_nodes=1.
 */
public final class AggregateInfo extends AggregateInfoBase {
    private final static Logger LOG = LogManager.getLogger(AggregateInfo.class);

    public enum AggPhase {
        FIRST,
        FIRST_MERGE,
        SECOND,
        SECOND_MERGE;

        public boolean isMerge() { return this == FIRST_MERGE || this == SECOND_MERGE; }
    };

    // created by createMergeAggInfo()
    private AggregateInfo mergeAggInfo_;

    // created by createDistinctAggInfo()
    private AggregateInfo secondPhaseDistinctAggInfo_;

    private final AggPhase aggPhase_;

    // Map from all grouping and aggregate exprs to a SlotRef referencing the corresp. slot
    // in the intermediate tuple. Identical to outputTupleSmap_ if no aggregateExpr has an
    // output type that is different from its intermediate type.
    protected ExprSubstitutionMap intermediateTupleSmap_ = new ExprSubstitutionMap();

    // Map from all grouping and aggregate exprs to a SlotRef referencing the corresp. slot
    // in the output tuple.
    protected ExprSubstitutionMap outputTupleSmap_ = new ExprSubstitutionMap();

    // Map from slots of outputTupleSmap_ to the corresponding slot in
    // intermediateTupleSmap_.
    protected ExprSubstitutionMap outputToIntermediateTupleSmap_ =
            new ExprSubstitutionMap();

    // if set, a subset of groupingExprs_; set and used during planning
    private List<Expr> partitionExprs_;

    // indices into aggregateExprs for those that need to be materialized;
    // shared between this, mergeAggInfo and secondPhaseDistinctAggInfo
    private ArrayList<Integer> materializedAggregateSlots_ = Lists.newArrayList();
    // if true, this AggregateInfo is the first phase of a 2-phase DISTINCT computation
    private boolean isDistinctAgg = false;
    // If true, the sql has MultiDistinct
    private boolean isMultiDistinct_;

    // the multi distinct's begin pos  and end pos in groupby exprs
    private ArrayList<Integer> firstIdx_ = Lists.newArrayList();
    private ArrayList<Integer> lastIdx_ = Lists.newArrayList();

    // C'tor creates copies of groupingExprs and aggExprs.
    private AggregateInfo(ArrayList<Expr> groupingExprs,
                          ArrayList<FunctionCallExpr> aggExprs, AggPhase aggPhase)  {
        this(groupingExprs, aggExprs, aggPhase, false);
    }

    private AggregateInfo(ArrayList<Expr> groupingExprs,
                          ArrayList<FunctionCallExpr> aggExprs, AggPhase aggPhase, boolean isMultiDistinct)  {
        super(groupingExprs, aggExprs);
        aggPhase_ = aggPhase;
        isMultiDistinct_ = isMultiDistinct;
    }

    /**
     * C'tor for cloning.
     */
    private AggregateInfo(AggregateInfo other) {
        super(other);
        if (other.mergeAggInfo_ != null) {
            mergeAggInfo_ = other.mergeAggInfo_.clone();
        }
        if (other.secondPhaseDistinctAggInfo_ != null) {
            secondPhaseDistinctAggInfo_ = other.secondPhaseDistinctAggInfo_.clone();
        }
        aggPhase_ = other.aggPhase_;
        outputTupleSmap_ = other.outputTupleSmap_.clone();
        if (other.requiresIntermediateTuple()) {
            intermediateTupleSmap_ = other.intermediateTupleSmap_.clone();
        } else {
            Preconditions.checkState(other.intermediateTupleDesc_ == other.outputTupleDesc_);
            intermediateTupleSmap_ = outputTupleSmap_;
        }
        partitionExprs_ =
                (other.partitionExprs_ != null) ? Expr.cloneList(other.partitionExprs_) : null;
    }

    public List<Expr> getPartitionExprs() { return partitionExprs_; }
    public void setPartitionExprs(List<Expr> exprs) { partitionExprs_ = exprs; }
    
    /**
     * Creates complete AggregateInfo for groupingExprs and aggExprs, including
     * aggTupleDesc and aggTupleSMap. If parameter tupleDesc != null, sets aggTupleDesc to
     * that instead of creating a new descriptor (after verifying that the passed-in
     * descriptor is correct for the given aggregation).
     * Also creates mergeAggInfo and secondPhaseDistinctAggInfo, if needed.
     * If an aggTupleDesc is created, also registers eq predicates between the
     * grouping exprs and their respective slots with 'analyzer'.
     */
    static public AggregateInfo create(
            ArrayList<Expr> groupingExprs, ArrayList<FunctionCallExpr> aggExprs,
            TupleDescriptor tupleDesc, Analyzer analyzer)
            throws AnalysisException {
        Preconditions.checkState(
                (groupingExprs != null && !groupingExprs.isEmpty())
                        || (aggExprs != null && !aggExprs.isEmpty()));
        AggregateInfo result = new AggregateInfo(groupingExprs, aggExprs, AggPhase.FIRST);

        // collect agg exprs with DISTINCT clause
        ArrayList<FunctionCallExpr> distinctAggExprs = Lists.newArrayList();
        if (aggExprs != null) {
            for (FunctionCallExpr aggExpr : aggExprs) {
                if (aggExpr.isDistinct()) {
                    distinctAggExprs.add(aggExpr);
                }
            }
        }

        // aggregation algorithm includes two kinds:one stage aggregation, tow stage aggregation.
        // for case:
        // 1: if aggExprs don't hava distinct or hava multi distinct , create aggregate info for
        // one stage aggregation. 
        // 2: if aggExprs hava one distinct , create aggregate info for two stage aggregation
        boolean isMultiDistinct = result.estimateIfContainsMultiDistinct(distinctAggExprs);
        if (distinctAggExprs.isEmpty() 
               || isMultiDistinct) {
            // It is used to map new aggr expr to old expr to help create an external 
            // reference to the aggregation node tuple
            result.setIsMultiDistinct(isMultiDistinct);
            if (tupleDesc == null) {
                result.createTupleDescs(analyzer);
                result.createSmaps(analyzer);
            } else {
                // A tupleDesc should only be given for UNION DISTINCT.
                Preconditions.checkState(aggExprs == null);
                result.outputTupleDesc_ = tupleDesc;
                result.intermediateTupleDesc_ = tupleDesc;
            }
            result.createMergeAggInfo(analyzer);
        } else {
            // case 2:
            // we don't allow you to pass in a descriptor for distinct aggregation
            // (we need two descriptors)
            Preconditions.checkState(tupleDesc == null);
            result.createDistinctAggInfo(groupingExprs, distinctAggExprs, analyzer);
        }
        if (LOG.isDebugEnabled())  LOG.debug("agg info:\n{}", result.debugString());
        return result;
    }


    /**
     * estimate if functions contains multi distinct
     * @param distinctAggExprs
     * @return
     */
    public static boolean estimateIfContainsMultiDistinct(List<FunctionCallExpr> distinctAggExprs) 
      throws AnalysisException {
        
        if (distinctAggExprs == null || distinctAggExprs.size() <= 0) {
            return false;
        }

        ArrayList<Expr> expr0Children = Lists.newArrayList();
        if (distinctAggExprs.get(0).getFnName().getFunction().equalsIgnoreCase("group_concat")) {
            // Ignore separator parameter, otherwise the same would have to be present for all
            // other distinct aggregates as well.
            // TODO: Deal with constant exprs more generally, instead of special-casing
            // group_concat().
            expr0Children.add(distinctAggExprs.get(0).getChild(0).ignoreImplicitCast());
        } else {
            for (Expr expr : distinctAggExprs.get(0).getChildren()) {
                expr0Children.add(expr.ignoreImplicitCast());
            }
        }
        boolean hasMultiDistinct = false;
        for (int i = 1; i < distinctAggExprs.size(); ++i) {
            ArrayList<Expr> exprIChildren = Lists.newArrayList();
            if (distinctAggExprs.get(i).getFnName().getFunction().equalsIgnoreCase("group_concat")) {
                exprIChildren.add(distinctAggExprs.get(i).getChild(0).ignoreImplicitCast());
            } else {
                for (Expr expr : distinctAggExprs.get(i).getChildren()) {
                    exprIChildren.add(expr.ignoreImplicitCast());
                }
            }
            if (!Expr.equalLists(expr0Children, exprIChildren)) {
                if (exprIChildren.size() > 1 || expr0Children.size() > 1) {
                    throw new AnalysisException("The query contains multi count distinct or "
                            + "sum distinct, each can't have multi columns.");   
                }
                hasMultiDistinct = true;
            }
        }
        return hasMultiDistinct;
    }

    /**
     * Create aggregate info for select block containing aggregate exprs with
     * DISTINCT clause.
     * This creates:
     * - aggTupleDesc
     * - a complete secondPhaseDistinctAggInfo
     * - mergeAggInfo
     *
     * At the moment, we require that all distinct aggregate
     * functions be applied to the same set of exprs (ie, we can't do something
     * like SELECT COUNT(DISTINCT id), COUNT(DISTINCT address)).
     * Aggregation happens in two successive phases:
     * - the first phase aggregates by all grouping exprs plus all parameter exprs
     *   of DISTINCT aggregate functions
     *
     * Example:
     *   SELECT a, COUNT(DISTINCT b, c), MIN(d), COUNT(*) FROM T GROUP BY a
     * - 1st phase grouping exprs: a, b, c
     * - 1st phase agg exprs: MIN(d), COUNT(*)
     * - 2nd phase grouping exprs: a
     * - 2nd phase agg exprs: COUNT(*), MIN(<MIN(d) from 1st phase>),
     *     SUM(<COUNT(*) from 1st phase>)
     *
     * TODO: expand implementation to cover the general case; this will require
     * a different execution strategy
     */
    private void createDistinctAggInfo(
            ArrayList<Expr> origGroupingExprs,
            ArrayList<FunctionCallExpr> distinctAggExprs, Analyzer analyzer)
            throws AnalysisException {
        Preconditions.checkState(!distinctAggExprs.isEmpty());
        // make sure that all DISTINCT params are the same;
        // ignore top-level implicit casts in the comparison, we might have inserted
        // those during analysis
        ArrayList<Expr> expr0Children = Lists.newArrayList();
        if (distinctAggExprs.get(0).getFnName().getFunction().equalsIgnoreCase("group_concat")) {
            // Ignore separator parameter, otherwise the same would have to be present for all
            // other distinct aggregates as well.
            // TODO: Deal with constant exprs more generally, instead of special-casing
            // group_concat().
            expr0Children.add(distinctAggExprs.get(0).getChild(0).ignoreImplicitCast());
        } else {
            for (Expr expr : distinctAggExprs.get(0).getChildren()) {
                expr0Children.add(expr.ignoreImplicitCast());
            }
        }

        this.isMultiDistinct_= estimateIfContainsMultiDistinct(distinctAggExprs);
        isDistinctAgg = true;

        // add DISTINCT parameters to grouping exprs
        if (!isMultiDistinct_) {
            groupingExprs_.addAll(expr0Children);
        } 

        // remove DISTINCT aggregate functions from aggExprs
        aggregateExprs_.removeAll(distinctAggExprs);

        createTupleDescs(analyzer);
        createSmaps(analyzer);
        createMergeAggInfo(analyzer);
        createSecondPhaseAggInfo(origGroupingExprs, distinctAggExprs, analyzer);
    }

    public ArrayList<FunctionCallExpr> getMaterializedAggregateExprs() {
        ArrayList<FunctionCallExpr> result = Lists.newArrayList();
        for (Integer i: materializedSlots_) {
          result.add(aggregateExprs_.get(i));
        }
        return result;
    }

    public AggregateInfo getMergeAggInfo() {
        return mergeAggInfo_;
    }

    public boolean isMerge() { return aggPhase_.isMerge(); }
    public boolean isDistinctAgg() { return secondPhaseDistinctAggInfo_ != null; }
    public ExprSubstitutionMap getIntermediateSmap() { return intermediateTupleSmap_; }
    public ExprSubstitutionMap getOutputSmap() { return outputTupleSmap_; }
    public ExprSubstitutionMap getOutputToIntermediateSmap() {
        return outputToIntermediateTupleSmap_;
    }

    public boolean hasAggregateExprs() {
        return !aggregateExprs_.isEmpty() ||
                (secondPhaseDistinctAggInfo_ != null &&
                        !secondPhaseDistinctAggInfo_.getAggregateExprs().isEmpty());
    }

    public void setIsMultiDistinct(boolean value) {
        this.isMultiDistinct_ = value;
    }

    public boolean isMultiDistinct() {
        return isMultiDistinct_;
    }

    public AggregateInfo getSecondPhaseDistinctAggInfo() {
        return secondPhaseDistinctAggInfo_;
    }

    /**
     * Return the tuple id produced in the final aggregation step.
     */
    public TupleId getResultTupleId() {
        if (isDistinctAgg()) return secondPhaseDistinctAggInfo_.getOutputTupleId();
        return getOutputTupleId();
    }

    /**
     * Append ids of all slots that are being referenced in the process
     * of performing the aggregate computation described by this AggregateInfo.
     */
    public void getRefdSlots(List<SlotId> ids) {
        Preconditions.checkState(outputTupleDesc_ != null);
        if (groupingExprs_ != null) {
            Expr.getIds(groupingExprs_, null, ids);
        }
        Expr.getIds(aggregateExprs_, null, ids);
        // The backend assumes that the entire aggTupleDesc is materialized
        for (int i = 0; i < outputTupleDesc_.getSlots().size(); ++i) {
            ids.add(outputTupleDesc_.getSlots().get(i).getId());
        }
    }

    /**
     * Substitute all the expressions (grouping expr, aggregate expr) and update our
     * substitution map according to the given substitution map:
     * - smap typically maps from tuple t1 to tuple t2 (example: the smap of an
     *   inline view maps the virtual table ref t1 into a base table ref t2)
     * - our grouping and aggregate exprs need to be substituted with the given
     *   smap so that they also reference t2
     * - aggTupleSMap needs to be recomputed to map exprs based on t2
     *   onto our aggTupleDesc (ie, the left-hand side needs to be substituted with
     *   smap)
     * - mergeAggInfo: this is not affected, because
     *   * its grouping and aggregate exprs only reference aggTupleDesc_
     *   * its smap is identical to aggTupleSMap_
     * - 2ndPhaseDistinctAggInfo:
     *   * its grouping and aggregate exprs also only reference aggTupleDesc_
     *     and are therefore not affected
     *   * its smap needs to be recomputed to map exprs based on t2 to its own
     *     aggTupleDesc
     */
    public void substitute(ExprSubstitutionMap smap, Analyzer analyzer) {
        groupingExprs_ = Expr.substituteList(groupingExprs_, smap, analyzer, true);
        if (LOG.isTraceEnabled()) {
            LOG.trace("AggInfo: grouping_exprs=" + Expr.debugString(groupingExprs_));
        }

        // The smap in this case should not substitute the aggs themselves, only
        // their subexpressions.
        List<Expr> substitutedAggs =
            Expr.substituteList(aggregateExprs_, smap, analyzer, false);
        aggregateExprs_.clear();
        for (Expr substitutedAgg: substitutedAggs) {
            aggregateExprs_.add((FunctionCallExpr) substitutedAgg);
        }

        outputTupleSmap_.substituteLhs(smap, analyzer);
        intermediateTupleSmap_.substituteLhs(smap, analyzer);
        if (secondPhaseDistinctAggInfo_ != null) {
            secondPhaseDistinctAggInfo_.substitute(smap, analyzer);
        }
    }

    /**
     * Create the info for an aggregation node that merges its pre-aggregated inputs:
     * - pre-aggregation is computed by 'this'
     * - tuple desc and smap are the same as that of the input (we're materializing
     *   the same logical tuple)
     * - grouping exprs: slotrefs to the input's grouping slots
     * - aggregate exprs: aggregation of the input's aggregateExprs slots
     *
     * The returned AggregateInfo shares its descriptor and smap with the input info;
     * createAggTupleDesc() must not be called on it.
     */
    private void createMergeAggInfo(Analyzer analyzer)  {
        Preconditions.checkState(mergeAggInfo_ == null);
        TupleDescriptor inputDesc = intermediateTupleDesc_;
        // construct grouping exprs
        ArrayList<Expr> groupingExprs = Lists.newArrayList();
        for (int i = 0; i < getGroupingExprs().size(); ++i) {
            groupingExprs.add(new SlotRef(inputDesc.getSlots().get(i)));
        }

        // construct agg exprs
        ArrayList<FunctionCallExpr> aggExprs = Lists.newArrayList();
        for (int i = 0; i < getAggregateExprs().size(); ++i) {
            FunctionCallExpr inputExpr = getAggregateExprs().get(i);
            Preconditions.checkState(inputExpr.isAggregateFunction());
            Expr aggExprParam =
                    new SlotRef(inputDesc.getSlots().get(i + getGroupingExprs().size()));
            FunctionCallExpr aggExpr = FunctionCallExpr.createMergeAggCall(
                    inputExpr, Lists.newArrayList(aggExprParam));
            aggExpr.analyzeNoThrow(analyzer);
            aggExprs.add(aggExpr);
        }

        AggPhase aggPhase =
                (aggPhase_ == AggPhase.FIRST) ? AggPhase.FIRST_MERGE : AggPhase.SECOND_MERGE;
        mergeAggInfo_ = new AggregateInfo(groupingExprs, aggExprs, aggPhase, isMultiDistinct_);
        mergeAggInfo_.intermediateTupleDesc_ = intermediateTupleDesc_;
        mergeAggInfo_.outputTupleDesc_ = outputTupleDesc_;
        mergeAggInfo_.intermediateTupleSmap_ = intermediateTupleSmap_;
        mergeAggInfo_.outputTupleSmap_ = outputTupleSmap_;
        mergeAggInfo_.materializedSlots_ = materializedSlots_;
    }

    /**
     * Creates an IF function call that returns NULL if any of the slots
     * at indexes [firstIdx, lastIdx] return NULL.
     * For example, the resulting IF function would like this for 3 slots:
     * IF(IsNull(slot1), NULL, IF(IsNull(slot2), NULL, slot3))
     * Returns null if firstIdx is greater than lastIdx.
     * Returns a SlotRef to the last slot if there is only one slot in range.
     */
    private Expr createCountDistinctAggExprParam(int firstIdx, int lastIdx,
            ArrayList<SlotDescriptor> slots) {
        if (firstIdx > lastIdx) {
            return null;
        }

        Expr elseExpr = new SlotRef(slots.get(lastIdx));
        if (firstIdx == lastIdx) {
            return elseExpr;
        }

        for (int i = lastIdx - 1; i >= firstIdx; --i) {
            ArrayList<Expr> ifArgs = Lists.newArrayList();
            SlotRef slotRef = new SlotRef(slots.get(i));
            // Build expr: IF(IsNull(slotRef), NULL, elseExpr)
            Expr isNullPred = new IsNullPredicate(slotRef, false);
            ifArgs.add(isNullPred);
            ifArgs.add(new NullLiteral());
            ifArgs.add(elseExpr);
            elseExpr = new FunctionCallExpr("if", ifArgs);
        }
        return elseExpr;
    }

    /**
     * Create the info for an aggregation node that computes the second phase of
     * DISTINCT aggregate functions.
     * (Refer to createDistinctAggInfo() for an explanation of the phases.)
     * - 'this' is the phase 1 aggregation
     * - grouping exprs are those of the original query (param origGroupingExprs)
     * - aggregate exprs for the DISTINCT agg fns: these are aggregating the grouping
     *   slots that were added to the original grouping slots in phase 1;
     *   count is mapped to count(*) and sum is mapped to sum
     * - other aggregate exprs: same as the non-DISTINCT merge case
     *   (count is mapped to sum, everything else stays the same)
     *
     * This call also creates the tuple descriptor and smap for the returned AggregateInfo.
     */
    private void createSecondPhaseAggInfo(
            ArrayList<Expr> origGroupingExprs,
            ArrayList<FunctionCallExpr> distinctAggExprs, Analyzer analyzer) throws AnalysisException {
        Preconditions.checkState(secondPhaseDistinctAggInfo_ == null);
        Preconditions.checkState(!distinctAggExprs.isEmpty());

        // The output of the 1st phase agg is the 1st phase intermediate.
        TupleDescriptor inputDesc = intermediateTupleDesc_;

        // construct agg exprs for original DISTINCT aggregate functions
        // (these aren't part of this.aggExprs)
        ArrayList<FunctionCallExpr> secondPhaseAggExprs = Lists.newArrayList();
        int distinctExprPos = 0;
        for (FunctionCallExpr inputExpr : distinctAggExprs) {
            Preconditions.checkState(inputExpr.isAggregateFunction());
            FunctionCallExpr aggExpr = null;
            if (!isMultiDistinct_) {
                if (inputExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.COUNT)) {
                    // COUNT(DISTINCT ...) ->
                    // COUNT(IF(IsNull(<agg slot 1>), NULL, IF(IsNull(<agg slot 2>), NULL, ...)))
                    // We need the nested IF to make sure that we do not count
                    // column-value combinations if any of the distinct columns are NULL.
                    // This behavior is consistent with MySQL.
                    Expr ifExpr = createCountDistinctAggExprParam(origGroupingExprs.size(),
                            origGroupingExprs.size() + inputExpr.getChildren().size() - 1,
                            inputDesc.getSlots());
                    Preconditions.checkNotNull(ifExpr);
                    ifExpr.analyzeNoThrow(analyzer);
                    aggExpr = new FunctionCallExpr(FunctionSet.COUNT, Lists.newArrayList(ifExpr));
                } else if (inputExpr.getFnName().getFunction().equals("group_concat")) {
                    // Syntax: GROUP_CONCAT([DISTINCT] expression [, separator])
                    ArrayList<Expr> exprList = Lists.newArrayList();
                    // Add "expression" parameter. Need to get it from the inputDesc's slots so the
                    // tuple reference is correct.
                    exprList.add(new SlotRef(inputDesc.getSlots().get(origGroupingExprs.size())));
                    // Check if user provided a custom separator
                    if (inputExpr.getChildren().size() == 2) exprList.add(inputExpr.getChild(1));
                    aggExpr = new FunctionCallExpr(inputExpr.getFnName(), exprList);
                } else {
                    // SUM(DISTINCT <expr>) -> SUM(<last grouping slot>);
                    // (MIN(DISTINCT ...) and MAX(DISTINCT ...) have their DISTINCT turned
                    // off during analysis, and AVG() is changed to SUM()/COUNT())
                    Expr aggExprParam = new SlotRef(inputDesc.getSlots().get(origGroupingExprs.size()));
                    aggExpr = new FunctionCallExpr(inputExpr.getFnName(), Lists.newArrayList(aggExprParam));
                }
            } else {
                // multi distinct can't run here    
                Preconditions.checkState(false);
            }
            secondPhaseAggExprs.add(aggExpr);
        }

        // map all the remaining agg fns
        for (int i = 0; i < aggregateExprs_.size(); ++i) {
            FunctionCallExpr inputExpr = aggregateExprs_.get(i);
            Preconditions.checkState(inputExpr.isAggregateFunction());
            // we're aggregating an output slot of the 1st agg phase
            Expr aggExprParam =
                    new SlotRef(inputDesc.getSlots().get(i + getGroupingExprs().size()));
            FunctionCallExpr aggExpr = FunctionCallExpr.createMergeAggCall(
                    inputExpr, Lists.newArrayList(aggExprParam));
            secondPhaseAggExprs.add(aggExpr);
        }
        Preconditions.checkState(
                secondPhaseAggExprs.size() == aggregateExprs_.size() + distinctAggExprs.size());

        for (FunctionCallExpr aggExpr : secondPhaseAggExprs) {
            aggExpr.analyzeNoThrow(analyzer);
            Preconditions.checkState(aggExpr.isAggregateFunction());
        }

        ArrayList<Expr> substGroupingExprs =
                Expr.substituteList(origGroupingExprs, intermediateTupleSmap_, analyzer, false);
        secondPhaseDistinctAggInfo_ =
                new AggregateInfo(substGroupingExprs, secondPhaseAggExprs, AggPhase.SECOND, isMultiDistinct_);
        secondPhaseDistinctAggInfo_.createTupleDescs(analyzer);
        secondPhaseDistinctAggInfo_.createSecondPhaseAggSMap(this, distinctAggExprs);
        secondPhaseDistinctAggInfo_.createMergeAggInfo(analyzer);
    }

    /**
     * Create smap to map original grouping and aggregate exprs onto output
     * of secondPhaseDistinctAggInfo.
     */
    private void createSecondPhaseAggSMap(
            AggregateInfo inputAggInfo, ArrayList<FunctionCallExpr> distinctAggExprs) {
        outputTupleSmap_.clear();
        int slotIdx = 0;
        ArrayList<SlotDescriptor> slotDescs = outputTupleDesc_.getSlots();

        int numDistinctParams = 0;
        if (!isMultiDistinct_) {
            numDistinctParams = distinctAggExprs.get(0).getChildren().size();
            // If we are counting distinct params of group_concat, we cannot include the custom
            // separator since it is not a distinct param.
            if (distinctAggExprs.get(0).getFnName().getFunction().equalsIgnoreCase("group_concat")
                    && numDistinctParams == 2) {
                --numDistinctParams;
            }
        } else {
            for (int i = 0; i < distinctAggExprs.size(); i++) {
                numDistinctParams += distinctAggExprs.get(i).getChildren().size();
            }
        }

        int numOrigGroupingExprs = inputAggInfo.getGroupingExprs().size() - numDistinctParams;
        Preconditions.checkState(
                slotDescs.size() == numOrigGroupingExprs + distinctAggExprs.size() +
                        inputAggInfo.getAggregateExprs().size());

        // original grouping exprs -> first m slots
        for (int i = 0; i < numOrigGroupingExprs; ++i, ++slotIdx) {
            Expr groupingExpr = inputAggInfo.getGroupingExprs().get(i);
            outputTupleSmap_.put(
                    groupingExpr.clone(), new SlotRef(slotDescs.get(slotIdx)));
        }

        // distinct agg exprs -> next n slots
        for (int i = 0; i < distinctAggExprs.size(); ++i, ++slotIdx) {
            Expr aggExpr = distinctAggExprs.get(i);
            outputTupleSmap_.put(aggExpr.clone(), (new SlotRef(slotDescs.get(slotIdx))));
        }

        // remaining agg exprs -> remaining slots
        for (int i = 0; i < inputAggInfo.getAggregateExprs().size(); ++i, ++slotIdx) {
            Expr aggExpr = inputAggInfo.getAggregateExprs().get(i);
            outputTupleSmap_.put(aggExpr.clone(), new SlotRef(slotDescs.get(slotIdx)));
        }
    }

    /**
     * Populates the output and intermediate smaps based on the output and intermediate
     * tuples that are assumed to be set. If an intermediate tuple is required, also
     * populates the output-to-intermediate smap and registers auxiliary equivalence
     * predicates between the grouping slots of the two tuples.
     */
    public void createSmaps(Analyzer analyzer) {
        Preconditions.checkNotNull(outputTupleDesc_);
        Preconditions.checkNotNull(intermediateTupleDesc_);

        List<Expr> exprs = Lists.newArrayListWithCapacity(
                groupingExprs_.size() + aggregateExprs_.size());
        exprs.addAll(groupingExprs_);
        exprs.addAll(aggregateExprs_);
        for (int i = 0; i < exprs.size(); ++i) {
            Expr expr = exprs.get(i);
            outputTupleSmap_.put(expr.clone(),
                    new SlotRef(outputTupleDesc_.getSlots().get(i)));
            if (!requiresIntermediateTuple()) continue;

            intermediateTupleSmap_.put(expr.clone(),
                    new SlotRef(intermediateTupleDesc_.getSlots().get(i)));
            outputToIntermediateTupleSmap_.put(
                    new SlotRef(outputTupleDesc_.getSlots().get(i)),
                    new SlotRef(intermediateTupleDesc_.getSlots().get(i)));
            if (i < groupingExprs_.size()) {
                analyzer.createAuxEquivPredicate(
                        new SlotRef(outputTupleDesc_.getSlots().get(i)),
                        new SlotRef(intermediateTupleDesc_.getSlots().get(i)));
            }
        }
        if (!requiresIntermediateTuple()) intermediateTupleSmap_ = outputTupleSmap_;

        if (LOG.isTraceEnabled()) {
            LOG.trace("output smap=" + outputTupleSmap_.debugString());
            LOG.trace("intermediate smap=" + intermediateTupleSmap_.debugString());
        }
    }

    /**
     * Changing type of slot ref which is the same as the type of slot desc.
     * Putting this logic in here is helpless.
     * If Doris could analyze mv column in the future, please move this logic before reanalyze.
     * <p>
     * - The parameters of the sum function may involve the columns of a materialized view.
     * - The type of this column may happen to be inconsistent with the column type of the base table.
     * - In order to ensure the correctness of the result,
     *   the parameter type needs to be changed to the type of the materialized view column
     *   to ensure the correctness of the result.
     * - Currently only the sum function will involve this problem.
     */
    public void updateTypeOfAggregateExprs() {
        for (FunctionCallExpr functionCallExpr : aggregateExprs_) {
            if (!functionCallExpr.getFnName().getFunction().equalsIgnoreCase("sum")) {
                continue;
            }
            List<SlotRef> slots = new ArrayList<>();
            functionCallExpr.collect(SlotRef.class, slots);
            if (slots.size() != 1) {
                continue;
            }
            SlotRef slotRef = slots.get(0);
            if (slotRef.getDesc() == null) {
                continue;
            }
            if (slotRef.getType() != slotRef.getDesc().getType()) {
                slotRef.setType(slotRef.getDesc().getType());
            }
        }
    }

    /**
     * Mark slots required for this aggregation as materialized:
     * - all grouping output slots as well as grouping exprs
     * - for non-distinct aggregation: the aggregate exprs of materialized aggregate slots;
     *   this assumes that the output slots corresponding to aggregate exprs have already
     *   been marked by the consumer of this select block
     * - for distinct aggregation, we mark all aggregate output slots in order to keep
     *   things simple
     * Also computes materializedAggregateExprs.
     * This call must be idempotent because it may be called more than once for Union stmt.
     */
    @Override
    public void materializeRequiredSlots(Analyzer analyzer, ExprSubstitutionMap smap) {
        for (int i = 0; i < groupingExprs_.size(); ++i) {
            outputTupleDesc_.getSlots().get(i).setIsMaterialized(true);
            intermediateTupleDesc_.getSlots().get(i).setIsMaterialized(true);
        }

        // collect input exprs: grouping exprs plus aggregate exprs that need to be
        // materialized
        materializedSlots_.clear();
        List<Expr> exprs = Lists.newArrayList();
        exprs.addAll(groupingExprs_);
     
        boolean hasCountStar = false;
        int aggregateExprsSize = aggregateExprs_.size();
        int groupExprsSize = groupingExprs_.size();
        boolean isDistinctAgg = isDistinctAgg();
        for (int i = 0; i < aggregateExprsSize; ++i) {
            FunctionCallExpr functionCallExpr = aggregateExprs_.get(i);
            SlotDescriptor slotDesc =
                    outputTupleDesc_.getSlots().get(groupExprsSize + i);
            SlotDescriptor intermediateSlotDesc =
                    intermediateTupleDesc_.getSlots().get(groupExprsSize + i);
            if (isDistinctAgg || isMultiDistinct_) {
                slotDesc.setIsMaterialized(true);
                intermediateSlotDesc.setIsMaterialized(true);
            }
            
            if (functionCallExpr.isCountStar()) {
                hasCountStar = true;
            }
            
            if (!slotDesc.isMaterialized()) continue;
            
            intermediateSlotDesc.setIsMaterialized(true);
            exprs.add(functionCallExpr);
            materializedSlots_.add(i);
        }
        
        List<Expr> resolvedExprs = Expr.substituteList(exprs, smap, analyzer, false);
        
        // In order to meet the requirements materialize slots In the top-down phase 
        // over query statements, if aggregate functions contain count(*), now 
        // materialize all slots this SelectStmt maps.
        // chenhao added.
        if (hasCountStar && smap != null && smap.size() > 0) {
            resolvedExprs.addAll(smap.getRhs());
        } 
        analyzer.materializeSlots(resolvedExprs);

        if (isDistinctAgg()) {
            secondPhaseDistinctAggInfo_.materializeRequiredSlots(analyzer, null);
        }
    }

    /**
     * Returns DataPartition derived from grouping exprs.
     * Returns unpartitioned spec if no grouping.
     * TODO: this won't work when we start supporting range partitions,
     * because we could derive both hash and order-based partitions
     */
    public DataPartition getPartition() {
        if (groupingExprs_.isEmpty()) {
            return DataPartition.UNPARTITIONED;
        } else {
            return new DataPartition(TPartitionType.HASH_PARTITIONED, groupingExprs_);
        }
    }

    public String debugString() {
        StringBuilder out = new StringBuilder(super.debugString());
        out.append(MoreObjects.toStringHelper(this)
                .add("phase", aggPhase_)
                .add("intermediate_smap", intermediateTupleSmap_.debugString())
                .add("output_smap", outputTupleSmap_.debugString())
                .toString());
        if (mergeAggInfo_ != this && mergeAggInfo_ != null) {
            out.append("\nmergeAggInfo:\n" + mergeAggInfo_.debugString());
        }
        if (secondPhaseDistinctAggInfo_ != null) {
            out.append("\nsecondPhaseDistinctAggInfo:\n"
                    + secondPhaseDistinctAggInfo_.debugString());
        }
        return out.toString();
    }

    @Override
    protected String tupleDebugName() { return "agg-tuple"; }

    @Override
    public AggregateInfo clone() { return new AggregateInfo(this); }
}
