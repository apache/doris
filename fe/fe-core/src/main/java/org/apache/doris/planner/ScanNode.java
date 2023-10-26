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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/ScanNode.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.PredicateUtils;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.external.FederationBackendPolicy;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.statistics.query.StatsDelta;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Representation of the common elements of all scan nodes.
 */
public abstract class ScanNode extends PlanNode {
    private static final Logger LOG = LogManager.getLogger(ScanNode.class);
    protected final TupleDescriptor desc;
    // for distribution prunner
    protected Map<String, PartitionColumnFilter> columnFilters = Maps.newHashMap();
    // Use this if partition_prune_algorithm_version is 2.
    protected Map<String, ColumnRange> columnNameToRange = Maps.newHashMap();
    protected String sortColumn = null;
    protected Analyzer analyzer;
    protected List<TScanRangeLocations> scanRangeLocations = Lists.newArrayList();
    protected PartitionInfo partitionsInfo = null;

    public ScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName, StatisticalType statisticalType) {
        super(id, desc.getId().asList(), planNodeName, statisticalType);
        this.desc = desc;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        this.analyzer = analyzer;
        // materialize conjuncts in where
        analyzer.materializeSlots(conjuncts);
    }

    /**
     * Helper function to parse a "host:port" address string into TNetworkAddress
     * This is called with ipaddress:port when doing scan range assigment.
     */
    protected static TNetworkAddress addressToTNetworkAddress(String address) {
        TNetworkAddress result = new TNetworkAddress();
        String[] hostPort = address.split(":");
        result.hostname = hostPort[0];
        result.port = Integer.parseInt(hostPort[1]);
        return result;
    }

    public TupleDescriptor getTupleDesc() {
        return desc;
    }

    public void setSortColumn(String column) {
        sortColumn = column;
    }

    protected List<Split> getSplits() throws UserException {
        throw new NotImplementedException("Scan node sub class need to implement getSplits interface.");
    }

    /**
     * cast expr to SlotDescriptor type
     */
    protected Expr castToSlot(SlotDescriptor slotDesc, Expr expr) throws UserException {
        PrimitiveType dstType = slotDesc.getType().getPrimitiveType();
        PrimitiveType srcType = expr.getType().getPrimitiveType();
        if (PrimitiveType.typeWithPrecision.contains(dstType) && PrimitiveType.typeWithPrecision.contains(srcType)
                && !slotDesc.getType().equals(expr.getType())) {
            return expr.castTo(slotDesc.getType());
        } else if (dstType != srcType) {
            return expr.castTo(slotDesc.getType());
        } else {
            return expr;
        }
    }

    protected abstract void createScanRangeLocations() throws UserException;

    /**
     * Returns all scan ranges plus their locations. Needs to be preceded by a call to
     * finalize().
     *
     * @param maxScanRangeLength The maximum number of bytes each scan range should scan;
     *                           only applicable to HDFS; less than or equal to zero means no
     *                           maximum.
     */
    public abstract List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength);

    // If scan is key search, should not enable the shared scan opt to prevent the performance problem
    // 1. where contain the eq or in expr of key column slot
    // 2. key column slot is distribution column and first column
    protected boolean isKeySearch() {
        return false;
    }

    /**
     * Update required_slots in scan node contexts. This is called after Nereids planner do the projection.
     * In the projection process, some slots may be removed. So call this to update the slots info.
     * Currently, it is only used by ExternalFileScanNode, add the interface here to keep the Nereids code clean.
     */
    public void updateRequiredSlots(PlanTranslatorContext context,
            Set<SlotId> requiredByProjectSlotIdSet) throws UserException {
    }

    private void computeColumnFilter(Column column, SlotDescriptor slotDesc, PartitionInfo partitionsInfo) {
        // Set `columnFilters` all the time because `DistributionPruner` also use this.
        // Maybe we could use `columnNameToRange` for `DistributionPruner` and
        // only create `columnFilters` when `partition_prune_algorithm_version` is 1.
        PartitionColumnFilter keyFilter = createPartitionFilter(slotDesc, conjuncts, partitionsInfo);
        if (null != keyFilter) {
            columnFilters.put(column.getName(), keyFilter);
        }

        ColumnRange columnRange = createColumnRange(slotDesc, conjuncts, partitionsInfo);
        if (columnRange != null) {
            columnNameToRange.put(column.getName(), columnRange);
        }
    }

    // TODO(ML): move it into PrunerOptimizer
    public void computeColumnsFilter(List<Column> columns, PartitionInfo partitionsInfo) {
        if (columns.size() > conjuncts.size()) {
            Set<SlotRef> slotRefs = Sets.newHashSet();
            for (Expr conjunct : conjuncts) {
                conjunct.collect(SlotRef.class, slotRefs);
            }
            for (SlotRef slotRef : slotRefs) {
                SlotDescriptor slotDesc = slotRef.getDesc();
                if (null == slotDesc) {
                    continue;
                }
                Column column = slotDesc.getColumn();
                if (column == null) {
                    continue;
                }
                computeColumnFilter(column, slotDesc, partitionsInfo);
            }
        } else {
            for (Column column : columns) {
                SlotDescriptor slotDesc = desc.getColumnSlot(column.getName());
                if (null == slotDesc) {
                    continue;
                }
                computeColumnFilter(column, slotDesc, partitionsInfo);
            }
        }
    }

    public void computeColumnsFilter() {
        // for load scan node, table is null
        // partitionsInfo maybe null for other scan node, eg: ExternalScanNode...
        if (desc.getTable() != null) {
            computeColumnsFilter(desc.getTable().getBaseSchema(), partitionsInfo);
        }
    }

    public static ColumnRange createColumnRange(SlotDescriptor desc,
            List<Expr> conjuncts, PartitionInfo partitionsInfo) {
        ColumnRange result = ColumnRange.create();
        for (Expr expr : conjuncts) {
            if (!expr.isBound(desc.getId())) {
                continue;
            }

            if (expr instanceof CompoundPredicate
                    && ((CompoundPredicate) expr).getOp() == CompoundPredicate.Operator.OR) {
                // Try to get column filter from disjunctive predicates.
                List<Expr> disjunctivePredicates = PredicateUtils.splitDisjunctivePredicates(expr);
                if (disjunctivePredicates.isEmpty()) {
                    continue;
                }

                List<Range<ColumnBound>> disjunctiveRanges = Lists.newArrayList();
                Set<Boolean> hasIsNull = Sets.newHashSet();
                boolean allMatch = disjunctivePredicates.stream().allMatch(e -> {
                    ColumnRanges ranges = expressionToRanges(e, desc, partitionsInfo);
                    switch (ranges.type) {
                        case IS_NULL:
                            hasIsNull.add(true);
                            return true;
                        case CONVERT_SUCCESS:
                            disjunctiveRanges.addAll(ranges.ranges);
                            return true;
                        case CONVERT_FAILURE:
                        default:
                            return false;

                    }
                });
                if (allMatch && !(disjunctiveRanges.isEmpty() && hasIsNull.isEmpty())) {
                    result.intersect(disjunctiveRanges);
                    result.setHasDisjunctiveIsNull(!hasIsNull.isEmpty());
                }
            } else {
                // Try to get column filter from conjunctive predicates.
                ColumnRanges ranges = expressionToRanges(expr, desc, partitionsInfo);
                switch (ranges.type) {
                    case IS_NULL:
                        result.setHasConjunctiveIsNull(true);
                        break;
                    case CONVERT_SUCCESS:
                        result.intersect(ranges.ranges);
                        break;
                    case CONVERT_FAILURE:
                    default:
                        break;
                }
            }
        }
        return result;
    }

    public static ColumnRanges expressionToRanges(Expr expr,
            SlotDescriptor desc, PartitionInfo partitionsInfo) {
        if (expr instanceof IsNullPredicate) {
            IsNullPredicate isNullPredicate = (IsNullPredicate) expr;
            if (isNullPredicate.isSlotRefChildren() && !isNullPredicate.isNotNull()) {
                return ColumnRanges.createIsNull();
            }
        }

        List<Range<ColumnBound>> result = Lists.newArrayList();
        if (expr instanceof BinaryPredicate) {
            BinaryPredicate binPred = (BinaryPredicate) expr;
            ArrayList<Expr> partitionExprs = (partitionsInfo != null && partitionsInfo.enableAutomaticPartition())
                    ? partitionsInfo.getPartitionExprs()
                    : null;
            Expr slotBinding = binPred.getSlotBinding(desc.getId(), partitionExprs);
            if (slotBinding == null || !slotBinding.isConstant() || !(slotBinding instanceof LiteralExpr)) {
                return ColumnRanges.createFailure();
            }

            LiteralExpr value = (LiteralExpr) slotBinding;
            switch (binPred.getOp()) {
                case EQ:
                    ColumnBound bound = ColumnBound.of(value);
                    result.add(Range.closed(bound, bound));
                    break;
                case LE:
                    result.add(Range.atMost(ColumnBound.of(value)));
                    break;
                case LT:
                    result.add(Range.lessThan(ColumnBound.of(value)));
                    break;
                case GE:
                    result.add(Range.atLeast(ColumnBound.of(value)));
                    break;
                case GT:
                    result.add(Range.greaterThan(ColumnBound.of(value)));
                    break;
                case NE:
                    ColumnBound b = ColumnBound.of(value);
                    result.add(Range.greaterThan(b));
                    result.add(Range.lessThan(b));
                    break;
                default:
                    break;
            }
        } else if (expr instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) expr;
            if (!inPredicate.isLiteralChildren() || inPredicate.isNotIn()) {
                return ColumnRanges.createFailure();
            }

            if (!(inPredicate.getChild(0).unwrapExpr(false) instanceof SlotRef)) {
                // If child(0) of the in predicate is not a SlotRef,
                // then other children of in predicate should not be used as a condition for partition prune.
                return ColumnRanges.createFailure();
            }

            for (int i = 1; i < inPredicate.getChildren().size(); ++i) {
                ColumnBound bound = ColumnBound.of((LiteralExpr) inPredicate.getChild(i));
                result.add(Range.closed(bound, bound));
            }
        } else if (expr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
            ColumnRanges leftChildRange = null;
            ColumnRanges rightChildRange = null;
            switch (compoundPredicate.getOp()) {
                case AND:
                    leftChildRange = expressionToRanges(compoundPredicate.getChild(0), desc, partitionsInfo);
                    rightChildRange = expressionToRanges(compoundPredicate.getChild(1), desc, partitionsInfo);
                    return leftChildRange.intersectRanges(rightChildRange);
                case OR:
                    leftChildRange = expressionToRanges(compoundPredicate.getChild(0), desc, partitionsInfo);
                    rightChildRange = expressionToRanges(compoundPredicate.getChild(1), desc, partitionsInfo);
                    return leftChildRange.unionRanges(rightChildRange);
                case NOT:
                    leftChildRange = expressionToRanges(compoundPredicate.getChild(0), desc, partitionsInfo);
                    return leftChildRange.complementOfRanges();
                default:
                    throw new RuntimeException("unknown OP in compound predicate: "
                        + compoundPredicate.getOp().toString());
            }
        }

        if (result.isEmpty()) {
            return ColumnRanges.createFailure();
        } else {
            return ColumnRanges.create(result);
        }
    }

    private PartitionColumnFilter createPartitionFilter(SlotDescriptor desc, List<Expr> conjuncts,
            PartitionInfo partitionsInfo) {
        PartitionColumnFilter partitionColumnFilter = null;
        for (Expr expr : conjuncts) {
            if (!expr.isBound(desc.getId())) {
                continue;
            }

            if (expr instanceof BinaryPredicate) {
                BinaryPredicate binPredicate = (BinaryPredicate) expr;
                if (binPredicate.getOp() == BinaryPredicate.Operator.NE) {
                    continue;
                }

                ArrayList<Expr> partitionExprs = (partitionsInfo != null && partitionsInfo.enableAutomaticPartition())
                        ? partitionsInfo.getPartitionExprs()
                        : null;
                Expr slotBinding = binPredicate.getSlotBinding(desc.getId(), partitionExprs);

                if (slotBinding == null || !slotBinding.isConstant() || !(slotBinding instanceof LiteralExpr)) {
                    continue;
                }

                if (null == partitionColumnFilter) {
                    partitionColumnFilter = new PartitionColumnFilter();
                }
                LiteralExpr literal = (LiteralExpr) slotBinding;
                BinaryPredicate.Operator op = binPredicate.getOp();
                if (!binPredicate.slotIsLeft()) {
                    op = op.commutative();
                }
                switch (op) {
                    case EQ:
                        partitionColumnFilter.setLowerBound(literal, true);
                        partitionColumnFilter.setUpperBound(literal, true);
                        break;
                    case LE:
                        partitionColumnFilter.setUpperBound(literal, true);
                        partitionColumnFilter.lowerBoundInclusive = true;
                        break;
                    case LT:
                        partitionColumnFilter.setUpperBound(literal, false);
                        partitionColumnFilter.lowerBoundInclusive = true;
                        break;
                    case GE:
                        partitionColumnFilter.setLowerBound(literal, true);
                        break;
                    case GT:
                        partitionColumnFilter.setLowerBound(literal, false);
                        break;
                    default:
                        break;
                }
            } else if (expr instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) expr;
                if (!inPredicate.isLiteralChildren() || inPredicate.isNotIn()) {
                    continue;
                }
                if (!(inPredicate.getChild(0).unwrapExpr(false) instanceof SlotRef)) {
                    // If child(0) of the in predicate is not a SlotRef,
                    // then other children of in predicate should not be used as a condition for partition prune.
                    continue;
                }
                if (null == partitionColumnFilter) {
                    partitionColumnFilter = new PartitionColumnFilter();
                }
                partitionColumnFilter.setInPredicate(inPredicate);
            } else if (expr instanceof IsNullPredicate) {
                IsNullPredicate isNullPredicate = (IsNullPredicate) expr;
                if (!isNullPredicate.isSlotRefChildren() || isNullPredicate.isNotNull()) {
                    continue;
                }

                // If we meet a IsNull predicate on partition column, then other predicates are useless
                // eg: (xxxx) and (col is null), only the IsNull predicate has an effect on partition pruning.
                partitionColumnFilter = new PartitionColumnFilter();
                NullLiteral nullLiteral = new NullLiteral();
                partitionColumnFilter.setLowerBound(nullLiteral, true);
                partitionColumnFilter.setUpperBound(nullLiteral, true);
                break;
            }

        }
        LOG.debug("partitionColumnFilter: {}", partitionColumnFilter);
        return partitionColumnFilter;
    }

    public static class ColumnRanges {
        public enum Type {
            // Expression is `is null` predicate.
            IS_NULL,
            // Succeed to convert expression to ranges.
            CONVERT_SUCCESS,
            // Failed to convert expression to ranges.
            CONVERT_FAILURE
        }

        public final Type type;
        public final List<Range<ColumnBound>> ranges;

        private ColumnRanges(Type type, List<Range<ColumnBound>> ranges) {
            this.type = type;
            this.ranges = ranges;
        }

        private static final ColumnRanges IS_NULL = new ColumnRanges(Type.IS_NULL, null);

        private static final ColumnRanges CONVERT_FAILURE = new ColumnRanges(Type.CONVERT_FAILURE, null);

        public static ColumnRanges createIsNull() {
            return IS_NULL;
        }

        public ColumnRanges complementOfRanges() {
            if (type == Type.CONVERT_SUCCESS) {
                RangeSet<ColumnBound> rangeSet = TreeRangeSet.create();
                rangeSet.addAll(ranges);
                return create(Lists.newArrayList(rangeSet.complement().asRanges()));
            }
            return CONVERT_FAILURE;
        }

        public ColumnRanges intersectRanges(ColumnRanges other) {
            // intersect ranges can handle isnull
            switch (this.type) {
                case IS_NULL:
                    return createIsNull();
                case CONVERT_FAILURE:
                    return createFailure();
                case CONVERT_SUCCESS:
                    switch (other.type) {
                        case IS_NULL:
                            return createIsNull();
                        case CONVERT_FAILURE:
                            return createFailure();
                        case CONVERT_SUCCESS:
                            RangeSet<ColumnBound> rangeSet = TreeRangeSet.create();
                            rangeSet.addAll(this.ranges);
                            RangeSet<ColumnBound> intersectSet = TreeRangeSet.create();

                            other.ranges.forEach(range -> intersectSet.addAll(rangeSet.subRangeSet(range)));
                            return create(Lists.newArrayList(intersectSet.asRanges()));
                        default:
                            return createFailure();
                    }
                default:
                    return createFailure();
            }
        }

        public ColumnRanges unionRanges(ColumnRanges other) {
            switch (this.type) {
                case IS_NULL:
                case CONVERT_FAILURE:
                    return createFailure();
                case CONVERT_SUCCESS:
                    switch (other.type) {
                        case IS_NULL:
                        case CONVERT_FAILURE:
                            return createFailure();
                        case CONVERT_SUCCESS:
                            RangeSet<ColumnBound> rangeSet = TreeRangeSet.create();
                            rangeSet.addAll(this.ranges);
                            rangeSet.addAll(other.ranges);
                            List<Range<ColumnBound>> unionRangeList = Lists.newArrayList(rangeSet.asRanges());
                            return create(unionRangeList);
                        default:
                            return createFailure();
                    }
                default:
                    return createFailure();
            }
        }

        public static ColumnRanges createFailure() {
            return CONVERT_FAILURE;
        }

        public static ColumnRanges create(List<Range<ColumnBound>> ranges) {
            return new ColumnRanges(Type.CONVERT_SUCCESS, ranges);
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("tid", desc.getId().asInt()).add("tblName",
                desc.getTable().getName()).add("keyRanges", "").addValue(
                super.debugString()).toString();
    }

    // Some of scan node(eg, DataGenScanNode) does not need to check column priv
    // (because the it has no corresponding catalog/db/table info)
    // Subclass may override this method.
    public boolean needToCheckColumnPriv() {
        return true;
    }

    public void setOutputSmap(ExprSubstitutionMap smap, Analyzer analyzer) {
        outputSmap = smap;
        if (smap.getRhs().stream().anyMatch(expr -> !(expr instanceof SlotRef))) {
            if (outputTupleDesc == null) {
                outputTupleDesc = analyzer.getDescTbl().createTupleDescriptor("OlapScanNode");
            }
            if (projectList == null) {
                projectList = Lists.newArrayList();
            }
            // setOutputSmap may be called multiple times
            // this happens if the olap table is in the most inner sub-query block in the cascades sub-queries
            // create a tmpSmap for the later setOutputSmap call
            ExprSubstitutionMap tmpSmap = new ExprSubstitutionMap(
                    Lists.newArrayList(outputTupleDesc.getSlots().stream()
                            .filter(slot -> slot.isMaterialized())
                            .map(slot -> new SlotRef(slot))
                            .collect(Collectors.toList())),
                    Lists.newArrayList(projectList));
            Set<SlotId> allOutputSlotIds = outputTupleDesc.getSlots().stream().map(slot -> slot.getId())
                    .collect(Collectors.toSet());
            List<Expr> newRhs = Lists.newArrayList();
            List<Expr> rhs = smap.getRhs();
            for (int i = 0; i < smap.size(); ++i) {
                Expr rhsExpr = rhs.get(i);
                if (!(rhsExpr instanceof SlotRef) || !(allOutputSlotIds.contains(((SlotRef) rhsExpr).getSlotId()))) {
                    rhsExpr = rhsExpr.substitute(tmpSmap);
                    if (rhsExpr.isBound(desc.getId())) {
                        SlotDescriptor slotDesc = analyzer.addSlotDescriptor(outputTupleDesc);
                        slotDesc.initFromExpr(rhsExpr);
                        if (rhsExpr instanceof SlotRef) {
                            slotDesc.setSrcColumn(((SlotRef) rhsExpr).getColumn());
                            slotDesc.setIsMaterialized(((SlotRef) rhsExpr).getDesc().isMaterialized());
                        } else {
                            slotDesc.setIsMaterialized(true);
                        }
                        if (slotDesc.isMaterialized()) {
                            slotDesc.materializeSrcExpr();
                            projectList.add(rhsExpr);
                        }
                        newRhs.add(new SlotRef(slotDesc));
                        allOutputSlotIds.add(slotDesc.getId());
                    } else {
                        newRhs.add(rhs.get(i));
                    }
                } else {
                    newRhs.add(rhsExpr);
                }
            }
            outputSmap.updateRhsExprs(newRhs);
        }
    }

    public List<TupleId> getOutputTupleIds() {
        if (outputTupleDesc != null) {
            return Lists.newArrayList(outputTupleDesc.getId());
        }
        return tupleIds;
    }

    public StatsDelta genStatsDelta() throws AnalysisException {
        return null;
    }

    public StatsDelta genQueryStats() throws UserException {
        StatsDelta delta = genStatsDelta();
        if (delta == null) {
            return null;
        }
        for (SlotDescriptor slot : desc.getMaterializedSlots()) {
            if (slot.isScanSlot() && slot.getColumn() != null) {
                delta.addQueryStats(slot.getColumn().getName());
            }
        }

        for (Expr expr : conjuncts) {
            List<SlotId> slotIds = Lists.newArrayList();
            expr.getIds(null, slotIds);
            for (SlotId slotId : slotIds) {
                SlotDescriptor slot = desc.getSlot(slotId.asInt());
                if (slot.getColumn() != null) {
                    delta.addFilterStats(slot.getColumn().getName());
                }
            }
        }
        return delta;
    }

    // Create a single scan range locations for the given backend policy.
    // Used for those scan nodes which do not require data location.
    public static TScanRangeLocations createSingleScanRangeLocations(FederationBackendPolicy backendPolicy) {
        TScanRangeLocations scanRangeLocation = new TScanRangeLocations();
        scanRangeLocation.setScanRange(new TScanRange());
        TScanRangeLocation location = new TScanRangeLocation();
        Backend be = backendPolicy.getNextBe();
        location.setServer(new TNetworkAddress(be.getHost(), be.getBePort()));
        location.setBackendId(be.getId());
        scanRangeLocation.addToLocations(location);
        return scanRangeLocation;
    }

    // some scan should not enable the shared scan opt to prevent the performance problem
    // 1. is key search
    // 2. session variable not enable_shared_scan
    public boolean shouldDisableSharedScan(ConnectContext context) {
        return isKeySearch() || !context.getSessionVariable().getEnableSharedScan();
    }
}
