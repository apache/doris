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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.PlaceHolderExpr;
import org.apache.doris.analysis.PredicateUtils;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.FederationBackendPolicy;
import org.apache.doris.datasource.SplitAssignment;
import org.apache.doris.datasource.SplitGenerator;
import org.apache.doris.datasource.SplitSource;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPlanNode;
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
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Representation of the common elements of all scan nodes.
 */
public abstract class ScanNode extends PlanNode implements SplitGenerator {
    private static final Logger LOG = LogManager.getLogger(ScanNode.class);
    protected static final int NUM_SPLITS_PER_PARTITION = 10;
    protected static final int NUM_SPLITTERS_ON_FLIGHT = Config.max_external_cache_loader_thread_pool_size;
    protected TupleDescriptor desc;
    // for distribution prunner
    protected Map<String, PartitionColumnFilter> columnFilters = new CaseInsensitiveMap();
    // Use this if partition_prune_algorithm_version is 2.
    protected Map<String, ColumnRange> columnNameToRange = Maps.newHashMap();
    protected String sortColumn = null;
    protected List<TScanRangeLocations> scanRangeLocations = Lists.newArrayList();
    protected List<SplitSource> splitSources = Lists.newArrayList();
    protected PartitionInfo partitionsInfo = null;
    protected SplitAssignment splitAssignment = null;

    protected long selectedPartitionNum = 0;
    protected int selectedSplitNum = 0;

    // create a mapping between output slot's id and project expr
    Map<SlotId, Expr> outputSlotToProjectExpr = new HashMap<>();

    // support multi topn filter
    protected final List<SortNode> topnFilterSortNodes = Lists.newArrayList();

    protected TableSnapshot tableSnapshot;
    protected List<Column> columns;

    // Save the id of backends which this scan node will be executed on.
    // This is also important for local shuffle logic.
    // Now only OlapScanNode and FileQueryScanNode implement this.
    protected HashSet<Long> scanBackendIds = new HashSet<>();

    public ScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName, StatisticalType statisticalType) {
        super(id, desc.getId().asList(), planNodeName, statisticalType);
        this.desc = desc;
    }

    protected List<Column> getColumns() {
        if (columns == null && desc.getTable() != null) {
            columns = desc.getTable().getBaseSchema();
        }
        return columns;
    }

    public TupleDescriptor getTupleDesc() {
        return desc;
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
        } else if (dstType != srcType || slotDesc.getType().isAggStateType() && expr.getType().isAggStateType()
                && !slotDesc.getType().equals(expr.getType())) {
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
            computeColumnsFilter(getColumns(), partitionsInfo);
        }
    }

    public TableIf getTableIf() {
        return desc.getTable();
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
                LiteralExpr literal = slotBinding instanceof PlaceHolderExpr
                        ? ((PlaceHolderExpr) slotBinding).getLiteral() : (LiteralExpr) slotBinding;
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("partitionColumnFilter: {}", partitionColumnFilter);
        }
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
        return MoreObjects.toStringHelper(this)
                .add("id", getId().asInt())
                .add("tid", desc.getId().asInt())
                .add("tblName", desc.getTable().getName())
                .add("keyRanges", "")
                .addValue(super.debugString()).toString();
    }

    public List<TupleId> getOutputTupleIds() {
        if (outputTupleDesc != null) {
            return Lists.newArrayList(outputTupleDesc.getId());
        }
        return tupleIds;
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

    public int numScanBackends() {
        return scanBackendIds.size();
    }

    public int getScanRangeNum() {
        return Integer.MAX_VALUE;
    }

    public boolean shouldUseOneInstance(ConnectContext ctx) {
        int adaptivePipelineTaskSerialReadOnLimit = 10000;

        if (ctx != null) {
            if (ctx.getSessionVariable().enableAdaptivePipelineTaskSerialReadOnLimit) {
                adaptivePipelineTaskSerialReadOnLimit = ctx.getSessionVariable().adaptivePipelineTaskSerialReadOnLimit;
            } else {
                return false;
            }
        } else {
            // No connection context, typically for broker load.
        }

        if (hasLimit() && getLimit() <= adaptivePipelineTaskSerialReadOnLimit) {
            if (conjuncts.isEmpty()) {
                return true;
            } else {
                if (this instanceof OlapScanNode) {
                    OlapScanNode olapScanNode = (OlapScanNode) this;
                    if (olapScanNode.getOlapTable() != null
                            && olapScanNode.getOlapTable().getKeysType() == KeysType.UNIQUE_KEYS) {
                        // If the table is unique keys, we can check if the conjuncts only contains
                        // delete sign
                        if (conjuncts.size() == 1 && conjuncts.get(0) instanceof BinaryPredicate) {
                            BinaryPredicate binaryPredicate = (BinaryPredicate) conjuncts.get(0);
                            if (binaryPredicate.getOp() == BinaryPredicate.Operator.EQ
                                    && binaryPredicate.getChild(0) instanceof SlotRef
                                    && ((SlotRef) binaryPredicate.getChild(0)).getDesc().getColumn().getName()
                                            .equals(Column.DELETE_SIGN)) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    // In cloud mode, meta read lock is not enough to keep a snapshot of the partition versions.
    // After all scan node are collected, it is possible to gain a snapshot of the partition version.
    public static void setVisibleVersionForOlapScanNodes(List<ScanNode> scanNodes) throws UserException {
        if (Config.isNotCloudMode()) {
            return;
        }

        List<CloudPartition> partitions = new ArrayList<>();
        Set<Long> partitionSet = new HashSet<>();
        for (ScanNode node : scanNodes) {
            if (!(node instanceof OlapScanNode)) {
                continue;
            }

            OlapScanNode scanNode = (OlapScanNode) node;
            OlapTable table = scanNode.getOlapTable();
            for (Long id : scanNode.getSelectedPartitionIds()) {
                if (!partitionSet.contains(id)) {
                    partitionSet.add(id);
                    partitions.add((CloudPartition) table.getPartition(id));
                }
            }
        }

        if (partitions.isEmpty()) {
            return;
        }

        List<Long> versions;
        try {
            versions = CloudPartition.getSnapshotVisibleVersion(partitions);
        } catch (RpcException e) {
            throw new UserException("get visible version for OlapScanNode failed", e);
        }

        assert versions.size() == partitions.size() : "the got num versions is not equals to acquired num versions";
        if (versions.stream().anyMatch(x -> x <= 0)) {
            int size = versions.size();
            for (int i = 0; i < size; ++i) {
                if (versions.get(i) <= 0) {
                    LOG.warn("partition {} getVisibleVersion error, the visibleVersion is {}",
                            partitions.get(i).getId(), versions.get(i));
                    throw new UserException("partition " + partitions.get(i).getId()
                        + " getVisibleVersion error, the visibleVersion is " + versions.get(i));
                }
            }
        }

        // ATTN: the table ids are ignored here because the both id are allocated from a same id generator.
        Map<Long, Long> visibleVersionMap = IntStream.range(0, versions.size())
                .boxed()
                .collect(Collectors.toMap(i -> partitions.get(i).getId(), versions::get));

        for (ScanNode node : scanNodes) {
            if (!(node instanceof OlapScanNode)) {
                continue;
            }

            OlapScanNode scanNode = (OlapScanNode) node;
            scanNode.updateScanRangeVersions(visibleVersionMap);
        }
    }

    protected void toThrift(TPlanNode msg) {
        // topn filter
        if (useTopnFilter()) {
            List<Integer> topnFilterSourceNodeIds = getTopnFilterSortNodes()
                    .stream()
                    .map(sortNode -> sortNode.getId().asInt())
                    .collect(Collectors.toList());
            msg.setTopnFilterSourceNodeIds(topnFilterSourceNodeIds);
        }
    }

    public void addTopnFilterSortNode(SortNode sortNode) {
        topnFilterSortNodes.add(sortNode);
    }

    public List<SortNode> getTopnFilterSortNodes() {
        return topnFilterSortNodes;
    }

    public boolean useTopnFilter() {
        return !topnFilterSortNodes.isEmpty();
    }

    public long getSelectedPartitionNum() {
        return selectedPartitionNum;
    }

    public long getSelectedSplitNum() {
        return selectedSplitNum;
    }

    @Override
    public boolean isSerialOperator() {
        return numScanBackends() <= 0 || getScanRangeNum()
                < ConnectContext.get().getSessionVariable().getParallelExecInstanceNum() * numScanBackends()
                || (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().isForceToLocalShuffle());
    }

    @Override
    public boolean hasSerialScanChildren() {
        return isSerialOperator();
    }

    public void setDesc(TupleDescriptor desc) {
        this.desc = desc;
    }
}
