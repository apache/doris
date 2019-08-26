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

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Select best partitions for query.
 */
public final class PartitionSelector {
    private static final Logger LOG = LogManager.getLogger(PartitionSelector.class);
    private static final Map<String, Set<String>> supportedExprs;
    private final OlapTable olapTable;
    private final TupleDescriptor tupleDesc;
    private final List<Expr> predicates;
    private final Multimap<String, PartitionColumnFilter> binaryColumnFilters;
    private final Multimap<String, PartitionColumnFilter> inPredicatePartitionColumnFilters;
    private final Multimap<String, PartitionColumnFilter> isNullPredicatePartitionColumnFilters;
    private boolean filterEmptyPartition;

    static {
        // Partition pruning only supported monotonic expr or function.
        supportedExprs = Maps.newHashMap();

        supportedExprs.put("SlotRef", null);
        // Literal
        supportedExprs.put("BoolLiteral", null);
        supportedExprs.put("IntLiteral", null);
        supportedExprs.put("LargeIntLiteral", null);
        supportedExprs.put("FloatLiteral", null);
        supportedExprs.put("DecimalLiteral", null);
        supportedExprs.put("DateLiteral", null);
        supportedExprs.put("StringLiteral", null);

        // Cast
        supportedExprs.put("CastExpr", null);

        // Functions
        final Set<String> supportedFunctions = Sets.newHashSet();
        {
            supportedFunctions.add("unix_timestamp");
            supportedFunctions.add("from_unixtime");
            supportedFunctions.add("now");
            supportedFunctions.add("day");
            supportedFunctions.add("curtime");
            supportedFunctions.add("weekofyear");
            supportedFunctions.add("utc_timestamp");
            supportedFunctions.add("timestamp");
            supportedFunctions.add("from_days");
            supportedFunctions.add("to_days");
            supportedFunctions.add("years_add");
            supportedFunctions.add("years_sub");
            supportedFunctions.add("months_add");
            supportedFunctions.add("months_sub");
            supportedFunctions.add("weeks_add");
            supportedFunctions.add("weeks_sub");
            supportedFunctions.add("days_add");
            supportedFunctions.add("days_sub");
            supportedFunctions.add("hours_add");
            supportedFunctions.add("hours_sub");
            supportedFunctions.add("minutes_add");
            supportedFunctions.add("minutes_sub");
            supportedFunctions.add("seconds_add");
            supportedFunctions.add("seconds_sub");
            supportedFunctions.add("microseconds_add");
            supportedFunctions.add("microseconds_sub");
            supportedFunctions.add("datediff");
            supportedFunctions.add("timediff");
            supportedFunctions.add("str_to_date");
            supportedFunctions.add("date_format");
            supportedFunctions.add("date");

            supportedFunctions.add("pi");
            supportedFunctions.add("e");
            supportedFunctions.add("sqrt");
            supportedFunctions.add("pow");
            supportedFunctions.add("least");
            supportedFunctions.add("greatest");
        }
        supportedExprs.put(FunctionCallExpr.class.getClass().getName(), supportedFunctions);

        // ArithmeticExpr
        final Set<String> supportedArithmeticFunctions = Sets.newHashSet();
        supportedArithmeticFunctions.add(ArithmeticExpr.Operator.ADD.getName());
        supportedArithmeticFunctions.add(ArithmeticExpr.Operator.SUBTRACT.getName());
        supportedArithmeticFunctions.add(ArithmeticExpr.Operator.MULTIPLY.getName());
        supportedArithmeticFunctions.add(ArithmeticExpr.Operator.DIVIDE.getName());
        supportedExprs.put("ArithmeticExpr", supportedFunctions);
    }

    public PartitionSelector(OlapTable olapTable, TupleDescriptor tupleDesc, List<Expr> predicates, Analyzer analyzer) {
        this.olapTable = olapTable;
        this.tupleDesc = tupleDesc;
        this.predicates = predicates;
        this.binaryColumnFilters = ArrayListMultimap.create();
        this.inPredicatePartitionColumnFilters = ArrayListMultimap.create();
        this.isNullPredicatePartitionColumnFilters = ArrayListMultimap.create();
        this.filterEmptyPartition = false;
        initPartitionColumnFilters(analyzer);
    }

    private void initPartitionColumnFilters(Analyzer analyzer) {
        final Map<String, SlotId> pruningPartitionColumns = Maps.newHashMap();
        final PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo instanceof RangePartitionInfo) {
            final RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
            final List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
            Preconditions.checkArgument(partitionColumns.size() == 1,
                    "Range partition can only has one column.");

            final SlotDescriptor partitionSlotDesc = analyzer.getColumnSlot(tupleDesc, partitionColumns.get(0));
            if (partitionSlotDesc != null) {
                pruningPartitionColumns.put(partitionSlotDesc.getColumn().getName(), partitionSlotDesc.getId());
            }
        } else {
            Preconditions.checkArgument(partitionInfo instanceof SinglePartitionInfo);
        }

        for (Partition partition : olapTable.getPartitions()) {
            final HashDistributionInfo info = (HashDistributionInfo)partition.getDistributionInfo();
            // Now multi columns distribution can't be supported by partition pruning.
            if (info.getDistributionColumns().size() == 1) {
                Column column = info.getDistributionColumns().get(0);
                final SlotDescriptor slot = analyzer.getColumnSlot(tupleDesc, column);
                if (slot != null) {
                    pruningPartitionColumns.put(slot.getColumn().getName(), slot.getId());
                }
            }
        }

        if (pruningPartitionColumns.size() > 0 && predicates.size() > 0) {
            createPartitionFilter(pruningPartitionColumns, predicates);
        }
    }

    private void createPartitionFilter(Map<String, SlotId> pruningPartitionColumns, List<Expr> predicates) {
        final Multimap<String, Expr> pruningPartitionPredicates = ArrayListMultimap.create();
        for (String key : pruningPartitionColumns.keySet()) {
            for (Expr predicate : predicates) {
                if (predicate.isBound(pruningPartitionColumns.get(key))) {
                    pruningPartitionPredicates.put(key, predicate);
                }
            }
        }

        if (pruningPartitionPredicates.size() > 0) {
            createBinaryPredicateMeetPartitionPruning(pruningPartitionPredicates);
            createInPredicateMeetPartitionPruning(pruningPartitionPredicates);
            createIsNullPredicateMeetPartitionPruning(pruningPartitionPredicates);
        }
    }

    private void createBinaryPredicateMeetPartitionPruning(Multimap<String, Expr> pruningPartitionPredicates) {
        for (String key : pruningPartitionPredicates.keySet()) {
            for (Expr predicate : pruningPartitionPredicates.get(key)) {
                if (predicate instanceof BinaryPredicate) {
                    final BinaryPredicate binPredicate = (BinaryPredicate) predicate;
                    if (binPredicate.getOp() == BinaryPredicate.Operator.NE) {
                        continue;
                    }
                    final Expr slotBinding = binPredicate.getConstantExpr();
                    if (!(slotBinding instanceof LiteralExpr)) {
                        // The constant expr in BinaryPredicate's children is't supported by FE.
                        LOG.warn("FE can't support to calculate expr:" + slotBinding.toSql());
                        continue;
                    }

                    final Expr slotExpr = binPredicate.getColumnExpr();
                    if (!isSlotExprMeetingPartitionPruning(slotExpr)) {
                        continue;
                    }

                    final LiteralExpr literal = (LiteralExpr) slotBinding;
                    BinaryPredicate.Operator op = binPredicate.getOp();
                    if (!binPredicate.slotIsLeft()) {
                        op = op.commutative();
                    }
                    final PartitionColumnFilter binaryColumnFilter = new PartitionColumnFilter();
                    binaryColumnFilter.setColumnExpr(slotExpr);
                    binaryColumnFilters.put(key, binaryColumnFilter);
                    switch (op) {
                        case EQ:
                            binaryColumnFilter.setLowerBound(literal, true);
                            binaryColumnFilter.setUpperBound(literal, true);
                            break;
                        case LE:
                            binaryColumnFilter.setUpperBound(literal, true);
                            binaryColumnFilter.lowerBoundInclusive = true;
                            break;
                        case LT:
                            binaryColumnFilter.setUpperBound(literal, false);
                            break;
                        case GE:
                            binaryColumnFilter.setLowerBound(literal, true);
                            binaryColumnFilter.upperBoundInclusive = true;
                            break;
                        case GT:
                            binaryColumnFilter.setLowerBound(literal, false);
                            break;
                        default:
                            break;
                    }
                }
            }
        }
    }

    /**
     * For InPredicates which children are SlotRef and LiteralExprs, we will create a new InPredicate
     * which LiteralExpr children are intersections of these InPredicates.
     *
     */
    private void createInPredicateMeetPartitionPruning(Multimap<String, Expr> pruningPartitionPredicates) {
        for (String key : pruningPartitionPredicates.keySet()) {
            for (Expr predicate : pruningPartitionPredicates.get(key)) {
                if (predicate instanceof InPredicate) {
                    final PartitionColumnFilter partitionColumnFilter = new PartitionColumnFilter();
                    InPredicate inPredicate = (InPredicate) predicate;
                    if (!inPredicate.isLiteralChildren() || inPredicate.isNotIn()) {
                        continue;
                    }
                    partitionColumnFilter.setInPredicate(inPredicate);
                    inPredicatePartitionColumnFilters.put(key, partitionColumnFilter);
                }
            }
        }
    }

    private void createIsNullPredicateMeetPartitionPruning(Multimap<String, Expr> pruningPartitionPredicates) {
        for (String key : pruningPartitionPredicates.keySet()) {
            for (Expr predicate : pruningPartitionPredicates.get(key)) {
                if (predicate instanceof IsNullPredicate) {
                    IsNullPredicate isNullPredicate = (IsNullPredicate) predicate;
                    if (!isNullPredicate.isSlotRefChildren() || isNullPredicate.isNotNull()) {
                        continue;
                    }
                    final PartitionColumnFilter isNullPredicatePartitionColumnFilter = new PartitionColumnFilter();
                    // like EQ
                    final NullLiteral nullLiteral = new NullLiteral();
                    isNullPredicatePartitionColumnFilter.setLowerBound(nullLiteral, true);
                    isNullPredicatePartitionColumnFilter.setUpperBound(nullLiteral, true);
                    isNullPredicatePartitionColumnFilters.put(key, isNullPredicatePartitionColumnFilter);
                }
            }
        }
    }

    private boolean isSlotExprMeetingPartitionPruning(Expr slotExpr) {
        for (Expr child: slotExpr.getChildren()) {
            if (!isSupportedExpr(child)) {
                return false;
            }
        }
        return isSupportedExpr(slotExpr);
    }

    private boolean isSupportedExpr(Expr expr) {
        final String exprClassName = expr.getClass().getSimpleName();
        if (!supportedExprs.containsKey(exprClassName)) {
            return false;
        }

        if (supportedExprs.get(exprClassName) == null) {
            return true;
        }

        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr function = (FunctionCallExpr)expr;
            String functionName = function.getFnName().getFunction();
            if (supportedExprs.get(exprClassName).contains(functionName)) {
                return true;
            }
            return false;
        }

        if (expr instanceof ArithmeticExpr) {
            ArithmeticExpr arithmeticExpr = (ArithmeticExpr)expr;
            if (supportedExprs.get(exprClassName).contains(arithmeticExpr.getOp().getName())) {
                return true;
            }
            return false;
        }

        return true;
    }

    private Collection<Long> pruneRangePartitions(PartitionInfo partitionInfo) throws AnalysisException {
        BaseTableRef baseTableRef = (BaseTableRef) tupleDesc.getRef();
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
        Map<Long, Range<PartitionKey>> keyRangeById = rangePartitionInfo.shallowCloneRange();
        Set<Long> partitionIds = Sets.newHashSet();
        retainAndPrunePartitions(partitionIds, binaryColumnFilters, rangePartitionInfo, keyRangeById);
        retainAndPrunePartitions(partitionIds, inPredicatePartitionColumnFilters, rangePartitionInfo, keyRangeById);
        retainAndPrunePartitions(partitionIds, isNullPredicatePartitionColumnFilters, rangePartitionInfo, keyRangeById);
        if (filterEmptyPartition) {
            return partitionIds.stream().filter(id -> olapTable.getPartition(id).hasData() && filterEmptyPartition).collect(Collectors.toList());
        }
        return partitionIds;
    }

    private void retainAndPrunePartitions(
            Set<Long> partitionIds,
            Multimap<String, PartitionColumnFilter> partitionColumnFilters,
            RangePartitionInfo rangePartitionInfo,
            Map<Long, Range<PartitionKey>> keyRangeById) throws AnalysisException {
        for (String name : partitionColumnFilters.keySet()) {
            for (PartitionColumnFilter columnFilter : partitionColumnFilters.get(name)) {
                final Collection<Long> tmpPartitionIds =
                        prunePartitions(name, columnFilter, rangePartitionInfo, keyRangeById);
                if (partitionIds.size() > 0) {
                    partitionIds.retainAll(tmpPartitionIds);
                } else {
                    partitionIds.addAll(tmpPartitionIds);
                }
            }
        }
    }

    private Collection<Long> prunePartitions(String name,
            PartitionColumnFilter partitionColumnFilter, RangePartitionInfo rangePartitionInfo,
            Map<Long, Range<PartitionKey>> keyRangeById) throws AnalysisException {
        // Set column expr.
        for (Range<PartitionKey> range : keyRangeById.values()) {
            final PartitionKey lower = range.lowerEndpoint();
            if (lower != null) {
                lower.setColumnExpr(partitionColumnFilter.getColumnExpr());
                // Test whether the column expr is supported by FE.
                lower.getPartitionValue(0);
            }
            final PartitionKey upper = range.lowerEndpoint();
            if (upper != null) {
                upper.setColumnExpr(partitionColumnFilter.getColumnExpr());
                // Test whether the column expr is supported by FE.
                upper.getPartitionValue(0);
            }
        }
        // Pruning.
        final Map<String, PartitionColumnFilter> columnFilters = Maps.newHashMap();
        columnFilters.put(name, partitionColumnFilter);
        PartitionPruner partitionPruner = new RangePartitionPruner(keyRangeById,
                rangePartitionInfo.getPartitionColumns(),
                columnFilters);
        return partitionPruner.prune();
    }

    private void retainAndPruneTablets(Set<Long> partitionIds,
                                    Multimap<String, PartitionColumnFilter> partitionColumnFilters,
                                    HashDistributionInfo hashDistributionInfo,
                                    MaterializedIndex table) throws AnalysisException {
        if (partitionColumnFilters != null && partitionColumnFilters.size() > 0) {
            for (String key : partitionColumnFilters.keySet()) {
                for (PartitionColumnFilter columnFilter : partitionColumnFilters.get(key)) {
                    final Map<String, PartitionColumnFilter> columnFilters = Maps.newHashMap();
                    columnFilters.put(key, columnFilter);
                    DistributionPruner pruneTabletsr = new HashDistributionPruner(table.getTabletIdsInOrder(),
                            hashDistributionInfo.getDistributionColumns(),
                            columnFilters,
                            hashDistributionInfo.getBucketNum());
                    final Collection<Long> tmpPartitionIds = pruneTabletsr.prune();
                    if (partitionIds.size() > 0) {
                        partitionIds.retainAll(tmpPartitionIds);
                    } else {
                        partitionIds.addAll(tmpPartitionIds);
                    }
                }
            }
        }
    }

    public Collection<Long> getPartitionIds(PartitionInfo partitionInfo) {
        final Collection<Long> partitionIds = Lists.newArrayList();
        for (Partition partition : olapTable.getPartitions()) {
            if (!partition.hasData() && filterEmptyPartition) {
                continue;
            }
            partitionIds.add(partition.getId());
        }
        return partitionIds;
    }

    private Collection<Long> pruneTablets(
            MaterializedIndex table, DistributionInfo distributionInfo) throws AnalysisException {
        Preconditions.checkArgument(
                distributionInfo.getType() == DistributionInfo.DistributionInfoType.HASH);
        final Set<Long> tabletIds = Sets.newHashSet();
        final HashDistributionInfo hashDistributionInfo = (HashDistributionInfo)distributionInfo;
        retainAndPruneTablets(tabletIds, binaryColumnFilters, hashDistributionInfo, table);
        retainAndPruneTablets(tabletIds, inPredicatePartitionColumnFilters, hashDistributionInfo, table);
        retainAndPruneTablets(tabletIds, isNullPredicatePartitionColumnFilters, hashDistributionInfo, table);
        return tabletIds;
    }

    private void setTabletReplicasLocation(
            Partition partition, MaterializedIndex index, List<Tablet> tablets,
            SelectedPartitionRange selectedPartition) throws UserException  {
        final Map<Long, Integer> tabletId2BucketSeq = Maps.newHashMap();
        final List<Long> allTabletIds = index.getTabletIdsInOrder();
        for (int i = 0; i < allTabletIds.size(); i++) {
            tabletId2BucketSeq.put(allTabletIds.get(i), i);
        }

        final int schemaHash = olapTable.getSchemaHashByIndexId(index.getId());
        final String schemaHashStr = String.valueOf(schemaHash);
        final long visibleVersion = partition.getVisibleVersion();
        final long visibleVersionHash = partition.getVisibleVersionHash();
        final String visibleVersionStr = String.valueOf(visibleVersion);
        final String visibleVersionHashStr = String.valueOf(partition.getVisibleVersionHash());

        long localBeId = -1;
        if (Config.enable_local_replica_selection) {
            localBeId = Catalog.getCurrentSystemInfo().getBackendIdByHost(FrontendOptions.getLocalHostAddress());
        }
        int logNum = 0;
        for (Tablet tablet : tablets) {
            final long tabletId = tablet.getId();
            LOG.debug("{} tabletId={}", (logNum++), tabletId);
            TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

            TPaloScanRange paloRange = new TPaloScanRange();
            paloRange.setDb_name("");
            paloRange.setSchema_hash(schemaHashStr);
            paloRange.setVersion(visibleVersionStr);
            paloRange.setVersion_hash(visibleVersionHashStr);
            paloRange.setTablet_id(tabletId);

            // random shuffle List && only collect one copy
            List<Replica> allQueryableReplicas = Lists.newArrayList();
            List<Replica> localReplicas = Lists.newArrayList();
            tablet.getQueryableReplicas(allQueryableReplicas, localReplicas,
                    visibleVersion, visibleVersionHash, localBeId, schemaHash);
            if (allQueryableReplicas.isEmpty()) {
                LOG.error("no queryable replica found in tablet {}. visible version {}-{}",
                        tabletId, visibleVersion, visibleVersionHash);
                if (LOG.isDebugEnabled()) {
                    for (Replica replica : tablet.getReplicas()) {
                        LOG.debug("tablet {}, replica: {}", tabletId, replica.toString());
                    }
                }
                throw new UserException("Failed to get scan range, no queryable replica found in tablet: " + tabletId);
            }

            List<Replica> replicas;
            if (!localReplicas.isEmpty()) {
                replicas = localReplicas;
            } else {
                replicas = allQueryableReplicas;
            }

            Collections.shuffle(replicas);
            boolean tabletIsNull = true;
            boolean collectedStat = false;
            for (Replica replica : replicas) {
                final Backend backend = Catalog.getCurrentSystemInfo().getBackend(replica.getBackendId());
                if (backend == null) {
                    LOG.debug("replica {} not exists", replica.getBackendId());
                    continue;
                }
                final String ip = backend.getHost();
                final int port = backend.getBePort();
                TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress(ip, port));
                scanRangeLocation.setBackend_id(replica.getBackendId());
                scanRangeLocations.addToLocations(scanRangeLocation);
                paloRange.addToHosts(new TNetworkAddress(ip, port));
                tabletIsNull = false;

                //for CBO
                if (!collectedStat && replica.getRowCount() != -1) {
                    selectedPartition.cardinality += replica.getRowCount();
                    selectedPartition.totalBytes += replica.getDataSize();
                    collectedStat = true;
                }
                selectedPartition.scanBackendIds.add(backend.getId());
            }
            if (tabletIsNull) {
                throw new UserException(tabletId + " have no alive replicas");
            }
            TScanRange scanRange = new TScanRange();
            scanRange.setPalo_scan_range(paloRange);
            scanRangeLocations.setScan_range(scanRange);

            selectedPartition.bucketSeq2locations.put(tabletId2BucketSeq.get(tabletId), scanRangeLocations);
            selectedPartition.result.add(scanRangeLocations);
        }
    }

    private boolean isPruningPartitions() {
        if (!(olapTable.getPartitionInfo() instanceof RangePartitionInfo)) {
            return false;
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo)olapTable.getPartitionInfo();
        final String partitionColName = rangePartitionInfo.getPartitionColumns().get(0).getName();
        return binaryColumnFilters.get(partitionColName).size() > 0
                || inPredicatePartitionColumnFilters.get(partitionColName).size() > 0
                || isNullPredicatePartitionColumnFilters.get(partitionColName).size() > 0;
    }

    private boolean isPruningTablets(DistributionInfo info) {
        if (info.getType() != DistributionInfo.DistributionInfoType.HASH) {
            return false;
        }
        final String distributionColName = ((HashDistributionInfo)info).getDistributionColumns().get(0).getName();
        return binaryColumnFilters.get(distributionColName).size() > 0
                || inPredicatePartitionColumnFilters.get(distributionColName).size() > 0
                || isNullPredicatePartitionColumnFilters.get(distributionColName).size() > 0;
    }

    public SelectedPartitionRange selectBestPartitionRange(
            Analyzer analyzer, boolean isPreAggregation) throws UserException {

        // 1.Prune partitions.
        long start = System.currentTimeMillis();
        Collection<Long> partitionIds;
        try {
            if (!isPruningPartitions()) {
                partitionIds = Lists.newArrayList();
                partitionIds.addAll(getPartitionIds(olapTable.getPartitionInfo()));
            } else {
                partitionIds = pruneRangePartitions(olapTable.getPartitionInfo());
            }
        } catch (AnalysisException e) {
            // FE can't support column expr, so partition pruning fails.
            LOG.info(e.getMessage());
            partitionIds = getPartitionIds(olapTable.getPartitionInfo());
        }

        SelectedPartitionRange selectedPartitionRange = new SelectedPartitionRange();
        selectedPartitionRange.setSelectedPartitionNum(partitionIds.size());
        LOG.debug("Partition prune cost: {} ms, partitions: {}",
                (System.currentTimeMillis() - start), partitionIds);

        if (partitionIds.size() == 0) {
            return selectedPartitionRange;
        } 

        // 2.Select rollups.
        start = System.currentTimeMillis();
        final RollupSelector rollupSelector = new RollupSelector(analyzer, tupleDesc, olapTable);
        selectedPartitionRange.selectedRollupInfo =
                rollupSelector.selectBestRollup(partitionIds, predicates, isPreAggregation);
        LOG.debug("Rollup select cost: {} ms, index id: {}",
                (System.currentTimeMillis() - start), selectedPartitionRange.selectedRollupInfo.getSelectedRollupId());

        // 3.Select tablets.
        start = System.currentTimeMillis();
        for (Long partitionId : partitionIds) {
            Partition partition = olapTable.getPartition(partitionId);
            MaterializedIndex rollupIndex = partition.getIndex(
                    selectedPartitionRange.selectedRollupInfo.getSelectedRollupId());
            List<Tablet> tablets = Lists.newArrayList();
            Collection<Long> tabletIds;
            if (!isPruningTablets(partition.getDistributionInfo())) { 
                tabletIds = Lists.newArrayList();
                rollupIndex.getTablets().forEach(tablet -> tabletIds.add(tablet.getId())); 
            } else {
                tabletIds = pruneTablets(rollupIndex, partition.getDistributionInfo());
            }
            LOG.debug("distribution prune tablets: {}", tabletIds);

            if (tabletIds != null) {
                tabletIds.stream().forEach(id->tablets.add(rollupIndex.getTablet(id)));
            } else {
                tablets.addAll(rollupIndex.getTablets());
            }

            selectedPartitionRange.addTotalTabletsNum(rollupIndex.getTablets().size());
            selectedPartitionRange.addSelectedTabletsNum(tablets.size());
            setTabletReplicasLocation(partition, rollupIndex, tablets, selectedPartitionRange);
        }
        LOG.debug("distribution prune cost: {} ms", (System.currentTimeMillis() - start));

        return selectedPartitionRange;
    }

    public static class SelectedPartitionRange {
        private long cardinality;
        private long totalBytes;
        private long selectedTabletsNum;
        private long totalTabletsNum;
        private RollupSelector.SelectedRolupInfo selectedRollupInfo;
        private int selectedPartitionNum;
        private final HashSet<Long> scanBackendIds;
        public final ArrayListMultimap<Integer, TScanRangeLocations> bucketSeq2locations;
        private List<TScanRangeLocations> result;

        public SelectedPartitionRange() {
            this.cardinality = -1;
            this.totalBytes = 0;
            this.selectedTabletsNum = 0;
            this.totalTabletsNum = 0;
            this.selectedRollupInfo = null;
            this.selectedPartitionNum = 0;
            this.scanBackendIds = Sets.newHashSet();
            this.bucketSeq2locations= ArrayListMultimap.create();
            this.result = Lists.newArrayList();
        }

        public long getCardinality() {
            return cardinality;
        }

        public void setCardinality(long cardinality) {
            this.cardinality = cardinality;
        }

        public long getTotalBytes() {
            return totalBytes;
        }

        public void setTotalBytes(long totalBytes) {
            this.totalBytes = totalBytes;
        }

        public long getSelectedTabletsNum() {
            return selectedTabletsNum;
        }

        public void setSelectedTabletsNum(long selectedTabletsNum) {
            this.selectedTabletsNum = selectedTabletsNum;
        }

        public void addSelectedTabletsNum(long selectedTabletsNum) {
            this.selectedTabletsNum += selectedTabletsNum;
        }

        public int getSelectedPartitionNum() {
            return selectedPartitionNum;
        }

        public void setSelectedPartitionNum(int selectedPartitionNum) {
            this.selectedPartitionNum = selectedPartitionNum;
        }

        public void addTotalTabletsNum(long totalTabletsNum) {
            this.totalTabletsNum += totalTabletsNum;
        } 

        public long getTotalTabletsNum() {
            return totalTabletsNum;
        }

        public void setTotalTabletsNum(long totalTabletsNum) {
            this.totalTabletsNum = totalTabletsNum;
        }

        public RollupSelector.SelectedRolupInfo getSelectedIndexId() {
            return selectedRollupInfo;
        }

        public void setSelectedIndexId(RollupSelector.SelectedRolupInfo selectedRollupInfo) {
            this.selectedRollupInfo = selectedRollupInfo;
        }

        public HashSet<Long> getScanBackendIds() {
            return scanBackendIds;
        }

        public ArrayListMultimap<Integer, TScanRangeLocations> getBucketSeq2locations() {
            return bucketSeq2locations;
        }

        public List<TScanRangeLocations> getResult() {
            return result;
        }
    }
}
