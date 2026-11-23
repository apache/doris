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

package org.apache.doris.statistics.sampling;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.statistics.sampling.TabletSampleSpec.LegacySamplingSemantics;
import org.apache.doris.statistics.sampling.TabletSampleSpec.LegacyTabletScanClause;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/** Original tablet picker. Each method instance belongs to one column task. */
public class LegacyTabletSamplingMethod implements TabletSamplingMethod {
    private static final Logger LOG = LogManager.getLogger(LegacyTabletSamplingMethod.class);
    private static final long MAXIMUM_SAMPLE_ROWS = 1_000_000_000;
    static final long NO_SKIP_TABLET_ID = -1;

    private final OlapTable tbl;
    private final long indexId;
    private final long explicitSeek;
    private boolean keyColumnSampleTooManyRows;
    private boolean partitionColumnSampleTooManyRows;

    public LegacyTabletSamplingMethod(OlapTable table, long indexId, long explicitSeek) {
        this.tbl = table;
        this.indexId = indexId;
        this.explicitSeek = explicitSeek;
    }

    @Override
    public TabletSampleSpec build(TabletSamplingRequest request) {
        boolean forceDuj1 = DebugPointUtil.isEnable("OlapAnalysisTask.useDUJ1Template");
        long target = request.budget().targetRows();
        long population = request.populationRows();
        if (!forceDuj1 && population <= target) {
            return TabletSampleSpec.fullScan();
        }
        keyColumnSampleTooManyRows = false;
        partitionColumnSampleTooManyRows = false;
        Pair<List<Long>, Long> picked;
        tbl.readLock();
        try {
            picked = getSampleTablets(request);
        } finally {
            tbl.readUnlock();
        }
        if (!forceDuj1 && picked.second < target) {
            return TabletSampleSpec.fullScan();
        }
        ColumnPhysicalLayout layout = request.layout();
        long limit = 0;
        if (!layout.partition() && (!layout.key() || keyColumnSampleTooManyRows)) {
            limit = keyColumnSampleTooManyRows ? MAXIMUM_SAMPLE_ROWS : Math.min(target, picked.second);
        }
        boolean linear = !forceDuj1 && (partitionColumnSampleTooManyRows || layout.singleUniqueKey()
                || layout.distribution());
        double scale = (double) population / (limit > 0 ? limit : picked.second);
        return new TabletSampleSpec(new LegacyTabletScanClause(picked.first),
                new LegacySamplingSemantics(linear, scale, limit));
    }

    /**
     * Select the tablets to read.
     * @return Pair of tablet id list and how many rows are going to read.
     */
    protected Pair<List<Long>, Long> getSampleTablets(TabletSamplingRequest request) {
        Random random = new Random(request.seed());
        long targetSampleRows = request.budget().targetRows();
        OlapTable olapTable = tbl;
        boolean forPartitionColumn = request.layout().partition();
        long avgTargetRowsPerPartition = targetSampleRows / Math.max(olapTable.getPartitions().size(), 1);
        List<Long> sampleTabletIds = new ArrayList<>();
        long selectedRows = 0;
        boolean enough = false;
        // Sort the partitions to get stable result.
        List<Partition> sortedPartitions = olapTable.getPartitions().stream().sorted(
                Comparator.comparing(Partition::getName)).collect(Collectors.toList());
        long largeTabletId = 0;
        long largeTabletRows = Long.MAX_VALUE;
        for (Partition p : sortedPartitions) {
            MaterializedIndex materializedIndex = indexId == -1 ? p.getBaseIndex() : p.getIndex(indexId);
            if (materializedIndex == null) {
                continue;
            }
            List<Long> ids = materializedIndex.getTabletIdsInOrder();
            if (ids.isEmpty()) {
                continue;
            }
            long avgRowsPerTablet = Math.max(materializedIndex.getRowCount() / ids.size(), 1);
            long tabletCounts = Math.max(avgTargetRowsPerPartition / avgRowsPerTablet
                    + (avgTargetRowsPerPartition % avgRowsPerTablet != 0 ? 1 : 0), 1);
            tabletCounts = Math.min(tabletCounts, ids.size());
            long seek = explicitSeek >= 0 ? explicitSeek % ids.size() : random.nextInt(ids.size());
            for (int i = 0; i < tabletCounts; i++) {
                int seekTid = (int) ((i + seek) % ids.size());
                long tabletId = ids.get(seekTid);
                // for local mode, getCachedVisibleVersion return visibleVersion.
                // for cloud mode, the replica.checkVersionCatchUp always returns true.
                long tabletRows = materializedIndex.getTablet(tabletId)
                        .getMinReplicaRowCount(p.getCachedVisibleVersion());
                if (tabletRows > MAXIMUM_SAMPLE_ROWS) {
                    LOG.debug("Found one large tablet id {} in table {}, rows {}",
                            largeTabletId, tbl.getName(), largeTabletRows);
                    // Skip very large tablet and record the smallest large tablet id and row count.
                    if (tabletRows < largeTabletRows) {
                        LOG.debug("Current smallest large tablet id {} in table {}, rows {}",
                                largeTabletId, tbl.getName(), largeTabletRows);
                        largeTabletId = tabletId;
                        largeTabletRows = tabletRows;
                    }
                    continue;
                }
                sampleTabletIds.add(tabletId);
                if (tabletRows > 0) {
                    selectedRows += tabletRows;
                    // For regular column, will stop adding more tablets when selected tablets'
                    // row count is more than the target sample rows.
                    // But for partition columns, will not stop adding. For ndv sample accuracy,
                    // better to choose at least one tablet in each partition.
                    if (selectedRows >= targetSampleRows && !forPartitionColumn) {
                        enough = true;
                        break;
                    }
                }
            }
            if (enough) {
                break;
            }
        }
        // If we skipped some large tablets and this cause the sampled rows is not enough, we add the large tablet back.
        if (!enough && largeTabletId != 0) {
            sampleTabletIds.add(largeTabletId);
            selectedRows += largeTabletRows;
            LOG.info("Add large tablet {} in table {} back, with rows {}",
                    largeTabletId, tbl.getName(), largeTabletRows);
        }
        // If sampled rows are not enough, the caller falls back to a full table scan.
        if (forPartitionColumn && selectedRows > MAXIMUM_SAMPLE_ROWS) {
            // If the selected tablets for partition column contain too many rows, change to linear sample.
            partitionColumnSampleTooManyRows = true;
            sampleTabletIds.clear();
            Collections.shuffle(sortedPartitions, random);
            selectedRows = pickSamplePartition(sortedPartitions, sampleTabletIds, getSkipPartitionId(sortedPartitions));
        } else if (request.layout().key() && selectedRows > MAXIMUM_SAMPLE_ROWS) {
            // For key column, if a single tablet contains too many rows, need to use limit to control rows to read.
            // In most cases, a single tablet shouldn't contain more than MAXIMUM_SAMPLE_ROWS, in this case, we
            // don't use limit for key column for ndv accuracy reason.
            keyColumnSampleTooManyRows = true;
        }
        return Pair.of(sampleTabletIds, selectedRows);
    }

    // For partition tables with single time type partition column, we'd better to skip sampling the partition
    // that contains all the history data. Because this partition may contain many old data which is not
    // visited by most queries. To sample this partition may cause the statistics not accurate.
    // For example, one table has 366 partitions, partition 1 ~ 365 store date for each day of the year from now.
    // Partition 0 stores all the history data earlier than 1 year. We want to skip sampling partition 0.
    protected long getSkipPartitionId(List<Partition> partitions) {
        if (partitions == null || partitions.size() < StatisticsUtil.getPartitionSampleCount()) {
            return NO_SKIP_TABLET_ID;
        }
        PartitionInfo partitionInfo = tbl.getPartitionInfo();
        if (!PartitionType.RANGE.equals(partitionInfo.getType())) {
            return NO_SKIP_TABLET_ID;
        }
        if (partitionInfo.getPartitionColumns().size() != 1) {
            return NO_SKIP_TABLET_ID;
        }
        Column column = partitionInfo.getPartitionColumns().get(0);
        if (!column.getType().isDateType()) {
            return NO_SKIP_TABLET_ID;
        }
        PartitionKey lowestKey = PartitionKey.createMaxPartitionKey();
        long lowestPartitionId = -1;
        for (Partition p : partitions) {
            RangePartitionItem item = (RangePartitionItem) partitionInfo.getItem(p.getId());
            Range<PartitionKey> items = item.getItems();
            if (!items.hasLowerBound()) {
                lowestPartitionId = p.getId();
                break;
            }
            if (items.lowerEndpoint().compareTo(lowestKey) < 0) {
                lowestKey = items.lowerEndpoint();
                lowestPartitionId = p.getId();
            }
        }
        return lowestPartitionId;
    }

    protected long pickSamplePartition(List<Partition> partitions, List<Long> pickedTabletIds, long skipPartitionId) {
        Partition partition = tbl.getPartition(skipPartitionId);
        long averageRowsPerPartition;
        if (partition != null) {
            LOG.debug("Going to skip partition {} in table {}", skipPartitionId, tbl.getName());
            // If we want to skip the oldest partition, calculate the average rows per partition value without
            // the oldest partition, otherwise if the oldest partition is very large, we may skip all partitions.
            // Because we only pick partitions which meet partitionRowCount >= averageRowsPerPartition.
            Preconditions.checkNotNull(partitions, "Partition list of table " + tbl.getName() + " is null");
            Preconditions.checkState(partitions.size() > 1, "Too few partitions in " + tbl.getName());
            averageRowsPerPartition = (tbl.getRowCount() - partition.getRowCount()) / (partitions.size() - 1);
        } else {
            averageRowsPerPartition = tbl.getRowCount() / partitions.size();
        }
        long selectedIndex = indexId == -1 ? tbl.getBaseIndexId() : indexId;
        long pickedRows = 0;
        int pickedPartitionCount = 0;
        for (Partition p : partitions) {
            if (skipPartitionId == p.getId()) {
                LOG.info("Partition {} in table {} skipped", skipPartitionId, tbl.getName());
                continue;
            }
            long partitionRowCount = p.getRowCount();
            if (partitionRowCount >= averageRowsPerPartition) {
                pickedRows += partitionRowCount;
                pickedPartitionCount++;
                MaterializedIndex materializedIndex = p.getIndex(selectedIndex);
                pickedTabletIds.addAll(materializedIndex.getTabletIdsInOrder());
            }
            if (pickedRows >= StatisticsUtil.getPartitionSampleRowCount()
                    || pickedPartitionCount >= StatisticsUtil.getPartitionSampleCount()) {
                break;
            }
        }
        return pickedRows;
    }

}
