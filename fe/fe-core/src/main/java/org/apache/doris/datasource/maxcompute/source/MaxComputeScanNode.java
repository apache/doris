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

package org.apache.doris.datasource.maxcompute.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.planner.ListPartitionPrunerV2;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TMaxComputeFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.aliyun.odps.Table;
import com.aliyun.odps.tunnel.TunnelException;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MaxComputeScanNode extends FileQueryScanNode {

    private final MaxComputeExternalTable table;
    private final MaxComputeExternalCatalog catalog;
    public static final int MIN_SPLIT_SIZE = 4096;

    public MaxComputeScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        this(id, desc, "MCScanNode", StatisticalType.MAX_COMPUTE_SCAN_NODE, needCheckColumnPriv);
    }

    public MaxComputeScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
                              StatisticalType statisticalType, boolean needCheckColumnPriv) {
        super(id, desc, planNodeName, statisticalType, needCheckColumnPriv);
        table = (MaxComputeExternalTable) desc.getTable();
        catalog = (MaxComputeExternalCatalog) table.getCatalog();
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof MaxComputeSplit) {
            setScanParams(rangeDesc, (MaxComputeSplit) split);
        }
    }

    public void setScanParams(TFileRangeDesc rangeDesc, MaxComputeSplit maxComputeSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(TableFormatType.MAX_COMPUTE.value());
        TMaxComputeFileDesc fileDesc = new TMaxComputeFileDesc();
        if (maxComputeSplit.getPartitionSpec().isPresent()) {
            fileDesc.setPartitionSpec(maxComputeSplit.getPartitionSpec().get());
        }
        tableFormatFileDesc.setMaxComputeParams(fileDesc);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    @Override
    protected TFileType getLocationType() throws UserException {
        return getLocationType(null);
    }

    @Override
    protected TFileType getLocationType(String location) throws UserException {
        return TFileType.FILE_NET;
    }

    @Override
    public TFileFormatType getFileFormatType() {
        return TFileFormatType.FORMAT_JNI;
    }

    @Override
    public List<String> getPathPartitionKeys() {
        return Collections.emptyList();
    }

    @Override
    protected TableIf getTargetTable() throws UserException {
        return table;
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException {
        return new HashMap<>();
    }

    @Override
    public List<Split> getSplits() throws UserException {
        List<Split> result = new ArrayList<>();
        com.aliyun.odps.Table odpsTable = table.getOdpsTable();
        if (desc.getSlots().isEmpty() || odpsTable.getFileNum() <= 0) {
            return result;
        }
        try {
            if (!table.getPartitionNames().isEmpty()) {
                if (conjuncts.isEmpty()) {
                    throw new IllegalArgumentException("Max Compute partition table need partition predicate.");
                }
                List<String> partitionSpecs = getPartitionSpecs();
                for (String partitionSpec : partitionSpecs) {
                    addPartitionSplits(result, odpsTable, partitionSpec);
                }
            } else {
                addBatchSplits(result, odpsTable, table.getTotalRows());
            }
        } catch (TunnelException e) {
            throw new UserException("Max Compute tunnel SDK exception: " + e.getMessage(), e);

        }
        return result;
    }

    private static void addPartitionSplits(List<Split> result, Table odpsTable, String partitionSpec) {
        long modificationTime = odpsTable.getLastDataModifiedTime().getTime();
        // use '-1' to read whole partition, avoid expending too much time on calling table.getTotalRows()
        Pair<Long, Long> range = Pair.of(0L, -1L);
        FileSplit rangeSplit = new FileSplit(new Path("/virtual_slice_part"),
                range.first, range.second, -1, modificationTime, null, Collections.emptyList());
        result.add(new MaxComputeSplit(partitionSpec, rangeSplit));
    }

    private static void addBatchSplits(List<Split> result, Table odpsTable, long totalRows) {
        List<Pair<Long, Long>> sliceRange = new ArrayList<>();
        long fileNum = odpsTable.getFileNum();
        long start = 0;
        long splitSize = (long) Math.ceil((double) totalRows / fileNum);
        if (splitSize <= 0 || totalRows < MIN_SPLIT_SIZE) {
            // use whole split
            sliceRange.add(Pair.of(start, totalRows));
        } else {
            for (int i = 0; i < fileNum; i++) {
                if (start > totalRows) {
                    break;
                }
                sliceRange.add(Pair.of(start, splitSize));
                start += splitSize;
            }
        }
        long modificationTime = odpsTable.getLastDataModifiedTime().getTime();
        if (!sliceRange.isEmpty()) {
            for (int i = 0; i < sliceRange.size(); i++) {
                Pair<Long, Long> range = sliceRange.get(i);
                FileSplit rangeSplit = new FileSplit(new Path("/virtual_slice_" + i),
                        range.first, range.second, totalRows, modificationTime, null, Collections.emptyList());
                result.add(new MaxComputeSplit(rangeSplit));
            }
        }
    }

    private List<String> getPartitionSpecs() throws AnalysisException {
        return getPrunedPartitionSpecs();
    }

    private List<String> getPrunedPartitionSpecs() throws AnalysisException {
        List<String> result = new ArrayList<>();
        TablePartitionValues partitionValues = table.getPartitionValues();
        // prune partitions by expr
        partitionValues.readLock().lock();
        try {
            Map<Long, PartitionItem> idToPartitionItem = partitionValues.getIdToPartitionItem();
            this.totalPartitionNum = idToPartitionItem.size();
            ListPartitionPrunerV2 pruner = new ListPartitionPrunerV2(idToPartitionItem,
                    table.getPartitionColumns(), columnNameToRange,
                    partitionValues.getUidToPartitionRange(),
                    partitionValues.getRangeToId(),
                    partitionValues.getSingleColumnRangeMap(),
                    false);
            Collection<Long> filteredPartitionIds = pruner.prune();
            this.selectedPartitionNum = filteredPartitionIds.size();
            // get partitions from cache
            Map<Long, String> partitionIdToNameMap = partitionValues.getPartitionIdToNameMap();
            filteredPartitionIds.forEach(id -> result.add(partitionIdToNameMap.get(id)));
            return result;
        } finally {
            partitionValues.readLock().unlock();
        }
    }
}
