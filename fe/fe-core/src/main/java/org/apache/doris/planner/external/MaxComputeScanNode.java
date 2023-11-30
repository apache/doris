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

package org.apache.doris.planner.external;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.MaxComputeExternalTable;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.MaxComputeExternalCatalog;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;

import com.aliyun.odps.tunnel.TunnelException;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    protected List<Split> getSplits() throws UserException {
        List<Split> result = new ArrayList<>();
        // String splitPath = catalog.getTunnelUrl();
        // TODO: use single max compute scan node rather than file scan node
        com.aliyun.odps.Table odpsTable = table.getOdpsTable();
        if (desc.getSlots().isEmpty() || odpsTable.getFileNum() <= 0) {
            return result;
        }
        try {
            List<Pair<Long, Long>> sliceRange = new ArrayList<>();
            Optional<String> partitionSpec = table.getPartitionSpec(conjuncts);
            long totalRows = catalog.getTotalRows(table.getDbName(), table.getName(), partitionSpec);
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
                    result.add(new FileSplit(new Path("/virtual_slice_" + i), range.first, range.second,
                            totalRows, modificationTime, null, Collections.emptyList()));
                }
            }
        } catch (TunnelException e) {
            throw new UserException("Max Compute tunnel SDK exception.", e);

        }
        return result;
    }
}
