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

package org.apache.doris.datasource;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.common.util.Util;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanNode;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TPushAggOp;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base class for External File Scan, including external query and load.
 */
public abstract class FileScanNode extends ExternalScanNode {
    private static final Logger LOG = LogManager.getLogger(FileScanNode.class);

    public static final long DEFAULT_SPLIT_SIZE = 64 * 1024 * 1024; // 64MB

    // For explain
    protected long totalFileSize = 0;
    protected long totalPartitionNum = 0;
    protected long fileSplitSize;
    protected boolean isSplitSizeSetBySession = false;

    public FileScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName, StatisticalType statisticalType,
            boolean needCheckColumnPriv) {
        super(id, desc, planNodeName, statisticalType, needCheckColumnPriv);
        this.needCheckColumnPriv = needCheckColumnPriv;
    }

    @Override
    public void init() throws UserException {
        initFileSplitSize();
    }

    private void initFileSplitSize() {
        this.fileSplitSize = ConnectContext.get().getSessionVariable().getFileSplitSize();
        this.isSplitSizeSetBySession = this.fileSplitSize > 0;
        if (this.fileSplitSize <= 0) {
            this.fileSplitSize = DEFAULT_SPLIT_SIZE;
        }
    }

    @Override
    protected void toThrift(TPlanNode planNode) {
        planNode.setPushDownAggTypeOpt(pushDownAggNoGroupingOp);

        planNode.setNodeType(TPlanNodeType.FILE_SCAN_NODE);
        TFileScanNode fileScanNode = new TFileScanNode();
        fileScanNode.setTupleId(desc.getId().asInt());
        if (desc.getTable() != null) {
            fileScanNode.setTableName(desc.getTable().getName());
        }
        planNode.setFileScanNode(fileScanNode);
        super.toThrift(planNode);
    }

    public long getPushDownCount() {
        // 1. Do not use `0`: If the number of entries in the table is 0,
        //                    it is unclear whether optimization has been performed.
        // 2. Do not use `null` or `-`: This makes it easier for the program to parse the `explain` data.
        return -1;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("table: ").append(desc.getTable().getName()).append("\n");
        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("predicates: ").append(getExplainString(conjuncts)).append("\n");
        }
        if (!runtimeFilters.isEmpty()) {
            output.append(prefix).append("runtime filters: ");
            output.append(getRuntimeFilterExplainString(false));
        }

        output.append(prefix);
        if (isBatchMode()) {
            output.append("(approximate)");
        }
        output.append("inputSplitNum=").append(selectedSplitNum).append(", totalFileSize=")
            .append(totalFileSize).append(", scanRanges=").append(scanRangeLocations.size()).append("\n");
        output.append(prefix).append("partition=").append(selectedPartitionNum).append("/").append(totalPartitionNum)
            .append("\n");

        if (detailLevel == TExplainLevel.VERBOSE) {
            output.append(prefix).append("backends:").append("\n");
            Multimap<Long, TFileRangeDesc> scanRangeLocationsMap = ArrayListMultimap.create();
            // 1. group by backend id
            for (TScanRangeLocations locations : scanRangeLocations) {
                scanRangeLocationsMap.putAll(locations.getLocations().get(0).backend_id,
                        locations.getScanRange().getExtScanRange().getFileScanRange().getRanges());
            }
            for (long beId : scanRangeLocationsMap.keySet()) {
                output.append(prefix).append("  ").append(beId).append("\n");
                List<TFileRangeDesc> fileRangeDescs = Lists.newArrayList(scanRangeLocationsMap.get(beId));
                // 2. sort by file start offset
                Collections.sort(fileRangeDescs, new Comparator<TFileRangeDesc>() {
                    @Override
                    public int compare(TFileRangeDesc o1, TFileRangeDesc o2) {
                        return Long.compare(o1.getStartOffset(), o2.getStartOffset());
                    }
                });
                // 3. if size <= 4, print all. if size > 4, print first 3 and last 1
                int size = fileRangeDescs.size();
                if (size <= 4) {
                    for (TFileRangeDesc file : fileRangeDescs) {
                        output.append(prefix).append("    ").append(file.getPath())
                            .append(" start: ").append(file.getStartOffset())
                            .append(" length: ").append(file.getSize())
                            .append("\n");
                    }
                } else {
                    for (int i = 0; i < 3; i++) {
                        TFileRangeDesc file = fileRangeDescs.get(i);
                        output.append(prefix).append("    ").append(file.getPath())
                            .append(" start: ").append(file.getStartOffset())
                            .append(" length: ").append(file.getSize())
                            .append("\n");
                    }
                    int other = size - 4;
                    output.append(prefix).append("    ... other ").append(other).append(" files ...\n");
                    TFileRangeDesc file = fileRangeDescs.get(size - 1);
                    output.append(prefix).append("    ").append(file.getPath())
                        .append(" start: ").append(file.getStartOffset())
                        .append(" length: ").append(file.getSize())
                        .append("\n");
                }
            }
        }

        output.append(prefix);
        if (cardinality > 0) {
            output.append(String.format("cardinality=%s, ", cardinality));
        }
        if (avgRowSize > 0) {
            output.append(String.format("avgRowSize=%s, ", avgRowSize));
        }
        output.append(String.format("numNodes=%s", numNodes)).append("\n");

        // pushdown agg
        output.append(prefix).append(String.format("pushdown agg=%s", pushDownAggNoGroupingOp));
        if (pushDownAggNoGroupingOp.equals(TPushAggOp.COUNT)) {
            output.append(" (").append(getPushDownCount()).append(")");
        }
        output.append("\n");

        if (useTopnFilter()) {
            String topnFilterSources = String.join(",",
                    topnFilterSortNodes.stream()
                            .map(node -> node.getId().asInt() + "").collect(Collectors.toList()));
            output.append(prefix).append("TOPN OPT:").append(topnFilterSources).append("\n");
        }
        return output.toString();
    }

    protected void setDefaultValueExprs(TableIf tbl,
                                        Map<String, SlotDescriptor> slotDescByName,
                                        Map<String, Expr> exprByName,
                                        TFileScanRangeParams params,
                                        boolean useVarcharAsNull) throws UserException {
        Preconditions.checkNotNull(tbl);
        TExpr tExpr = new TExpr();
        tExpr.setNodes(Lists.newArrayList());

        for (Column column : tbl.getBaseSchema()) {
            Expr expr;
            if (column.getDefaultValue() != null) {
                if (column.getDefaultValueExprDef() != null) {
                    expr = column.getDefaultValueExpr();
                    expr.analyze(analyzer);
                } else {
                    expr = new StringLiteral(column.getDefaultValue());
                }
            } else {
                if (column.isAllowNull()) {
                    // For load, use Varchar as Null, for query, use column type.
                    if (useVarcharAsNull) {
                        expr = NullLiteral.create(org.apache.doris.catalog.Type.VARCHAR);
                    } else {
                        expr = NullLiteral.create(column.getType());
                    }
                } else {
                    expr = null;
                }
            }
            // if there is already an expr , just skip it.
            // eg:
            // (a, b, c, c=hll_hash(c)) in stream load
            // c will be filled with hll_hash(column c) , don't need to specify it.
            if (exprByName != null && exprByName.containsKey(column.getName())) {
                continue;
            }
            SlotDescriptor slotDesc = slotDescByName.get(column.getName());
            // if slot desc is null, which mean it is an unrelated slot, just skip.
            // eg:
            // (a, b, c) set (x=a, y=b, z=c)
            // c does not exist in file, the z will be filled with null, even if z has default value.
            // and if z is not nullable, the load will fail.
            if (slotDesc != null) {
                if (expr != null) {
                    expr = castToSlot(slotDesc, expr);
                    params.putToDefaultValueOfSrcSlot(slotDesc.getId().asInt(), expr.treeToThrift());
                } else {
                    params.putToDefaultValueOfSrcSlot(slotDesc.getId().asInt(), tExpr);
                }
            }
        }
    }

    protected List<Split> splitFile(LocationPath path, long blockSize, BlockLocation[] blockLocations, long length,
            long modificationTime, boolean splittable, List<String> partitionValues, SplitCreator splitCreator)
            throws IOException {
        if (blockLocations == null) {
            blockLocations = new BlockLocation[0];
        }
        List<Split> result = Lists.newArrayList();
        TFileCompressType compressType = Util.inferFileCompressTypeByPath(path.get());
        if (!splittable || compressType != TFileCompressType.PLAIN) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Path {} is not splittable.", path);
            }
            String[] hosts = blockLocations.length == 0 ? null : blockLocations[0].getHosts();
            result.add(splitCreator.create(path, 0, length, length, modificationTime, hosts, partitionValues));
            return result;
        }
        // if file split size is set by session variable, use session variable.
        // Otherwise, use max(file split size, block size)
        if (!isSplitSizeSetBySession) {
            fileSplitSize = Math.max(fileSplitSize, blockSize);
        }
        long bytesRemaining;
        for (bytesRemaining = length; (double) bytesRemaining / (double) fileSplitSize > 1.1D;
                bytesRemaining -= fileSplitSize) {
            int location = getBlockIndex(blockLocations, length - bytesRemaining);
            String[] hosts = location == -1 ? null : blockLocations[location].getHosts();
            result.add(splitCreator.create(path, length - bytesRemaining, fileSplitSize,
                    length, modificationTime, hosts, partitionValues));
        }
        if (bytesRemaining != 0L) {
            int location = getBlockIndex(blockLocations, length - bytesRemaining);
            String[] hosts = location == -1 ? null : blockLocations[location].getHosts();
            result.add(splitCreator.create(path, length - bytesRemaining, bytesRemaining,
                    length, modificationTime, hosts, partitionValues));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Path {} includes {} splits.", path, result.size());
        }
        return result;
    }

    protected int getBlockIndex(BlockLocation[] blkLocations, long offset) {
        if (blkLocations == null || blkLocations.length == 0) {
            return -1;
        }
        for (int i = 0; i < blkLocations.length; ++i) {
            if (blkLocations[i].getOffset() <= offset
                    && offset < blkLocations[i].getOffset() + blkLocations[i].getLength()) {
                return i;
            }
        }
        BlockLocation last = blkLocations[blkLocations.length - 1];
        long fileLength = last.getOffset() + last.getLength() - 1L;
        throw new IllegalArgumentException(String.format("Offset %d is outside of file (0..%d)", offset, fileLength));
    }
}
