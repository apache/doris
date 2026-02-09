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
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanNode;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TPushAggOp;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Base class for External File Scan, including external query and load.
 */
public abstract class FileScanNode extends ExternalScanNode {
    // For explain
    protected long totalFileSize = 0;
    protected long totalPartitionNum = 0;
    // For display pushdown agg result
    protected long tableLevelRowCount = -1;

    public FileScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName, boolean needCheckColumnPriv) {
        super(id, desc, planNodeName, needCheckColumnPriv);
        this.needCheckColumnPriv = needCheckColumnPriv;
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

    protected void setPushDownCount(long count) {
        tableLevelRowCount = count;
    }

    private long getPushDownCount() {
        return tableLevelRowCount;
    }

    public long getTotalFileSize() {
        return totalFileSize;
    }

    /**
     * Get all delete files for the given file range.
     * @param rangeDesc the file range descriptor
     * @return list of delete file paths (formatted strings)
     */
    protected List<String> getDeleteFiles(TFileRangeDesc rangeDesc) {
        // Default implementation: return empty list
        // Subclasses should override this method
        return Collections.emptyList();
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("table: ").append(desc.getTable().getNameWithFullQualifiers()).append("\n");
        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("predicates: ").append(getExplainString(conjuncts)).append("\n");
        }
        if (!runtimeFilters.isEmpty()) {
            output.append(prefix).append("runtime filters: ");
            output.append(getRuntimeFilterExplainString());
        }

        output.append(prefix);
        boolean isBatch = isBatchMode();
        if (isBatch) {
            output.append("(approximate)");
        }
        output.append("inputSplitNum=").append(selectedSplitNum).append(", totalFileSize=")
                .append(totalFileSize).append(", scanRanges=").append(scanRangeLocations.size()).append("\n");
        output.append(prefix).append("partition=").append(selectedPartitionNum).append("/").append(totalPartitionNum)
                .append("\n");

        if (detailLevel == TExplainLevel.VERBOSE && !isBatch) {
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

                // A Data file may be divided into different splits, so a set is used to remove duplicates.
                Set<String> dataFilesSet = new HashSet<>();
                // A delete file might be used by multiple data files, so use set to remove duplicates.
                Set<String> deleteFilesSet = new HashSet<>();
                // You can estimate how many delete splits need to be read for a data split
                // using deleteSplitNum / dataSplitNum(fileRangeDescs.size()) split.
                long deleteSplitNum = 0;
                for (TFileRangeDesc fileRangeDesc : fileRangeDescs) {
                    dataFilesSet.add(fileRangeDesc.getPath());
                    List<String> deletefiles =  getDeleteFiles(fileRangeDesc);
                    deleteFilesSet.addAll(deletefiles);
                    deleteSplitNum += deletefiles.size();
                }

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
                output.append(prefix).append("    ").append("dataFileNum=").append(dataFilesSet.size())
                        .append(", deleteFileNum=").append(deleteFilesSet.size())
                        .append(", deleteSplitNum=").append(deleteSplitNum)
                        .append("\n");
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

        printNestedColumns(output, prefix, getTupleDesc());

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

        Map<String, SlotDescriptor> nameToSlotDesc = Maps.newLinkedHashMap();
        for (SlotDescriptor slot : desc.getSlots()) {
            nameToSlotDesc.put(slot.getColumn().getName(), slot);
        }

        for (Column column : getColumns()) {
            Expr expr;
            Expression expression;
            if (column.getDefaultValue() != null) {
                expression = new NereidsParser().parseExpression(
                        column.getDefaultValueSql());
                ExpressionAnalyzer analyzer = new ExpressionAnalyzer(
                        null, new Scope(ImmutableList.of()), null, true, true);
                expression = analyzer.analyze(expression);
            } else {
                if (column.isAllowNull()) {
                    // For load, use Varchar as Null, for query, use column type.
                    if (useVarcharAsNull) {
                        expression = new NullLiteral(VarcharType.SYSTEM_DEFAULT);
                    } else {
                        SlotDescriptor slotDescriptor = nameToSlotDesc.get(column.getName());
                        // the nested type(map/array/struct) maybe pruned,
                        // we should use the pruned type to avoid be core
                        expression = new NullLiteral(DataType.fromCatalogType(
                                slotDescriptor == null ? column.getType() : slotDescriptor.getType()
                        ));
                    }
                } else {
                    expression = null;
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
            // c does not exist in file, the z will be filled with null, even if z has
            // default value.
            // and if z is not nullable, the load will fail.
            if (slotDesc != null) {
                if (expression != null) {
                    expression = TypeCoercionUtils.castIfNotSameType(expression,
                            DataType.fromCatalogType(slotDesc.getType()));
                    expr = ExpressionTranslator.translate(expression,
                            new PlanTranslatorContext(CascadesContext.initTempContext()));
                    params.putToDefaultValueOfSrcSlot(slotDesc.getId().asInt(), expr.treeToThrift());
                } else {
                    params.putToDefaultValueOfSrcSlot(slotDesc.getId().asInt(), tExpr);
                }
            }
        }
    }
}
