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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.Load;
import org.apache.doris.task.StreamLoadTask;
import org.apache.doris.thrift.TBrokerRangeDesc;
import org.apache.doris.thrift.TBrokerScanNode;
import org.apache.doris.thrift.TBrokerScanRange;
import org.apache.doris.thrift.TBrokerScanRangeParams;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * used to scan from stream
 */
public class StreamLoadScanNode extends LoadScanNode {
    private static final Logger LOG = LogManager.getLogger(StreamLoadScanNode.class);

    private TUniqueId loadId;
    // TODO(zc): now we use scanRange
    // input parameter
    private Table dstTable;
    private StreamLoadTask streamLoadTask;

    // helper
    private Analyzer analyzer;
    private TupleDescriptor srcTupleDesc;
    private TBrokerScanRange brokerScanRange;

    private Map<String, SlotDescriptor> slotDescByName = Maps.newHashMap();
    private Map<String, Expr> exprsByName = Maps.newHashMap();

    // used to construct for streaming loading
    public StreamLoadScanNode(
            TUniqueId loadId, PlanNodeId id, TupleDescriptor tupleDesc, Table dstTable, StreamLoadTask streamLoadTask) {
        super(id, tupleDesc, "StreamLoadScanNode");
        this.loadId = loadId;
        this.dstTable = dstTable;
        this.streamLoadTask = streamLoadTask;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        // can't call super.init(), because after super.init, conjuncts would be null
        assignConjuncts(analyzer);

        this.analyzer = analyzer;
        brokerScanRange = new TBrokerScanRange();

        TBrokerRangeDesc rangeDesc = new TBrokerRangeDesc();
        rangeDesc.file_type = streamLoadTask.getFileType();
        rangeDesc.format_type = streamLoadTask.getFormatType();
        if (rangeDesc.format_type == TFileFormatType.FORMAT_JSON) {
            if (!streamLoadTask.getJsonPaths().isEmpty()) {
                rangeDesc.setJsonpaths(streamLoadTask.getJsonPaths());
            }
            rangeDesc.setStrip_outer_array(streamLoadTask.isStripOuterArray());
        }
        rangeDesc.splittable = false;
        switch (streamLoadTask.getFileType()) {
            case FILE_LOCAL:
                rangeDesc.path = streamLoadTask.getPath();
                break;
            case FILE_STREAM:
                rangeDesc.path = "Invalid Path";
                rangeDesc.load_id = loadId;
                break;
            default:
                throw new UserException("unsupported file type, type=" + streamLoadTask.getFileType());
        }
        rangeDesc.start_offset = 0;
        rangeDesc.size = -1;
        brokerScanRange.addToRanges(rangeDesc);

        srcTupleDesc = analyzer.getDescTbl().createTupleDescriptor("StreamLoadScanNode");

        TBrokerScanRangeParams params = new TBrokerScanRangeParams();
        params.setStrict_mode(streamLoadTask.isStrictMode());

        Load.initColumns(dstTable, streamLoadTask.getColumnExprDescs(), null /* no hadoop function */,
                exprsByName, analyzer, srcTupleDesc, slotDescByName, params);

        // analyze where statement
        initWhereExpr(streamLoadTask.getWhereExpr(), analyzer);

        computeStats(analyzer);
        createDefaultSmap(analyzer);

        if (streamLoadTask.getColumnSeparator() != null) {
            String sep = streamLoadTask.getColumnSeparator().getColumnSeparator();
            params.setColumn_separator(sep.getBytes(Charset.forName("UTF-8"))[0]);
        } else {
            params.setColumn_separator((byte) '\t');
        }
        params.setLine_delimiter((byte) '\n');
        params.setSrc_tuple_id(srcTupleDesc.getId().asInt());
        params.setDest_tuple_id(desc.getId().asInt());
        brokerScanRange.setParams(params);

        brokerScanRange.setBroker_addresses(Lists.newArrayList());
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException, UserException {
        finalizeParams();
    }

    private void finalizeParams() throws UserException {
        boolean negative = streamLoadTask.getNegative();
        Map<Integer, Integer> destSidToSrcSidWithoutTrans = Maps.newHashMap();
        for (SlotDescriptor dstSlotDesc : desc.getSlots()) {
            if (!dstSlotDesc.isMaterialized()) {
                continue;
            }
            Expr expr = null;
            if (exprsByName != null) {
                expr = exprsByName.get(dstSlotDesc.getColumn().getName());
            }
            if (expr == null) {
                SlotDescriptor srcSlotDesc = slotDescByName.get(dstSlotDesc.getColumn().getName());
                if (srcSlotDesc != null) {
                    destSidToSrcSidWithoutTrans.put(dstSlotDesc.getId().asInt(), srcSlotDesc.getId().asInt());
                    // If dest is allow null, we set source to nullable
                    if (dstSlotDesc.getColumn().isAllowNull()) {
                        srcSlotDesc.setIsNullable(true);
                    }
                    expr = new SlotRef(srcSlotDesc);
                } else {
                    Column column = dstSlotDesc.getColumn();
                    if (column.getDefaultValue() != null) {
                        expr = new StringLiteral(dstSlotDesc.getColumn().getDefaultValue());
                    } else {
                        if (column.isAllowNull()) {
                            expr = NullLiteral.create(column.getType());
                        } else {
                            throw new AnalysisException("column has no source field, column=" + column.getName());
                        }
                    }
                }
            }

            // check hll_hash
            if (dstSlotDesc.getType().getPrimitiveType() == PrimitiveType.HLL) {
                if (!(expr instanceof FunctionCallExpr)) {
                    throw new AnalysisException("HLL column must use hll_hash function, like "
                            + dstSlotDesc.getColumn().getName() + "=hll_hash(xxx)");
                }
                FunctionCallExpr fn = (FunctionCallExpr) expr;
                if (!fn.getFnName().getFunction().equalsIgnoreCase("hll_hash") && !fn.getFnName().getFunction().equalsIgnoreCase("hll_empty")) {
                    throw new AnalysisException("HLL column must use hll_hash function, like "
                            + dstSlotDesc.getColumn().getName() + "=hll_hash(xxx) or " + dstSlotDesc.getColumn().getName() + "=hll_empty()");
                }
                expr.setType(Type.HLL);
            }

            checkBitmapCompatibility(analyzer, dstSlotDesc, expr);

            if (negative && dstSlotDesc.getColumn().getAggregationType() == AggregateType.SUM) {
                expr = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, expr, new IntLiteral(-1));
                expr.analyze(analyzer);
            }
            expr = castToSlot(dstSlotDesc, expr);
            brokerScanRange.params.putToExpr_of_dest_slot(dstSlotDesc.getId().asInt(), expr.treeToThrift());
        }
        brokerScanRange.params.setDest_sid_to_src_sid_without_trans(destSidToSrcSidWithoutTrans);
        brokerScanRange.params.setDest_tuple_id(desc.getId().asInt());
        // LOG.info("brokerScanRange is {}", brokerScanRange);

        // Need re compute memory layout after set some slot descriptor to nullable
        srcTupleDesc.computeMemLayout();
    }

    @Override
    protected void toThrift(TPlanNode planNode) {
        planNode.setNode_type(TPlanNodeType.BROKER_SCAN_NODE);
        TBrokerScanNode brokerScanNode = new TBrokerScanNode(desc.getId().asInt());
        planNode.setBroker_scan_node(brokerScanNode);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        TScanRangeLocations locations = new TScanRangeLocations();
        TScanRange scanRange = new TScanRange();
        scanRange.setBroker_scan_range(brokerScanRange);
        locations.setScan_range(scanRange);
        locations.setLocations(Lists.newArrayList());
        return Lists.newArrayList(locations);
    }

    @Override
    public int getNumInstances() { return 1; }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return "StreamLoadScanNode";
    }
}
