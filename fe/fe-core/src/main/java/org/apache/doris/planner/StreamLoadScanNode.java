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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.load.Load;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.TBrokerRangeDesc;
import org.apache.doris.thrift.TBrokerScanRange;
import org.apache.doris.thrift.TBrokerScanRangeParams;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
    private LoadTaskInfo taskInfo;

    // helper
    private Analyzer analyzer;
    private TupleDescriptor srcTupleDesc;
    private TBrokerScanRange brokerScanRange;

    // If use case sensitive map, for example,
    // the column name 「A」 in the table and the mapping '(a) set (A = a)' in load sql，
    // Slotdescbyname stores「a」, later will use 「a」to get table's 「A」 column info, will throw exception.
    private final Map<String, SlotDescriptor> slotDescByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    private final Map<String, Expr> exprsByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    // used to construct for streaming loading
    public StreamLoadScanNode(
            TUniqueId loadId, PlanNodeId id, TupleDescriptor tupleDesc, Table dstTable, LoadTaskInfo taskInfo) {
        super(id, tupleDesc, "StreamLoadScanNode", StatisticalType.STREAM_LOAD_SCAN_NODE);
        this.loadId = loadId;
        this.dstTable = dstTable;
        this.taskInfo = taskInfo;
        this.numInstances = 1;
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        // can't call super.init(), because after super.init, conjuncts would be null
        assignConjuncts(analyzer);

        this.analyzer = analyzer;
        brokerScanRange = new TBrokerScanRange();

        deleteCondition = taskInfo.getDeleteCondition();
        mergeType = taskInfo.getMergeType();

        TBrokerRangeDesc rangeDesc = new TBrokerRangeDesc();
        rangeDesc.file_type = taskInfo.getFileType();
        rangeDesc.format_type = taskInfo.getFormatType();
        if (rangeDesc.format_type == TFileFormatType.FORMAT_JSON) {
            if (!taskInfo.getJsonPaths().isEmpty()) {
                rangeDesc.setJsonpaths(taskInfo.getJsonPaths());
            }
            if (!taskInfo.getJsonRoot().isEmpty()) {
                rangeDesc.setJsonRoot(taskInfo.getJsonRoot());
            }
            rangeDesc.setStripOuterArray(taskInfo.isStripOuterArray());
            rangeDesc.setNumAsString(taskInfo.isNumAsString());
            rangeDesc.setFuzzyParse(taskInfo.isFuzzyParse());
            rangeDesc.setReadJsonByLine(taskInfo.isReadJsonByLine());
        }
        rangeDesc.splittable = false;
        switch (taskInfo.getFileType()) {
            case FILE_LOCAL:
                rangeDesc.path = taskInfo.getPath();
                break;
            case FILE_STREAM:
                rangeDesc.path = "Invalid Path";
                rangeDesc.load_id = loadId;
                break;
            default:
                throw new UserException("unsupported file type, type=" + taskInfo.getFileType());
        }
        rangeDesc.start_offset = 0;
        rangeDesc.setHeaderType(taskInfo.getHeaderType());
        rangeDesc.size = -1;
        brokerScanRange.addToRanges(rangeDesc);

        srcTupleDesc = analyzer.getDescTbl().createTupleDescriptor("StreamLoadScanNode");

        TBrokerScanRangeParams params = new TBrokerScanRangeParams();
        LoadTaskInfo.ImportColumnDescs columnExprDescs = taskInfo.getColumnExprDescs();
        if (!columnExprDescs.isColumnDescsRewrited) {
            if (mergeType == LoadTask.MergeType.MERGE) {
                columnExprDescs.descs.add(ImportColumnDesc.newDeleteSignImportColumnDesc(deleteCondition));
            } else if (mergeType == LoadTask.MergeType.DELETE) {
                columnExprDescs.descs.add(ImportColumnDesc.newDeleteSignImportColumnDesc(new IntLiteral(1)));
            }
            if (taskInfo.hasSequenceCol()) {
                columnExprDescs.descs.add(new ImportColumnDesc(Column.SEQUENCE_COL,
                        new SlotRef(null, taskInfo.getSequenceCol())));
            }
        }

        if (params.getSrcSlotIds() == null) {
            params.setSrcSlotIds(Lists.newArrayList());
        }
        Load.initColumns(dstTable, columnExprDescs, null /* no hadoop function */, exprsByName, analyzer, srcTupleDesc,
                slotDescByName, params.getSrcSlotIds(), taskInfo.getFormatType(), taskInfo.getHiddenColumns(),
                VectorizedUtil.isVectorized());

        // analyze where statement
        initAndSetPrecedingFilter(taskInfo.getPrecedingFilter(), this.srcTupleDesc, analyzer);
        initAndSetWhereExpr(taskInfo.getWhereExpr(), this.desc, analyzer);

        createDefaultSmap(analyzer);

        if (taskInfo.getColumnSeparator() != null) {
            String sep = taskInfo.getColumnSeparator().getSeparator();
            params.setColumnSeparatorStr(sep);
            params.setColumnSeparatorLength(sep.getBytes(Charset.forName("UTF-8")).length);
            params.setColumnSeparator(sep.getBytes(Charset.forName("UTF-8"))[0]);
        } else {
            params.setColumnSeparator((byte) '\t');
            params.setColumnSeparatorLength(1);
            params.setColumnSeparatorStr("\t");
        }
        if (taskInfo.getLineDelimiter() != null) {
            String sep = taskInfo.getLineDelimiter().getSeparator();
            params.setLineDelimiterStr(sep);
            params.setLineDelimiterLength(sep.getBytes(Charset.forName("UTF-8")).length);
            params.setLineDelimiter(sep.getBytes(Charset.forName("UTF-8"))[0]);
        } else {
            params.setLineDelimiter((byte) '\n');
            params.setLineDelimiterLength(1);
        }
        params.setDestTupleId(desc.getId().asInt());
        brokerScanRange.setParams(params);

        brokerScanRange.setBrokerAddresses(Lists.newArrayList());
        computeStats(analyzer);
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        finalizeParams(slotDescByName, exprsByName, brokerScanRange.params, srcTupleDesc,
                taskInfo.isStrictMode(), taskInfo.getNegative(), analyzer);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        TScanRangeLocations locations = new TScanRangeLocations();
        TScanRange scanRange = new TScanRange();
        scanRange.setBrokerScanRange(brokerScanRange);
        locations.setScanRange(scanRange);
        locations.setLocations(Lists.newArrayList());
        return Lists.newArrayList(locations);
    }

    @Override
    public int getNumInstances() {
        return 1;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return "StreamLoadScanNode";
    }
}
