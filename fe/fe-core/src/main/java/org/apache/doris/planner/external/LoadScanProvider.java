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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.Load;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.planner.external.ExternalFileScanNode.ParamCreateContext;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.mapred.InputSplit;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LoadScanProvider implements FileScanProviderIf {

    FileGroupInfo fileGroupInfo;

    public LoadScanProvider(FileGroupInfo fileGroup) {
        this.fileGroupInfo = fileGroupInfo;
    }

    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        return null;
    }

    @Override
    public TFileType getLocationType() throws DdlException, MetaNotFoundException {
        return null;
    }

    @Override
    public int getGroupNum() {
        return 0;
    }

    @Override
    public List<InputSplit> getSplits(List<Expr> exprs) throws IOException, UserException {
        return null;
    }

    @Override
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        return null;
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        return null;
    }

    @Override
    public ParamCreateContext createContext(Analyzer analyzer) throws UserException {
        ParamCreateContext ctx = new ParamCreateContext();
        ctx.scanProvider = this;
        ctx.fileGroup = fileGroupInfo.getFileGroup();
        ctx.timezone = analyzer.getTimezone();

        TFileScanRangeParams params = new TFileScanRangeParams();
        ctx.params = params;

        BrokerFileGroup fileGroup = ctx.fileGroup;
        params.setStrictMode(fileGroupInfo.isStrictMode());
        params.setProperties(fileGroupInfo.getBrokerDesc().getProperties());
        ctx.deleteCondition = fileGroup.getDeleteCondition();
        ctx.mergeType = fileGroup.getMergeType();
        initColumns(ctx, analyzer);
        return ctx;
    }

    @Override
    public void createScanRangeLocations(ParamCreateContext context, BackendPolicy backendPolicy,
            List<TScanRangeLocations> scanRangeLocations) throws UserException {
        Preconditions.checkNotNull(fileGroupInfo);
        fileGroupInfo.getFileStatusAndCalcInstance(backendPolicy);
        fileGroupInfo.processFileGroup(context, backendPolicy, scanRangeLocations);
    }

    /**
     * This method is used to calculate the slotDescByName and exprMap.
     * The expr in exprMap is analyzed in this function.
     * The smap of slot which belongs to expr will be analyzed by src desc.
     * slotDescByName: the single slot from columns in load stmt
     * exprMap: the expr from column mapping in load stmt.
     *
     * @param context
     * @throws UserException
     */
    private void initColumns(ParamCreateContext context, Analyzer analyzer) throws UserException {
        context.srcTupleDescriptor = analyzer.getDescTbl().createTupleDescriptor();
        context.slotDescByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        context.exprMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

        // for load job, column exprs is got from file group
        // for query, there is no column exprs, they will be got from table's schema in "Load.initColumns"
        LoadTaskInfo.ImportColumnDescs columnDescs = new LoadTaskInfo.ImportColumnDescs();
        // fileGroup.getColumnExprList() contains columns from path
        columnDescs.descs = context.fileGroup.getColumnExprList();
        if (context.mergeType == LoadTask.MergeType.MERGE) {
            columnDescs.descs.add(ImportColumnDesc.newDeleteSignImportColumnDesc(context.deleteCondition));
        } else if (context.mergeType == LoadTask.MergeType.DELETE) {
            columnDescs.descs.add(ImportColumnDesc.newDeleteSignImportColumnDesc(new IntLiteral(1)));
        }
        // add columnExpr for sequence column
        if (context.fileGroup.hasSequenceCol()) {
            columnDescs.descs.add(
                    new ImportColumnDesc(Column.SEQUENCE_COL, new SlotRef(null, context.fileGroup.getSequenceCol())));
        }
        // TOOD:
        // required_slots 按顺序记录了源表的列，表标记哪些sipartition列，最终在BE cia c层 file slot and partition slot
        // src slot id就按 order记录了源文件的列，然后在BE端作为input block的列order。并且from pat is 排在end的。
        List<Integer> srcSlotIds = Lists.newArrayList();
        Load.initColumns(fileGroupInfo.getTargetTable(), columnDescs, context.fileGroup.getColumnToHadoopFunction(),
                context.exprMap, analyzer, context.srcTupleDescriptor, context.slotDescByName, srcSlotIds,
                formatType(context.fileGroup.getFileFormat(), ""), null, VectorizedUtil.isVectorized());

        int numColumnsFromFile = srcSlotIds.size() - context.fileGroup.getColumnsFromPath().size();
        Preconditions.checkState(numColumnsFromFile >= 0,
                "srcSlotIds.size is: " + srcSlotIds.size() + ", num columns from path: "
                        + context.fileGroup.getColumnsFromPath().size());
        for (int i = 0; i < srcSlotIds.size(); ++i) {
            TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
            slotInfo.setSlotId(srcSlotIds.get(i));
            slotInfo.setIsFileSlot(i < numColumnsFromFile);
        }
    }

    private TFileFormatType formatType(String fileFormat, String path) throws UserException {
        if (fileFormat != null) {
            if (fileFormat.toLowerCase().equals("parquet")) {
                return TFileFormatType.FORMAT_PARQUET;
            } else if (fileFormat.toLowerCase().equals("orc")) {
                return TFileFormatType.FORMAT_ORC;
            } else if (fileFormat.toLowerCase().equals("json")) {
                return TFileFormatType.FORMAT_JSON;
                // csv/csv_with_name/csv_with_names_and_types treat as csv format
            } else if (fileFormat.toLowerCase().equals(FeConstants.csv) || fileFormat.toLowerCase()
                    .equals(FeConstants.csv_with_names) || fileFormat.toLowerCase()
                    .equals(FeConstants.csv_with_names_and_types)
                    // TODO: Add TEXTFILE to TFileFormatType to Support hive text file format.
                    || fileFormat.toLowerCase().equals(FeConstants.text)) {
                return TFileFormatType.FORMAT_CSV_PLAIN;
            } else {
                throw new UserException("Not supported file format: " + fileFormat);
            }
        }

        String lowerCasePath = path.toLowerCase();
        if (lowerCasePath.endsWith(".parquet") || lowerCasePath.endsWith(".parq")) {
            return TFileFormatType.FORMAT_PARQUET;
        } else if (lowerCasePath.endsWith(".gz")) {
            return TFileFormatType.FORMAT_CSV_GZ;
        } else if (lowerCasePath.endsWith(".bz2")) {
            return TFileFormatType.FORMAT_CSV_BZ2;
        } else if (lowerCasePath.endsWith(".lz4")) {
            return TFileFormatType.FORMAT_CSV_LZ4FRAME;
        } else if (lowerCasePath.endsWith(".lzo")) {
            return TFileFormatType.FORMAT_CSV_LZOP;
        } else if (lowerCasePath.endsWith(".deflate")) {
            return TFileFormatType.FORMAT_CSV_DEFLATE;
        } else {
            return TFileFormatType.FORMAT_CSV_PLAIN;
        }
    }
}
