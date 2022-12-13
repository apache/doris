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
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.Load;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.planner.external.ExternalFileScanNode.ParamCreateContext;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THdfsParams;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.mapred.InputSplit;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LoadScanProvider implements FileScanProviderIf {

    private FileGroupInfo fileGroupInfo;
    private TupleDescriptor destTupleDesc;

    public LoadScanProvider(FileGroupInfo fileGroupInfo, TupleDescriptor destTupleDesc) {
        this.fileGroupInfo = fileGroupInfo;
        this.destTupleDesc = destTupleDesc;
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
        ctx.destTupleDescriptor = destTupleDesc;
        ctx.fileGroup = fileGroupInfo.getFileGroup();
        ctx.timezone = analyzer.getTimezone();

        TFileScanRangeParams params = new TFileScanRangeParams();
        params.setFormatType(formatType(fileGroupInfo.getFileGroup().getFileFormat(), ""));
        params.setCompressType(fileGroupInfo.getFileGroup().getCompressType());
        params.setStrictMode(fileGroupInfo.isStrictMode());
        params.setProperties(fileGroupInfo.getBrokerDesc().getProperties());
        if (fileGroupInfo.getBrokerDesc().getFileType() == TFileType.FILE_HDFS) {
            THdfsParams tHdfsParams = HdfsResource.generateHdfsParam(fileGroupInfo.getBrokerDesc().getProperties());
            params.setHdfsParams(tHdfsParams);
        }
        TFileAttributes fileAttributes = new TFileAttributes();
        setFileAttributes(ctx.fileGroup, fileAttributes);
        params.setFileAttributes(fileAttributes);
        params.setFileType(fileGroupInfo.getBrokerDesc().getFileType());
        ctx.params = params;

        initColumns(ctx, analyzer);
        return ctx;
    }

    public void setFileAttributes(BrokerFileGroup fileGroup, TFileAttributes fileAttributes) {
        TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
        textParams.setColumnSeparator(fileGroup.getColumnSeparator());
        textParams.setLineDelimiter(fileGroup.getLineDelimiter());
        fileAttributes.setTextParams(textParams);
        fileAttributes.setStripOuterArray(fileGroup.isStripOuterArray());
        fileAttributes.setJsonpaths(fileGroup.getJsonPaths());
        fileAttributes.setJsonRoot(fileGroup.getJsonRoot());
        fileAttributes.setNumAsString(fileGroup.isNumAsString());
        fileAttributes.setFuzzyParse(fileGroup.isFuzzyParse());
        fileAttributes.setReadJsonByLine(fileGroup.isReadJsonByLine());
        fileAttributes.setReadByColumnDef(true);
        fileAttributes.setHeaderType(getHeaderType(fileGroup.getFileFormat()));
    }

    private String getHeaderType(String formatType) {
        if (formatType != null) {
            if (formatType.equalsIgnoreCase(FeConstants.csv_with_names) || formatType.equalsIgnoreCase(
                    FeConstants.csv_with_names_and_types)) {
                return formatType;
            }
        }
        return "";
    }

    @Override
    public void createScanRangeLocations(ParamCreateContext context, BackendPolicy backendPolicy,
            List<TScanRangeLocations> scanRangeLocations) throws UserException {
        Preconditions.checkNotNull(fileGroupInfo);
        fileGroupInfo.getFileStatusAndCalcInstance(backendPolicy);
        fileGroupInfo.createScanRangeLocations(context, backendPolicy, scanRangeLocations);
    }

    @Override
    public int getInputSplitNum() {
        return fileGroupInfo.getFileStatuses().size();
    }

    @Override
    public long getInputFileSize() {
        long res = 0;
        for (TBrokerFileStatus fileStatus : fileGroupInfo.getFileStatuses()) {
            res = fileStatus.getSize();
        }
        return res;
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
        context.srcSlotDescByName = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        context.exprMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

        // for load job, column exprs is got from file group
        // for query, there is no column exprs, they will be got from table's schema in "Load.initColumns"
        LoadTaskInfo.ImportColumnDescs columnDescs = new LoadTaskInfo.ImportColumnDescs();
        // fileGroup.getColumnExprList() contains columns from path
        columnDescs.descs = context.fileGroup.getColumnExprList();
        if (context.fileGroup.getMergeType() == LoadTask.MergeType.MERGE) {
            columnDescs.descs.add(
                    ImportColumnDesc.newDeleteSignImportColumnDesc(context.fileGroup.getDeleteCondition()));
        } else if (context.fileGroup.getMergeType() == LoadTask.MergeType.DELETE) {
            columnDescs.descs.add(ImportColumnDesc.newDeleteSignImportColumnDesc(new IntLiteral(1)));
        }
        // add columnExpr for sequence column
        TableIf targetTable = getTargetTable();
        if (targetTable instanceof OlapTable && ((OlapTable) targetTable).hasSequenceCol()) {
            String sequenceCol = ((OlapTable) targetTable).getSequenceMapCol();
            if (sequenceCol == null) {
                sequenceCol = context.fileGroup.getSequenceCol();
            }
            columnDescs.descs.add(new ImportColumnDesc(Column.SEQUENCE_COL,
                    new SlotRef(null, sequenceCol)));
        }
        List<Integer> srcSlotIds = Lists.newArrayList();
        Load.initColumns(fileGroupInfo.getTargetTable(), columnDescs, context.fileGroup.getColumnToHadoopFunction(),
                context.exprMap, analyzer, context.srcTupleDescriptor, context.srcSlotDescByName, srcSlotIds,
                formatType(context.fileGroup.getFileFormat(), ""), null, VectorizedUtil.isVectorized());

        int columnCountFromPath = 0;
        if (context.fileGroup.getColumnNamesFromPath() != null) {
            columnCountFromPath = context.fileGroup.getColumnNamesFromPath().size();
        }
        int numColumnsFromFile = srcSlotIds.size() - columnCountFromPath;
        Preconditions.checkState(numColumnsFromFile >= 0,
                "srcSlotIds.size is: " + srcSlotIds.size() + ", num columns from path: "
                        + columnCountFromPath);
        context.params.setNumOfColumnsFromFile(numColumnsFromFile);
        for (int i = 0; i < srcSlotIds.size(); ++i) {
            TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
            slotInfo.setSlotId(srcSlotIds.get(i));
            slotInfo.setIsFileSlot(i < numColumnsFromFile);
            context.params.addToRequiredSlots(slotInfo);
        }
    }

    private TFileFormatType formatType(String fileFormat, String path) throws UserException {
        if (fileFormat != null) {
            String lowerFileFormat = fileFormat.toLowerCase();
            if (lowerFileFormat.equals("parquet")) {
                return TFileFormatType.FORMAT_PARQUET;
            } else if (lowerFileFormat.equals("orc")) {
                return TFileFormatType.FORMAT_ORC;
            } else if (lowerFileFormat.equals("json")) {
                return TFileFormatType.FORMAT_JSON;
                // csv/csv_with_name/csv_with_names_and_types treat as csv format
            } else if (lowerFileFormat.equals(FeConstants.csv) || lowerFileFormat.equals(FeConstants.csv_with_names)
                    || lowerFileFormat.equals(FeConstants.csv_with_names_and_types)
                    // TODO: Add TEXTFILE to TFileFormatType to Support hive text file format.
                    || lowerFileFormat.equals(FeConstants.text)) {
                return TFileFormatType.FORMAT_CSV_PLAIN;
            } else {
                throw new UserException("Not supported file format: " + fileFormat);
            }
        } else {
            // get file format by the suffix of file
            return Util.getFileFormatType(path);
        }
    }

    @Override
    public TableIf getTargetTable() {
        return fileGroupInfo.getTargetTable();
    }
}
