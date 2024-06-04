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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ColumnDef.DefaultValue;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.FileFormatConstants;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.Load;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.planner.FileLoadScanNode;
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
import org.apache.doris.thrift.TTextSerdeType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LoadScanProvider {

    private FileGroupInfo fileGroupInfo;
    private TupleDescriptor destTupleDesc;

    public LoadScanProvider(FileGroupInfo fileGroupInfo, TupleDescriptor destTupleDesc) {
        this.fileGroupInfo = fileGroupInfo;
        this.destTupleDesc = destTupleDesc;
    }

    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        return null;
    }

    public TFileType getLocationType() throws DdlException, MetaNotFoundException {
        return null;
    }

    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        return null;
    }

    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        return null;
    }

    public FileLoadScanNode.ParamCreateContext createContext(Analyzer analyzer) throws UserException {
        FileLoadScanNode.ParamCreateContext ctx = new FileLoadScanNode.ParamCreateContext();
        ctx.destTupleDescriptor = destTupleDesc;
        ctx.fileGroup = fileGroupInfo.getFileGroup();
        ctx.timezone = analyzer.getTimezone();

        TFileScanRangeParams params = new TFileScanRangeParams();
        params.setFormatType(formatType(fileGroupInfo.getFileGroup().getFileFormat()));
        params.setCompressType(fileGroupInfo.getFileGroup().getCompressType());
        params.setStrictMode(fileGroupInfo.isStrictMode());
        if (fileGroupInfo.getFileGroup().getFileFormat() != null
                && fileGroupInfo.getFileGroup().getFileFormat().equals("hive_text")) {
            params.setTextSerdeType(TTextSerdeType.HIVE_TEXT_SERDE);
        }
        params.setProperties(fileGroupInfo.getBrokerDesc().getProperties());
        if (fileGroupInfo.getBrokerDesc().getFileType() == TFileType.FILE_HDFS) {
            THdfsParams tHdfsParams = HdfsResource.generateHdfsParam(fileGroupInfo.getBrokerDesc().getProperties());
            params.setHdfsParams(tHdfsParams);
        }
        TFileAttributes fileAttributes = new TFileAttributes();
        setFileAttributes(ctx.fileGroup, fileAttributes);
        params.setFileAttributes(fileAttributes);
        params.setFileType(fileGroupInfo.getFileType());
        ctx.params = params;

        initColumns(ctx, analyzer);
        return ctx;
    }

    public void setFileAttributes(BrokerFileGroup fileGroup, TFileAttributes fileAttributes) {
        TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
        textParams.setColumnSeparator(fileGroup.getColumnSeparator());
        textParams.setLineDelimiter(fileGroup.getLineDelimiter());
        textParams.setEnclose(fileGroup.getEnclose());
        textParams.setEscape(fileGroup.getEscape());
        fileAttributes.setTextParams(textParams);
        fileAttributes.setStripOuterArray(fileGroup.isStripOuterArray());
        fileAttributes.setJsonpaths(fileGroup.getJsonPaths());
        fileAttributes.setJsonRoot(fileGroup.getJsonRoot());
        fileAttributes.setNumAsString(fileGroup.isNumAsString());
        fileAttributes.setFuzzyParse(fileGroup.isFuzzyParse());
        fileAttributes.setReadJsonByLine(fileGroup.isReadJsonByLine());
        fileAttributes.setReadByColumnDef(true);
        fileAttributes.setHeaderType(getHeaderType(fileGroup.getFileFormat()));
        fileAttributes.setTrimDoubleQuotes(fileGroup.getTrimDoubleQuotes());
        fileAttributes.setSkipLines(fileGroup.getSkipLines());
        fileAttributes.setIgnoreCsvRedundantCol(fileGroup.getIgnoreCsvRedundantCol());
    }

    private String getHeaderType(String formatType) {
        if (formatType != null) {
            if (formatType.equalsIgnoreCase(FileFormatConstants.FORMAT_CSV_WITH_NAMES)
                    || formatType.equalsIgnoreCase(FileFormatConstants.FORMAT_CSV_WITH_NAMES_AND_TYPES)) {
                return formatType;
            }
        }
        return "";
    }

    public void createScanRangeLocations(FileLoadScanNode.ParamCreateContext context,
                                         FederationBackendPolicy backendPolicy,
                                         List<TScanRangeLocations> scanRangeLocations) throws UserException {
        Preconditions.checkNotNull(fileGroupInfo);
        fileGroupInfo.getFileStatusAndCalcInstance(backendPolicy);
        fileGroupInfo.createScanRangeLocations(context, backendPolicy, scanRangeLocations);
    }

    public int getInputSplitNum() {
        return fileGroupInfo.getFileStatuses().size();
    }

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
    private void initColumns(FileLoadScanNode.ParamCreateContext context, Analyzer analyzer) throws UserException {
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
            OlapTable olapTable = (OlapTable) targetTable;
            String sequenceCol = olapTable.getSequenceMapCol();
            if (sequenceCol != null) {
                String finalSequenceCol = sequenceCol;
                Optional<ImportColumnDesc> foundCol = columnDescs.descs.stream()
                        .filter(c -> c.getColumnName().equalsIgnoreCase(finalSequenceCol)).findAny();
                // if `columnDescs.descs` is empty, that means it's not a partial update load, and user not specify
                // column name.
                if (foundCol.isPresent() || shouldAddSequenceColumn(columnDescs)) {
                    columnDescs.descs.add(new ImportColumnDesc(Column.SEQUENCE_COL,
                            new SlotRef(null, sequenceCol)));
                } else if (!fileGroupInfo.isPartialUpdate()) {
                    Column seqCol = olapTable.getFullSchema().stream()
                                    .filter(col -> col.getName().equals(olapTable.getSequenceMapCol()))
                                    .findFirst().get();
                    if (seqCol.getDefaultValue() == null
                                    || !seqCol.getDefaultValue().equals(DefaultValue.CURRENT_TIMESTAMP)) {
                        throw new UserException("Table " + olapTable.getName()
                                + " has sequence column, need to specify the sequence column");
                    }
                }
            } else {
                sequenceCol = context.fileGroup.getSequenceCol();
                columnDescs.descs.add(new ImportColumnDesc(Column.SEQUENCE_COL,
                        new SlotRef(null, sequenceCol)));
            }
        }
        List<Integer> srcSlotIds = Lists.newArrayList();
        Load.initColumns(fileGroupInfo.getTargetTable(), columnDescs, context.fileGroup.getColumnToHadoopFunction(),
                context.exprMap, analyzer, context.srcTupleDescriptor, context.srcSlotDescByName, srcSlotIds,
                formatType(context.fileGroup.getFileFormat()), fileGroupInfo.getHiddenColumns(),
                fileGroupInfo.isPartialUpdate());

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

    /**
     * if not set sequence column and column size is null or only have deleted sign ,return true
     */
    private boolean shouldAddSequenceColumn(LoadTaskInfo.ImportColumnDescs columnDescs) {
        if (columnDescs.descs.isEmpty()) {
            return true;
        }
        return columnDescs.descs.size() == 1 && columnDescs.descs.get(0).getColumnName()
                .equalsIgnoreCase(Column.DELETE_SIGN);
    }

    private TFileFormatType formatType(String fileFormat) throws UserException {
        if (fileFormat == null) {
            // get file format by the file path
            return TFileFormatType.FORMAT_CSV_PLAIN;
        }
        TFileFormatType formatType = Util.getFileFormatTypeFromName(fileFormat);
        if (formatType == TFileFormatType.FORMAT_UNKNOWN) {
            throw new UserException("Not supported file format: " + fileFormat);
        }
        return formatType;
    }

    public TableIf getTargetTable() {
        return fileGroupInfo.getTargetTable();
    }
}
