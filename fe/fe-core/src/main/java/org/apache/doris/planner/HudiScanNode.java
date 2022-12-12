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
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.external.hive.util.HiveUtil;
import org.apache.doris.external.hudi.HudiProperty;
import org.apache.doris.external.hudi.HudiTable;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerRangeDesc;
import org.apache.doris.thrift.TBrokerScanRangeParams;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.THdfsParams;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mortbay.log.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hudi scan node to query hudi table.
 */
public class HudiScanNode extends BrokerScanNode {
    private static final Logger LOG = LogManager.getLogger(HudiScanNode.class);

    private HudiTable hudiTable;

    private ExprNodeGenericFuncDesc hivePartitionPredicate;
    private List<ImportColumnDesc> parsedColumnExprList = new ArrayList<>();
    private String hdfsUri;

    private Table remoteHiveTable;

    /* hudi table properties */
    private String fileFormat;
    private String inputFormatName;
    private String basePath;
    private List<String> partitionKeys = new ArrayList<>();
    /* hudi table properties */

    private List<TScanRangeLocations> scanRangeLocations;

    public HudiScanNode(PlanNodeId id, TupleDescriptor destTupleDesc, String planNodeName,
                        List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded) {
        super(id, destTupleDesc, planNodeName, fileStatusesList, filesAdded);
        this.hudiTable = (HudiTable) destTupleDesc.getTable();
    }

    public String getHdfsUri() {
        return hdfsUri;
    }

    public List<ImportColumnDesc> getParsedColumnExprList() {
        return parsedColumnExprList;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public String getBasePath() {
        return basePath;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    /**
     *  super init will invoke initFileGroup, In initFileGroup will do
     *  1, get hudi table from hive metastore
     *  2, resolve hudi table type, query mode, table base path, partition columns information.
     *  3. generate fileGroup
     *
     * @param analyzer analyzer
     * @throws UserException when init failed.
     */
    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        // init scan range params
        initParams(analyzer);
    }

    @Override
    public int getNumInstances() {
        return scanRangeLocations.size();
    }

    @Override
    protected void initFileGroup() throws UserException {
        resolvHiveTable();
        analyzeColumnFromPath();

        HudiTable hudiTable = (HudiTable) desc.getTable();
        fileGroups = Lists.newArrayList(
                new BrokerFileGroup(hudiTable.getId(),
                        "\t",
                        "\n",
                        getBasePath(),
                        getFileFormat(),
                        getPartitionKeys(),
                        getParsedColumnExprList()));
        brokerDesc = new BrokerDesc("HudiTableDesc", StorageBackend.StorageType.HDFS, hudiTable.getTableProperties());

    }

    /**
     * Override this function just for skip parent's getFileStatus.
     */
    @Override
    protected void getFileStatus() throws DdlException {
        // set fileStatusesList as empty, we do not need fileStatusesList
        fileStatusesList = Lists.newArrayList();
        filesAdded = 0;
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        try {
            ParamCreateContext context = getParamCreateContexts().get(0);
            finalizeParams(context.slotDescByName, context.exprMap, context.params,
                    context.srcTupleDescriptor, false, context.fileGroup.isNegative(), analyzer);
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }
        try {
            buildScanRange();
        } catch (IOException e) {
            LOG.error("Build scan range failed.", e);
            throw new UserException("Build scan range failed.", e);
        }
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocations;
    }

    private void resolvHiveTable() throws DdlException {
        this.remoteHiveTable = HiveMetaStoreClientHelper.getTable(
                hudiTable.getHmsDatabaseName(),
                hudiTable.getHmsTableName(),
                hudiTable.getTableProperties().get(HudiProperty.HUDI_HIVE_METASTORE_URIS));

        this.inputFormatName = remoteHiveTable.getSd().getInputFormat();
        this.fileFormat = HiveMetaStoreClientHelper.HiveFileFormat.getFormat(this.inputFormatName);
        this.basePath = remoteHiveTable.getSd().getLocation();
        for (FieldSchema fieldSchema : remoteHiveTable.getPartitionKeys()) {
            this.partitionKeys.add(fieldSchema.getName());
        }
        Log.info("Hudi inputFileFormat is " + inputFormatName + ", basePath is " + this.basePath);
    }

    private void initParams(Analyzer analyzer) {
        ParamCreateContext context = getParamCreateContexts().get(0);
        TBrokerScanRangeParams params = context.params;

        Map<String, SlotDescriptor> slotDescByName = Maps.newHashMap();

        List<Column> columns = hudiTable.getBaseSchema(false);
        // init slot desc add expr map, also transform hadoop functions
        for (Column column : columns) {
            SlotDescriptor slotDesc = analyzer.getDescTbl().addSlotDescriptor(context.srcTupleDescriptor);
            slotDesc.setType(ScalarType.createType(PrimitiveType.VARCHAR));
            slotDesc.setIsMaterialized(true);
            slotDesc.setIsNullable(true);
            slotDesc.setColumn(new Column(column.getName(), PrimitiveType.VARCHAR));
            params.addToSrcSlotIds(slotDesc.getId().asInt());
            slotDescByName.put(column.getName(), slotDesc);
        }
        context.slotDescByName = slotDescByName;
    }

    private InputSplit[] getSplits() throws UserException, IOException {
        String splitsPath = basePath;
        if (partitionKeys.size() > 0) {
            hivePartitionPredicate = HiveMetaStoreClientHelper.convertToHivePartitionExpr(
                    conjuncts, partitionKeys, hudiTable.getName());

            String metaStoreUris = hudiTable.getTableProperties().get(HudiProperty.HUDI_HIVE_METASTORE_URIS);
            List<Partition> hivePartitions =
                    HiveMetaStoreClientHelper.getHivePartitions(metaStoreUris, remoteHiveTable, hivePartitionPredicate);
            splitsPath = hivePartitions.stream()
                    .map(x -> x.getSd().getLocation()).collect(Collectors.joining(","));
        }


        Configuration configuration = new HdfsConfiguration();
        InputFormat<?, ?> inputFormat = HiveUtil.getInputFormat(configuration, inputFormatName, false);
        // alway get fileSplits from inputformat,
        // because all hoodie input format have UseFileSplitsFromInputFormat annotation
        JobConf jobConf = new JobConf(configuration);
        FileInputFormat.setInputPaths(jobConf, splitsPath);
        InputSplit[] inputSplits = inputFormat.getSplits(jobConf, 0);
        return inputSplits;

    }

    // If fileFormat is not null, we use fileFormat instead of check file's suffix
    protected void buildScanRange() throws UserException, IOException {
        scanRangeLocations = Lists.newArrayList();
        InputSplit[] inputSplits = getSplits();
        if (inputSplits.length == 0) {
            return;
        }

        String fullPath = ((FileSplit) inputSplits[0]).getPath().toUri().toString();
        String filePath = ((FileSplit) inputSplits[0]).getPath().toUri().getPath();
        String fsName = fullPath.replace(filePath, "");
        Log.debug("Hudi path's host is " + fsName);

        TFileFormatType fileFormatType = null;
        if (this.inputFormatName.toLowerCase().contains("parquet")) {
            fileFormatType = TFileFormatType.FORMAT_PARQUET;
        } else if (this.inputFormatName.toLowerCase(Locale.ROOT).contains("orc")) {
            fileFormatType = TFileFormatType.FORMAT_ORC;
        } else {
            throw new UserException("Unsupported hudi table format [" + this.inputFormatName + "].");
        }

        ParamCreateContext context = getParamCreateContexts().get(0);
        for (InputSplit split : inputSplits) {
            FileSplit fileSplit = (FileSplit) split;

            TScanRangeLocations curLocations = newLocations(context.params, brokerDesc);
            List<String> partitionValuesFromPath = BrokerUtil.parseColumnsFromPath(fileSplit.getPath().toString(),
                    getPartitionKeys(), false);
            int numberOfColumnsFromFile = context.slotDescByName.size() - partitionValuesFromPath.size();

            TBrokerRangeDesc rangeDesc = createBrokerRangeDesc(fileSplit, fileFormatType,
                    partitionValuesFromPath, numberOfColumnsFromFile, brokerDesc);
            rangeDesc.getHdfsParams().setFsName(fsName);
            rangeDesc.setReadByColumnDef(true);

            curLocations.getScanRange().getBrokerScanRange().addToRanges(rangeDesc);
            Log.debug("Assign to backend " + curLocations.getLocations().get(0).getBackendId()
                    + " with hudi split: " +  fileSplit.getPath()
                    + " ( " + fileSplit.getStart() + "," + fileSplit.getLength() + ")");

            // Put the last file
            if (curLocations.getScanRange().getBrokerScanRange().isSetRanges()) {
                scanRangeLocations.add(curLocations);
            }
        }
    }

    private TBrokerRangeDesc createBrokerRangeDesc(FileSplit fileSplit,
                                                   TFileFormatType formatType,
                                                   List<String> columnsFromPath, int numberOfColumnsFromFile,
                                                   BrokerDesc brokerDesc) {
        TBrokerRangeDesc rangeDesc = new TBrokerRangeDesc();
        rangeDesc.setFileType(brokerDesc.getFileType());
        rangeDesc.setFormatType(formatType);
        rangeDesc.setPath(fileSplit.getPath().toUri().getPath());
        rangeDesc.setSplittable(true);
        rangeDesc.setStartOffset(fileSplit.getStart());
        rangeDesc.setSize(fileSplit.getLength());
        rangeDesc.setNumOfColumnsFromFile(numberOfColumnsFromFile);
        rangeDesc.setColumnsFromPath(columnsFromPath);
        // set hdfs params for hdfs file type.
        switch (brokerDesc.getFileType()) {
            case FILE_HDFS:
                THdfsParams tHdfsParams = HdfsResource.generateHdfsParam(brokerDesc.getProperties());
                rangeDesc.setHdfsParams(tHdfsParams);
                break;
            default:
                break;
        }
        return rangeDesc;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        if (!isLoad()) {
            output.append(prefix).append("TABLE: ").append(hudiTable.getName()).append("\n");
            output.append(prefix).append("PATH: ")
                    .append(hudiTable.getTableProperties().get(HudiProperty.HUDI_HIVE_METASTORE_URIS)).append("\n");
        }
        return output.toString();
    }

    /**
     * Analyze columns from path, the partition columns.
     */
    private void analyzeColumnFromPath() {
        for (String colName : partitionKeys) {
            ImportColumnDesc importColumnDesc = new ImportColumnDesc(colName, null);
            parsedColumnExprList.add(importColumnDesc);
        }
    }
}
