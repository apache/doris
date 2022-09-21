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
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HiveBucketUtil;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.external.hive.util.HiveUtil;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.planner.external.ExternalFileScanNode.ParamCreateContext;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THdfsParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Joiner;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A HiveScanProvider to get information for scan node.
 */
public class HiveScanProvider implements HMSTableScanProviderIf {
    private static final Logger LOG = LogManager.getLogger(HiveScanProvider.class);

    protected HMSExternalTable hmsTable;

    protected int inputSplitNum = 0;
    protected long inputFileSize = 0;
    protected final TupleDescriptor desc;

    public HiveScanProvider(HMSExternalTable hmsTable, TupleDescriptor desc) {
        this.hmsTable = hmsTable;
        this.desc = desc;
    }

    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        TFileFormatType type = null;
        String inputFormatName = getRemoteHiveTable().getSd().getInputFormat();
        String hiveFormat = HiveMetaStoreClientHelper.HiveFileFormat.getFormat(inputFormatName);
        if (hiveFormat.equals(HiveMetaStoreClientHelper.HiveFileFormat.PARQUET.getDesc())) {
            type = TFileFormatType.FORMAT_PARQUET;
        } else if (hiveFormat.equals(HiveMetaStoreClientHelper.HiveFileFormat.ORC.getDesc())) {
            type = TFileFormatType.FORMAT_ORC;
        } else if (hiveFormat.equals(HiveMetaStoreClientHelper.HiveFileFormat.TEXT_FILE.getDesc())) {
            type = TFileFormatType.FORMAT_CSV_PLAIN;
        }
        return type;
    }

    @Override
    public TFileType getLocationType() throws DdlException, MetaNotFoundException {
        String location = hmsTable.getRemoteTable().getSd().getLocation();
        if (location != null && !location.isEmpty()) {
            if (location.startsWith("s3a") || location.startsWith("s3n")) {
                return TFileType.FILE_S3;
            } else if (location.startsWith("hdfs:")) {
                return TFileType.FILE_HDFS;
            }
        }
        throw new DdlException("Unknown file location for hms table.");
    }

    @Override
    public String getMetaStoreUrl() {
        return hmsTable.getMetastoreUri();
    }

    @Override
    public List<InputSplit> getSplits(List<Expr> exprs) throws IOException, UserException {
        String splitsPath = getRemoteHiveTable().getSd().getLocation();
        List<String> partitionKeys = getRemoteHiveTable().getPartitionKeys().stream().map(FieldSchema::getName)
                .collect(Collectors.toList());
        List<Partition> hivePartitions = new ArrayList<>();

        if (partitionKeys.size() > 0) {
            ExprNodeGenericFuncDesc hivePartitionPredicate = HiveMetaStoreClientHelper.convertToHivePartitionExpr(exprs,
                    partitionKeys, hmsTable.getName());

            String metaStoreUris = getMetaStoreUrl();
            hivePartitions.addAll(HiveMetaStoreClientHelper.getHivePartitions(metaStoreUris, getRemoteHiveTable(),
                    hivePartitionPredicate));
        }

        String inputFormatName = getRemoteHiveTable().getSd().getInputFormat();

        Configuration configuration = setConfiguration();
        InputFormat<?, ?> inputFormat = HiveUtil.getInputFormat(configuration, inputFormatName, false);
        List<InputSplit> splits;
        if (!hivePartitions.isEmpty()) {
            try {
                splits = hivePartitions.stream().flatMap(x -> {
                    try {
                        return getSplitsByPath(inputFormat, configuration, x.getSd().getLocation()).stream();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());
            } catch (RuntimeException e) {
                throw new IOException(e);
            }
        } else {
            splits = getSplitsByPath(inputFormat, configuration, splitsPath);
        }
        return HiveBucketUtil.getPrunedSplitsByBuckets(splits, hmsTable.getName(), exprs,
                getRemoteHiveTable().getSd().getBucketCols(), getRemoteHiveTable().getSd().getNumBuckets(),
                getRemoteHiveTable().getParameters());
    }

    private List<InputSplit> getSplitsByPath(InputFormat<?, ?> inputFormat, Configuration configuration,
            String splitsPath) throws IOException {
        JobConf jobConf = new JobConf(configuration);
        // For Tez engine, it may generate subdirectoies for "union" query.
        // So there may be files and directories in the table directory at the same time. eg:
        //      /user/hive/warehouse/region_tmp_union_all2/000000_0
        //      /user/hive/warehouse/region_tmp_union_all2/1
        //      /user/hive/warehouse/region_tmp_union_all2/2
        // So we need to set this config to support visit dir recursively.
        // Otherwise, getSplits() may throw exception: "Not a file xxx"
        // https://blog.actorsfit.com/a?ID=00550-ce56ec63-1bff-4b0c-a6f7-447b93efaa31
        jobConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        FileInputFormat.setInputPaths(jobConf, splitsPath);
        InputSplit[] splits = inputFormat.getSplits(jobConf, 0);
        return Lists.newArrayList(splits);
    }


    protected Configuration setConfiguration() {
        Configuration conf = new HdfsConfiguration();
        Map<String, String> dfsProperties = hmsTable.getDfsProperties();
        for (Map.Entry<String, String> entry : dfsProperties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        Map<String, String> s3Properties = hmsTable.getS3Properties();
        for (Map.Entry<String, String> entry : s3Properties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }

    @Override
    public Table getRemoteHiveTable() throws DdlException, MetaNotFoundException {
        return hmsTable.getRemoteTable();
    }

    @Override
    public Map<String, String> getTableProperties() throws MetaNotFoundException {
        // TODO: implement it when we really properties from remote table.
        return Maps.newHashMap();
    }

    @Override
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        TFileType locationType = getLocationType();
        if (locationType == TFileType.FILE_S3) {
            return hmsTable.getS3Properties();
        } else if (locationType == TFileType.FILE_HDFS) {
            return hmsTable.getDfsProperties();
        } else {
            return Maps.newHashMap();
        }
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        return getRemoteHiveTable().getPartitionKeys().stream().map(FieldSchema::getName).collect(Collectors.toList());
    }

    @Override
    public ParamCreateContext createContext(Analyzer analyzer) throws UserException {
        ParamCreateContext context = new ParamCreateContext();
        context.params = new TFileScanRangeParams();
        context.destTupleDescriptor = desc;
        context.params.setDestTupleId(desc.getId().asInt());
        context.fileGroup = new BrokerFileGroup(hmsTable.getId(),
                hmsTable.getRemoteTable().getSd().getLocation(), hmsTable.getRemoteTable().getSd().getInputFormat());


        // Hive table must extract partition value from path and hudi/iceberg table keep
        // partition field in file.
        List<String> partitionKeys = getPathPartitionKeys();
        List<Column> columns = hmsTable.getBaseSchema(false);
        context.params.setNumOfColumnsFromFile(columns.size() - partitionKeys.size());
        for (SlotDescriptor slot : desc.getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }

            TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
            slotInfo.setSlotId(slot.getId().asInt());
            slotInfo.setIsFileSlot(!partitionKeys.contains(slot.getColumn().getName()));
            context.params.addToRequiredSlots(slotInfo);
        }
        return context;
    }

    @Override
    public void createScanRangeLocations(ParamCreateContext context, BackendPolicy backendPolicy,
            List<TScanRangeLocations> scanRangeLocations) throws UserException {
        try {
            List<InputSplit> inputSplits = getSplits(context.conjuncts);
            this.inputSplitNum = inputSplits.size();
            if (inputSplits.isEmpty()) {
                return;
            }

            String fullPath = ((FileSplit) inputSplits.get(0)).getPath().toUri().toString();
            String filePath = ((FileSplit) inputSplits.get(0)).getPath().toUri().getPath();
            String fsName = fullPath.replace(filePath, "");
            TFileType locationType = getLocationType();
            context.params.setFileType(locationType);
            context.params.setFormatType(getFileFormatType());
            // set hdfs params for hdfs file type.
            Map<String, String> locationProperties = getLocationProperties();
            if (locationType == TFileType.FILE_HDFS) {
                THdfsParams tHdfsParams = BrokerUtil.generateHdfsParam(locationProperties);
                tHdfsParams.setFsName(fsName);
                context.params.setHdfsParams(tHdfsParams);
            } else if (locationType == TFileType.FILE_S3) {
                context.params.setProperties(locationProperties);
            }

            TScanRangeLocations curLocations = newLocations(context.params, backendPolicy);

            FileSplitStrategy fileSplitStrategy = new FileSplitStrategy();

            for (InputSplit split : inputSplits) {
                FileSplit fileSplit = (FileSplit) split;
                List<String> partitionValuesFromPath = BrokerUtil.parseColumnsFromPath(fileSplit.getPath().toString(),
                        getPathPartitionKeys(), false);

                TFileRangeDesc rangeDesc = createFileRangeDesc(fileSplit, partitionValuesFromPath);

                curLocations.getScanRange().getExtScanRange().getFileScanRange().addToRanges(rangeDesc);
                LOG.info(
                        "Assign to backend " + curLocations.getLocations().get(0).getBackendId() + " with table split: "
                                + fileSplit.getPath() + " ( " + fileSplit.getStart() + "," + fileSplit.getLength() + ")"
                                + " loaction: " + Joiner.on("|").join(split.getLocations()));

                fileSplitStrategy.update(fileSplit);
                // Add a new location when it's can be split
                if (fileSplitStrategy.hasNext()) {
                    scanRangeLocations.add(curLocations);
                    curLocations = newLocations(context.params, backendPolicy);
                    fileSplitStrategy.next();
                }
                this.inputFileSize += fileSplit.getLength();
            }
            if (curLocations.getScanRange().getExtScanRange().getFileScanRange().getRangesSize() > 0) {
                scanRangeLocations.add(curLocations);
            }
        } catch (IOException e) {
            throw new UserException(e);
        }
    }

    @Override
    public int getInputSplitNum() {
        return this.inputSplitNum;
    }

    @Override
    public long getInputFileSize() {
        return this.inputFileSize;
    }

    private TScanRangeLocations newLocations(TFileScanRangeParams params, BackendPolicy backendPolicy) {
        // Generate on file scan range
        TFileScanRange fileScanRange = new TFileScanRange();
        fileScanRange.setParams(params);

        // Scan range
        TExternalScanRange externalScanRange = new TExternalScanRange();
        externalScanRange.setFileScanRange(fileScanRange);
        TScanRange scanRange = new TScanRange();
        scanRange.setExtScanRange(externalScanRange);

        // Locations
        TScanRangeLocations locations = new TScanRangeLocations();
        locations.setScanRange(scanRange);

        TScanRangeLocation location = new TScanRangeLocation();
        Backend selectedBackend = backendPolicy.getNextBe();
        location.setBackendId(selectedBackend.getId());
        location.setServer(new TNetworkAddress(selectedBackend.getHost(), selectedBackend.getBePort()));
        locations.addToLocations(location);

        return locations;
    }

    private TFileRangeDesc createFileRangeDesc(FileSplit fileSplit, List<String> columnsFromPath)
            throws DdlException, MetaNotFoundException {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setStartOffset(fileSplit.getStart());
        rangeDesc.setSize(fileSplit.getLength());
        rangeDesc.setColumnsFromPath(columnsFromPath);

        if (getLocationType() == TFileType.FILE_HDFS) {
            rangeDesc.setPath(fileSplit.getPath().toUri().getPath());
        } else if (getLocationType() == TFileType.FILE_S3) {
            rangeDesc.setPath(fileSplit.getPath().toString());
        }
        return rangeDesc;
    }

}

