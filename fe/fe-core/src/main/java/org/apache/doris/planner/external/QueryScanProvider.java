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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.planner.ScanRangeList;
import org.apache.doris.planner.Split;
import org.apache.doris.planner.Splitter;
import org.apache.doris.planner.external.ExternalFileScanNode.ParamCreateContext;
import org.apache.doris.planner.external.iceberg.IcebergScanProvider;
import org.apache.doris.planner.external.iceberg.IcebergSplit;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THdfsParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public abstract class QueryScanProvider implements FileScanProviderIf {
    public static final Logger LOG = LogManager.getLogger(QueryScanProvider.class);
    private int inputSplitNum = 0;
    private long inputFileSize = 0;
    protected Splitter splitter;

    public abstract TFileAttributes getFileAttributes() throws UserException;

    @Override
    public int getInputSplitNum() {
        return this.inputSplitNum;
    }

    @Override
    public long getInputFileSize() {
        return this.inputFileSize;
    }

    private TFileRangeDesc createFileRangeDesc(FileSplit fileSplit, List<String> columnsFromPath,
            List<String> columnsFromPathKeys)
            throws DdlException, MetaNotFoundException {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setStartOffset(fileSplit.getStart());
        rangeDesc.setSize(fileSplit.getLength());
        // fileSize only be used when format is orc or parquet and TFileType is broker
        // When TFileType is other type, it is not necessary
        rangeDesc.setFileSize(fileSplit.getFileLength());
        rangeDesc.setColumnsFromPath(columnsFromPath);
        rangeDesc.setColumnsFromPathKeys(columnsFromPathKeys);

        if (getLocationType() == TFileType.FILE_HDFS) {
            rangeDesc.setPath(fileSplit.getPath().toUri().getPath());
        } else if (getLocationType() == TFileType.FILE_S3 || getLocationType() == TFileType.FILE_BROKER) {
            // need full path
            rangeDesc.setPath(fileSplit.getPath().toString());
        }
        return rangeDesc;
    }

    @Override
    public void createScanRangeList(ParamCreateContext context, ScanRangeList scanRangeList) throws UserException {
        long start = System.currentTimeMillis();
        List<Split> inputSplits = splitter.getSplits(context.conjuncts);
        this.inputSplitNum = inputSplits.size();
        if (inputSplits.isEmpty()) {
            return;
        }
        FileSplit inputSplit = (FileSplit) inputSplits.get(0);
        TFileType locationType = getLocationType();
        context.params.setFileType(locationType);
        TFileFormatType fileFormatType = getFileFormatType();
        context.params.setFormatType(getFileFormatType());
        if (fileFormatType == TFileFormatType.FORMAT_CSV_PLAIN || fileFormatType == TFileFormatType.FORMAT_JSON) {
            context.params.setFileAttributes(getFileAttributes());
        }

        TScanRange range = newRange(context.params);
        // set hdfs params for hdfs file type.
        Map<String, String> locationProperties = getLocationProperties();
        if (locationType == TFileType.FILE_HDFS || locationType == TFileType.FILE_BROKER) {
            String fsName = "";
            if (this instanceof TVFScanProvider) {
                fsName = ((TVFScanProvider) this).getFsName();
            } else {
                String fullPath = inputSplit.getPath().toUri().toString();
                String filePath = inputSplit.getPath().toUri().getPath();
                // eg:
                // hdfs://namenode
                // s3://buckets
                fsName = fullPath.replace(filePath, "");
            }
            THdfsParams tHdfsParams = HdfsResource.generateHdfsParam(locationProperties);
            tHdfsParams.setFsName(fsName);
            context.params.setHdfsParams(tHdfsParams);

            if (locationType == TFileType.FILE_BROKER) {
                FsBroker broker = Env.getCurrentEnv().getBrokerMgr().getAnyAliveBroker();
                if (broker == null) {
                    throw new UserException("No alive broker.");
                }
                context.params.addToBrokerAddresses(new TNetworkAddress(broker.ip, broker.port));
            }
        } else if (locationType == TFileType.FILE_S3) {
            context.params.setProperties(locationProperties);
        }

        FileSplitStrategy fileSplitStrategy = new FileSplitStrategy();
        for (Split split : inputSplits) {
            FileSplit fileSplit = (FileSplit) split;
            List<String> pathPartitionKeys = getPathPartitionKeys();
            List<String> partitionValuesFromPath = BrokerUtil.parseColumnsFromPath(fileSplit.getPath().toString(),
                    pathPartitionKeys, false);

            TFileRangeDesc rangeDesc = createFileRangeDesc(fileSplit, partitionValuesFromPath, pathPartitionKeys);
            // external data lake table
            if (fileSplit instanceof IcebergSplit) {
                IcebergScanProvider.setIcebergParams(rangeDesc, (IcebergSplit) fileSplit);
            }
            range.getExtScanRange().getFileScanRange().addToRanges(rangeDesc);
            this.inputFileSize += fileSplit.getLength();
            fileSplitStrategy.update(fileSplit);
            // Add a new location when current scanInfo size is too large or including too many splits.
            if (fileSplitStrategy.hasNext()) {
                scanRangeList.addToScanRanges(range);
                range = newRange(context.params);
                fileSplitStrategy.next();
            }
        }
        if (range.getExtScanRange().getFileScanRange().getRangesSize() > 0) {
            scanRangeList.addToScanRanges(range);
        }
        LOG.debug("create #{} ScanRangeList cost: {} ms",
                scanRangeList.getScanRangeSize(), (System.currentTimeMillis() - start));
    }

    private TScanRange newRange(TFileScanRangeParams params) {
        // Generate on file scan range
        TFileScanRange fileScanRange = new TFileScanRange();
        fileScanRange.setParams(params);
        // Scan range
        TExternalScanRange externalScanRange = new TExternalScanRange();
        externalScanRange.setFileScanRange(fileScanRange);
        TScanRange scanRange = new TScanRange();
        scanRange.setExtScanRange(externalScanRange);
        return scanRange;
    }
}
