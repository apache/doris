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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.planner.external.ExternalFileScanNode.ParamCreateContext;
import org.apache.doris.system.Backend;
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
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Joiner;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class QueryScanProvider implements FileScanProviderIf {
    public static final Logger LOG = LogManager.getLogger(QueryScanProvider.class);
    private int inputSplitNum = 0;
    private long inputFileSize = 0;

    public abstract TFileAttributes getFileAttributes() throws UserException;

    @Override
    public void createScanRangeLocations(ParamCreateContext context, BackendPolicy backendPolicy,
            List<TScanRangeLocations> scanRangeLocations) throws UserException {
        long start = System.currentTimeMillis();
        try {
            List<InputSplit> inputSplits = getSplits(context.conjuncts);
            this.inputSplitNum = inputSplits.size();
            if (inputSplits.isEmpty()) {
                return;
            }
            InputSplit inputSplit = inputSplits.get(0);
            TFileType locationType = getLocationType();
            context.params.setFileType(locationType);
            TFileFormatType fileFormatType = getFileFormatType();
            context.params.setFormatType(getFileFormatType());
            if (fileFormatType == TFileFormatType.FORMAT_CSV_PLAIN || fileFormatType == TFileFormatType.FORMAT_JSON) {
                context.params.setFileAttributes(getFileAttributes());
            }

            if (inputSplit instanceof IcebergSplit) {
                IcebergScanProvider.setIcebergParams(context, (IcebergSplit) inputSplit);
            }
            // set hdfs params for hdfs file type.
            Map<String, String> locationProperties = getLocationProperties();
            if (locationType == TFileType.FILE_HDFS) {
                String fsName = "";
                if (this instanceof TVFScanProvider) {
                    fsName = ((TVFScanProvider) this).getFsName();
                } else {
                    String fullPath = ((FileSplit) inputSplit).getPath().toUri().toString();
                    String filePath = ((FileSplit) inputSplit).getPath().toUri().getPath();
                    // eg:
                    // hdfs://namenode
                    // s3://buckets
                    fsName = fullPath.replace(filePath, "");
                }
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
                List<String> pathPartitionKeys = getPathPartitionKeys();
                List<String> partitionValuesFromPath = BrokerUtil.parseColumnsFromPath(fileSplit.getPath().toString(),
                        pathPartitionKeys, false);

                TFileRangeDesc rangeDesc = createFileRangeDesc(fileSplit, partitionValuesFromPath, pathPartitionKeys);

                curLocations.getScanRange().getExtScanRange().getFileScanRange().addToRanges(rangeDesc);
                LOG.debug("assign to backend {} with table split: {} ({}, {}), location: {}",
                        curLocations.getLocations().get(0).getBackendId(), fileSplit.getPath(), fileSplit.getStart(),
                        fileSplit.getLength(), Joiner.on("|").join(split.getLocations()));

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
            LOG.debug("create #{} ScanRangeLocations cost: {} ms",
                    scanRangeLocations.size(), (System.currentTimeMillis() - start));
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

    private TFileRangeDesc createFileRangeDesc(FileSplit fileSplit, List<String> columnsFromPath,
            List<String> columnsFromPathKeys)
            throws DdlException, MetaNotFoundException {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setStartOffset(fileSplit.getStart());
        rangeDesc.setSize(fileSplit.getLength());
        rangeDesc.setColumnsFromPath(columnsFromPath);
        rangeDesc.setColumnsFromPathKeys(columnsFromPathKeys);

        if (getLocationType() == TFileType.FILE_HDFS) {
            rangeDesc.setPath(fileSplit.getPath().toUri().getPath());
        } else if (getLocationType() == TFileType.FILE_S3) {
            rangeDesc.setPath(fileSplit.getPath().toString());
        }
        return rangeDesc;
    }
}
