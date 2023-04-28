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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.planner.FileLoadScanNode;
import org.apache.doris.planner.Split;
import org.apache.doris.planner.Splitter;
import org.apache.doris.planner.external.iceberg.IcebergScanProvider;
import org.apache.doris.planner.external.iceberg.IcebergSplit;
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
    public void createScanRangeLocations(FileLoadScanNode.ParamCreateContext context,
                                         FederationBackendPolicy backendPolicy,
                                         List<TScanRangeLocations> scanRangeLocations) throws UserException {
    }

    @Override
    public FileLoadScanNode.ParamCreateContext createContext(Analyzer analyzer) throws UserException {
        return null;
    }

    @Override
    public void createScanRangeLocations(List<Expr> conjuncts, TFileScanRangeParams params,
                                         FederationBackendPolicy backendPolicy,
                                         List<TScanRangeLocations> scanRangeLocations) throws UserException {
        long start = System.currentTimeMillis();
        List<Split> inputSplits = splitter.getSplits(conjuncts);
        this.inputSplitNum = inputSplits.size();
        if (inputSplits.isEmpty()) {
            return;
        }
        FileSplit inputSplit = (FileSplit) inputSplits.get(0);
        TFileType locationType = getLocationType();
        params.setFileType(locationType);
        TFileFormatType fileFormatType = getFileFormatType();
        params.setFormatType(getFileFormatType());
        if (fileFormatType == TFileFormatType.FORMAT_CSV_PLAIN || fileFormatType == TFileFormatType.FORMAT_JSON) {
            params.setFileAttributes(getFileAttributes());
        }

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
            params.setHdfsParams(tHdfsParams);

            if (locationType == TFileType.FILE_BROKER) {
                FsBroker broker = Env.getCurrentEnv().getBrokerMgr().getAnyAliveBroker();
                if (broker == null) {
                    throw new UserException("No alive broker.");
                }
                params.addToBrokerAddresses(new TNetworkAddress(broker.ip, broker.port));
            }
        } else if (locationType == TFileType.FILE_S3) {
            params.setProperties(locationProperties);
        }

        List<String> pathPartitionKeys = getPathPartitionKeys();
        for (Split split : inputSplits) {
            TScanRangeLocations curLocations = newLocations(params, backendPolicy);
            FileSplit fileSplit = (FileSplit) split;

            // If fileSplit has partition values, use the values collected from hive partitions.
            // Otherwise, use the values in file path.
            List<String> partitionValuesFromPath = fileSplit.getPartitionValues() == null
                    ? BrokerUtil.parseColumnsFromPath(fileSplit.getPath().toString(), pathPartitionKeys, false)
                    : fileSplit.getPartitionValues();

            TFileRangeDesc rangeDesc = createFileRangeDesc(fileSplit, partitionValuesFromPath, pathPartitionKeys);
            // external data lake table
            if (fileSplit instanceof IcebergSplit) {
                IcebergScanProvider.setIcebergParams(rangeDesc, (IcebergSplit) fileSplit);
            }

            curLocations.getScanRange().getExtScanRange().getFileScanRange().addToRanges(rangeDesc);
            LOG.debug("assign to backend {} with table split: {} ({}, {}), location: {}",
                    curLocations.getLocations().get(0).getBackendId(), fileSplit.getPath(), fileSplit.getStart(),
                    fileSplit.getLength(), Joiner.on("|").join(fileSplit.getHosts()));
            scanRangeLocations.add(curLocations);
            this.inputFileSize += fileSplit.getLength();
        }
        LOG.debug("create #{} ScanRangeLocations cost: {} ms",
                scanRangeLocations.size(), (System.currentTimeMillis() - start));
    }

    @Override
    public int getInputSplitNum() {
        return this.inputSplitNum;
    }

    @Override
    public long getInputFileSize() {
        return this.inputFileSize;
    }

    private TScanRangeLocations newLocations(TFileScanRangeParams params, FederationBackendPolicy backendPolicy) {
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
        location.setServer(new TNetworkAddress(selectedBackend.getIp(), selectedBackend.getBePort()));
        locations.addToLocations(location);

        return locations;
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
}
