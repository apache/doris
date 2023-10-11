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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.planner.FileLoadScanNode;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * FileTable encapsulates a set of files to be scanned into a Table like structure,
 * which has table attributes such as Schema, and file attributes such as row and column separators.
 * Mainly used to unify HMSTableScan and FileScan
 */
public class FileGroupInfo {
    private static final Logger LOG = LogManager.getLogger(FileGroupInfo.class);

    private static final String HIVE_DEFAULT_COLUMN_SEPARATOR = "\001";
    private static final String HIVE_DEFAULT_LINE_DELIMITER = "\n";

    public enum JobType {
        BULK_LOAD,
        STREAM_LOAD
    }

    private JobType jobType;

    private TUniqueId loadId;
    private long loadJobId;
    private long txnId;
    private Table targetTable;
    private BrokerDesc brokerDesc;
    private BrokerFileGroup fileGroup;
    private List<TBrokerFileStatus> fileStatuses;
    private int filesAdded;
    private boolean strictMode;
    private int loadParallelism;
    // set by getFileStatusAndCalcInstance
    private long bytesPerInstance = 0;
    // used for stream load, FILE_LOCAL or FILE_STREAM
    private TFileType fileType;
    private List<String> hiddenColumns = null;
    private boolean isPartialUpdate = false;

    // for broker load
    public FileGroupInfo(long loadJobId, long txnId, Table targetTable, BrokerDesc brokerDesc,
            BrokerFileGroup fileGroup, List<TBrokerFileStatus> fileStatuses, int filesAdded, boolean strictMode,
            int loadParallelism) {
        this.jobType = JobType.BULK_LOAD;
        this.loadJobId = loadJobId;
        this.txnId = txnId;
        this.targetTable = targetTable;
        this.brokerDesc = brokerDesc;
        this.fileGroup = fileGroup;
        this.fileStatuses = fileStatuses;
        this.filesAdded = filesAdded;
        this.strictMode = strictMode;
        this.loadParallelism = loadParallelism;
        this.fileType = brokerDesc.getFileType();
    }

    // for stream load
    public FileGroupInfo(TUniqueId loadId, long txnId, Table targetTable, BrokerDesc brokerDesc,
            BrokerFileGroup fileGroup, TBrokerFileStatus fileStatus, boolean strictMode,
            TFileType fileType, List<String> hiddenColumns, boolean isPartialUpdate) {
        this.jobType = JobType.STREAM_LOAD;
        this.loadId = loadId;
        this.txnId = txnId;
        this.targetTable = targetTable;
        this.brokerDesc = brokerDesc;
        this.fileGroup = fileGroup;
        this.fileStatuses = Lists.newArrayList();
        this.fileStatuses.add(fileStatus);
        this.filesAdded = 1;
        this.strictMode = strictMode;
        this.fileType = fileType;
        this.hiddenColumns = hiddenColumns;
        this.isPartialUpdate = isPartialUpdate;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public BrokerFileGroup getFileGroup() {
        return fileGroup;
    }

    public List<TBrokerFileStatus> getFileStatuses() {
        return fileStatuses;
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    public int getLoadParallelism() {
        return loadParallelism;
    }

    public TFileType getFileType() {
        return fileType;
    }

    public String getExplainString(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append("file scan\n");
        return sb.toString();
    }

    public List<String> getHiddenColumns() {
        return hiddenColumns;
    }

    public boolean isPartialUpdate() {
        return isPartialUpdate;
    }

    public void getFileStatusAndCalcInstance(FederationBackendPolicy backendPolicy) throws UserException {
        if (filesAdded == 0) {
            throw new UserException("No source file in this table(" + targetTable.getName() + ").");
        }

        int numInstances = 1;
        if (jobType == JobType.BULK_LOAD) {
            long totalBytes = 0;
            for (TBrokerFileStatus fileStatus : fileStatuses) {
                totalBytes += fileStatus.size;
            }
            numInstances = (int) (totalBytes / Config.min_bytes_per_broker_scanner);
            int totalLoadParallelism = loadParallelism * backendPolicy.numBackends();
            numInstances = Math.min(totalLoadParallelism, numInstances);
            numInstances = Math.min(numInstances, Config.max_broker_concurrency);
            numInstances = Math.max(1, numInstances);

            bytesPerInstance = totalBytes / numInstances + 1;
            if (bytesPerInstance > Config.max_bytes_per_broker_scanner) {
                throw new UserException(
                        "Scan bytes per file scanner exceed limit: " + Config.max_bytes_per_broker_scanner);
            }
        } else {
            // stream load, not need to split
            bytesPerInstance = Long.MAX_VALUE;
        }
        LOG.info("number instance of file scan node is: {}, bytes per instance: {}", numInstances, bytesPerInstance);
    }

    public void createScanRangeLocations(FileLoadScanNode.ParamCreateContext context,
                                         FederationBackendPolicy backendPolicy,
                                         List<TScanRangeLocations> scanRangeLocations) throws UserException {
        TScanRangeLocations curLocations = newLocations(context.params, brokerDesc, backendPolicy);
        long curInstanceBytes = 0;
        long curFileOffset = 0;
        for (int i = 0; i < fileStatuses.size(); ) {
            TBrokerFileStatus fileStatus = fileStatuses.get(i);
            long leftBytes = fileStatus.size - curFileOffset;
            long tmpBytes = curInstanceBytes + leftBytes;
            // header_type
            TFileFormatType formatType = formatType(context.fileGroup.getFileFormat(), fileStatus.path);
            context.params.setFormatType(formatType);
            TFileCompressType compressType =
                    Util.getOrInferCompressType(context.fileGroup.getCompressType(), fileStatus.path);
            context.params.setCompressType(compressType);
            List<String> columnsFromPath = BrokerUtil.parseColumnsFromPath(fileStatus.path,
                    context.fileGroup.getColumnNamesFromPath());
            // Assign scan range locations only for broker load.
            // stream load has only one file, and no need to set multi scan ranges.
            if (tmpBytes > bytesPerInstance && jobType != JobType.STREAM_LOAD) {
                // Now only support split plain text
                if (compressType == TFileCompressType.PLAIN
                        && (formatType == TFileFormatType.FORMAT_CSV_PLAIN && fileStatus.isSplitable)
                        || formatType == TFileFormatType.FORMAT_JSON) {
                    long rangeBytes = bytesPerInstance - curInstanceBytes;
                    TFileRangeDesc rangeDesc = createFileRangeDesc(curFileOffset, fileStatus, rangeBytes,
                            columnsFromPath);
                    curLocations.getScanRange().getExtScanRange().getFileScanRange().addToRanges(rangeDesc);
                    curFileOffset += rangeBytes;
                } else {
                    TFileRangeDesc rangeDesc = createFileRangeDesc(0, fileStatus, leftBytes,
                            columnsFromPath);
                    curLocations.getScanRange().getExtScanRange().getFileScanRange().addToRanges(rangeDesc);
                    i++;
                }

                // New one scan
                scanRangeLocations.add(curLocations);
                curLocations = newLocations(context.params, brokerDesc, backendPolicy);
                curInstanceBytes = 0;

            } else {
                TFileRangeDesc rangeDesc = createFileRangeDesc(curFileOffset, fileStatus, leftBytes, columnsFromPath);
                curLocations.getScanRange().getExtScanRange().getFileScanRange().addToRanges(rangeDesc);
                curFileOffset = 0;
                curInstanceBytes += leftBytes;
                i++;
            }
        }

        // Put the last file
        if (curLocations.getScanRange().getExtScanRange().getFileScanRange().isSetRanges()) {
            scanRangeLocations.add(curLocations);
        }
    }

    protected TScanRangeLocations newLocations(TFileScanRangeParams params, BrokerDesc brokerDesc,
            FederationBackendPolicy backendPolicy) throws UserException {

        Backend selectedBackend = backendPolicy.getNextBe();

        // Generate one file scan range
        TFileScanRange fileScanRange = new TFileScanRange();

        if (brokerDesc.getStorageType() == StorageBackend.StorageType.BROKER) {
            FsBroker broker = null;
            try {
                broker = Env.getCurrentEnv().getBrokerMgr().getBroker(brokerDesc.getName(), selectedBackend.getHost());
            } catch (AnalysisException e) {
                throw new UserException(e.getMessage());
            }
            params.addToBrokerAddresses(new TNetworkAddress(broker.host, broker.port));
        } else {
            params.setBrokerAddresses(new ArrayList<>());
        }
        fileScanRange.setParams(params);

        // Scan range
        TExternalScanRange externalScanRange = new TExternalScanRange();
        externalScanRange.setFileScanRange(fileScanRange);
        TScanRange scanRange = new TScanRange();
        scanRange.setExtScanRange(externalScanRange);

        // Locations
        TScanRangeLocations locations = new TScanRangeLocations();
        locations.setScanRange(scanRange);

        if (jobType == JobType.BULK_LOAD) {
            TScanRangeLocation location = new TScanRangeLocation();
            location.setBackendId(selectedBackend.getId());
            location.setServer(new TNetworkAddress(selectedBackend.getHost(), selectedBackend.getBePort()));
            locations.addToLocations(location);
        } else {
            // stream load do not need locations
            locations.setLocations(Lists.newArrayList());
        }

        return locations;
    }

    private TFileFormatType formatType(String fileFormat, String path) throws UserException {
        if (fileFormat == null) {
            // get file format by the file path
            return Util.getFileFormatTypeFromPath(path);
        }
        TFileFormatType formatType = Util.getFileFormatTypeFromName(fileFormat);
        if (formatType == TFileFormatType.FORMAT_UNKNOWN) {
            throw new UserException("Not supported file format: " + fileFormat);
        }
        return formatType;
    }

    private TFileRangeDesc createFileRangeDesc(long curFileOffset, TBrokerFileStatus fileStatus, long rangeBytes,
            List<String> columnsFromPath) {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        if (jobType == JobType.BULK_LOAD) {
            rangeDesc.setPath(fileStatus.path);
            rangeDesc.setStartOffset(curFileOffset);
            rangeDesc.setSize(rangeBytes);
            rangeDesc.setFileSize(fileStatus.size);
            rangeDesc.setColumnsFromPath(columnsFromPath);
        } else {
            // for stream load
            if (getFileType() == TFileType.FILE_LOCAL) {
                // when loading parquet via stream, there will be a local file saved on BE
                // so to read it as a local file.
                Preconditions.checkState(fileGroup.getFilePaths().size() == 1);
                rangeDesc.setPath(fileGroup.getFilePaths().get(0));
                rangeDesc.setStartOffset(0);
            }
            rangeDesc.setLoadId(loadId);
            rangeDesc.setSize(fileStatus.size);
            rangeDesc.setFileSize(fileStatus.size);
        }
        rangeDesc.setModificationTime(fileStatus.getModificationTime());
        return rangeDesc;
    }
}


