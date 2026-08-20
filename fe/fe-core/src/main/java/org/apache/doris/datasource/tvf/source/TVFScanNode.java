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

package org.apache.doris.datasource.tvf.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.FileSplit.FileSplitCreator;
import org.apache.doris.datasource.FileSplitter;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.lance.LanceFragmentInfo;
import org.apache.doris.datasource.lance.LanceStorageOptions;
import org.apache.doris.datasource.lance.source.LanceSplit;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.Backend;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.tablefunction.LocalTableValuedFunction;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TLanceFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TVFScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(TVFScanNode.class);

    private final ExternalFileTableValuedFunction tableValuedFunction;
    private final FunctionGenTable table;

    /**
     * External file scan node for table value function
     * needCheckColumnPriv: Some of ExternalFileScanNode do not need to check column priv
     * eg: s3 tvf
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     */
    public TVFScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv, SessionVariable sv,
            ScanContext scanContext) {
        super(id, desc, "TVF_SCAN_NODE", StatisticalType.TVF_SCAN_NODE, scanContext, needCheckColumnPriv, sv);
        table = (FunctionGenTable) this.desc.getTable();
        tableValuedFunction = (ExternalFileTableValuedFunction) table.getTvf();
    }

    @Override
    protected void initBackendPolicy() throws UserException {
        List<String> preferLocations = new ArrayList<>();
        if (tableValuedFunction instanceof LocalTableValuedFunction) {
            // For local tvf, the backend was specified by backendId
            Long backendId = ((LocalTableValuedFunction) tableValuedFunction).getBackendId();
            if (backendId != -1) {
                // User has specified the backend, only use that backend
                // Otherwise, use all backends for shared storage.
                Backend backend = Env.getCurrentSystemInfo().getBackend(backendId);
                if (backend == null) {
                    throw new UserException("Backend " + backendId + " does not exist");
                }
                preferLocations.add(backend.getHost());
            }
        }
        backendPolicy.init(preferLocations);
        numNodes = backendPolicy.numBackends();
    }

    @Override
    public TFileAttributes getFileAttributes() {
        return tableValuedFunction.getFileAttributes();
    }

    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        return tableValuedFunction.getTFileFormatType();
    }

    @Override
    protected TFileCompressType getFileCompressType(FileSplit fileSplit) throws UserException {
        TFileCompressType fileCompressType = tableValuedFunction.getTFileCompressType();
        return Util.getOrInferCompressType(fileCompressType, fileSplit.getPathString());
    }

    @Override
    protected boolean isFileStreamType() {
        return tableValuedFunction.getTFileType() == TFileType.FILE_STREAM;
    }

    @Override
    public Map<String, String> getLocationProperties() {
        return tableValuedFunction.getBackendConnectProperties();
    }

    @Override
    public void createScanRangeLocations() throws UserException {
        super.createScanRangeLocations();
        if (tableValuedFunction.isLanceFormat()) {
            // lance-c opens the dataset itself and needs the options in Lance's own vocabulary.
            // Set at ScanNode level so credentials are not serialized once per fragment split.
            Map<String, String> lanceStorageOptions = LanceStorageOptions.toLanceOptions(
                    Collections.singletonList(tableValuedFunction.getStorageProperties()));
            if (!lanceStorageOptions.isEmpty()) {
                params.setLanceStorageOptions(lanceStorageOptions);
            }
        }
    }

    @Override
    public List<String> getPathPartitionKeys() {
        return tableValuedFunction.getPathPartitionKeys();
    }

    @Override
    public TableIf getTargetTable() {
        return table;
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        if (tableValuedFunction.isLanceFormat()) {
            return getLanceSplits();
        }

        List<Split> splits = Lists.newArrayList();
        if (tableValuedFunction.getTFileType() == TFileType.FILE_STREAM) {
            return splits;
        }

        List<TBrokerFileStatus> fileStatuses = tableValuedFunction.getFileStatuses();

        // Avoid splitting only for table-level COUNT(*). COUNT(column) still reads column data.
        boolean needSplit = true;
        if (isTableLevelCountStarPushdown()) {
            int parallelNum = sessionVariable.getParallelExecInstanceNum(scanContext.getClusterName());
            int totalFileNum = fileStatuses.size();
            needSplit = FileSplitter.needSplitForCountPushdown(parallelNum, numBackends, totalFileNum);
        }

        long targetFileSplitSize = determineTargetFileSplitSize(fileStatuses);

        for (TBrokerFileStatus fileStatus : fileStatuses) {
            try {
                splits.addAll(fileSplitter.splitFile(
                        LocationPath.of(fileStatus.getPath()),
                        targetFileSplitSize,
                        null,
                        fileStatus.getSize(),
                        fileStatus.getModificationTime(),
                        fileStatus.isSplitable && needSplit,
                        null,
                        FileSplitCreator.DEFAULT));
            } catch (IOException e) {
                LOG.warn("get file split failed for TVF: {}", fileStatus.getPath(), e);
                throw new UserException(e);
            }
        }
        return splits;
    }

    private List<Split> getLanceSplits() throws UserException {
        if (!sessionVariable.enableFileScannerV2) {
            throw new UserException("Lance TVF requires enable_file_scanner_v2=true");
        }
        if (tableValuedFunction.getTFileType() == TFileType.FILE_LOCAL) {
            // A local dataset is visible to its selected BE, not to FE. Keep exactly one
            // whole-dataset split; BE resolves version zero to latest when it opens the dataset.
            return Collections.singletonList(
                    LanceSplit.wholeDatasetAtLatest(tableValuedFunction.getFilePath()));
        }

        long version = tableValuedFunction.getLanceDatasetVersion();
        if (version <= 0) {
            throw new UserException(
                    "S3 Lance TVF metadata was not initialized with a fixed dataset version");
        }
        List<LanceFragmentInfo> fragments = tableValuedFunction.getLanceFragments();
        // Mirror LanceScanNode: use the largest fragment as one standard split so smaller fragments
        // keep their relative physical-row weight, keeping the catalog and S3/file TVF paths in sync.
        long targetRows = 1;
        for (LanceFragmentInfo fragment : fragments) {
            targetRows = Math.max(targetRows, Math.max(fragment.getPhysicalRows(), 1));
        }
        List<Split> splits = new ArrayList<>(fragments.size());
        for (LanceFragmentInfo fragment : fragments) {
            LanceSplit split = LanceSplit.forFragment(tableValuedFunction.getFilePath(), version,
                    fragment.getId(), fragment.getPhysicalRows());
            split.setTargetSplitSize(targetRows);
            splits.add(split);
        }
        return splits;
    }

    private long determineTargetFileSplitSize(List<TBrokerFileStatus> fileStatuses) {
        if (sessionVariable.getFileSplitSize() > 0) {
            return sessionVariable.getFileSplitSize();
        }
        long result = sessionVariable.getMaxInitialSplitSize();
        long totalFileSize = 0;
        boolean exceedInitialThreshold = false;
        for (TBrokerFileStatus fileStatus : fileStatuses) {
            totalFileSize += fileStatus.getSize();
            if (!exceedInitialThreshold
                    && totalFileSize >= sessionVariable.getMaxSplitSize() * sessionVariable.getMaxInitialSplitNum()) {
                exceedInitialThreshold = true;
            }
        }
        result = exceedInitialThreshold ? sessionVariable.getMaxSplitSize() : result;
        result = applyMaxFileSplitNumLimit(result, totalFileSize);
        return result;
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (tableValuedFunction.isLanceFormat()) {
            if (!(split instanceof LanceSplit)) {
                throw new IllegalArgumentException("Expected LanceSplit but got " + split.getClass().getName());
            }
            LanceSplit lanceSplit = (LanceSplit) split;
            TLanceFileDesc lanceParams = new TLanceFileDesc();
            lanceParams.setDatasetUri(lanceSplit.getDatasetUri());
            lanceParams.setVersion(lanceSplit.getVersion());
            if (lanceSplit.hasFragmentIds()) {
                lanceParams.setFragmentIds(lanceSplit.getFragmentIds());
            }

            TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
            tableFormatFileDesc.setTableFormatType(TableFormatType.LANCE.value());
            tableFormatFileDesc.setLanceParams(lanceParams);
            rangeDesc.setTableFormatParams(tableFormatFileDesc);
            return;
        }
        if (split instanceof FileSplit) {
            TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
            tableFormatFileDesc.setTableFormatType(TableFormatType.TVF.value());
            rangeDesc.setTableFormatParams(tableFormatFileDesc);
        }
    }

    @Override
    public int getNumInstances() {
        return scanRangeLocations.size();
    }
}
