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
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.system.Backend;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.tablefunction.LocalTableValuedFunction;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
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
    public TVFScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv) {
        super(id, desc, "TVF_SCAN_NODE", StatisticalType.TVF_SCAN_NODE, needCheckColumnPriv);
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
        return Util.getOrInferCompressType(fileCompressType, fileSplit.getPath().toString());
    }

    @Override
    public TFileType getLocationType() throws DdlException, MetaNotFoundException {
        return getLocationType(null);
    }

    @Override
    public TFileType getLocationType(String location) throws DdlException, MetaNotFoundException {
        return tableValuedFunction.getTFileType();
    }

    @Override
    public Map<String, String> getLocationProperties() throws MetaNotFoundException, DdlException {
        return tableValuedFunction.getLocationProperties();
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
    public List<Split> getSplits() throws UserException {
        List<Split> splits = Lists.newArrayList();
        if (tableValuedFunction.getTFileType() == TFileType.FILE_STREAM) {
            return splits;
        }
        List<TBrokerFileStatus> fileStatuses = tableValuedFunction.getFileStatuses();
        for (TBrokerFileStatus fileStatus : fileStatuses) {
            Path path = new Path(fileStatus.getPath());
            try {
                splits.addAll(splitFile(path, fileStatus.getBlockSize(), null, fileStatus.getSize(),
                        fileStatus.getModificationTime(), fileStatus.isSplitable, null));
            } catch (IOException e) {
                LOG.warn("get file split failed for TVF: {}", path, e);
                throw new UserException(e);
            }
        }
        return splits;
    }

    @Override
    public int getNumInstances() {
        return scanRangeLocations.size();
    }
}
