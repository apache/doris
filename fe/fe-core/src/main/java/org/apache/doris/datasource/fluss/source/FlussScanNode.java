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

package org.apache.doris.datasource.fluss.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalUtil;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.Split;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.annotations.VisibleForTesting;
import org.apache.fluss.metadata.TableInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class FlussScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(FlussScanNode.class);

    private FlussSource source;

    public FlussScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv,
            SessionVariable sv) {
        super(id, desc, "FLUSS_SCAN_NODE", needCheckColumnPriv, sv);
        source = new FlussSource(desc);
    }

    @VisibleForTesting
    public FlussScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        super(id, desc, "FLUSS_SCAN_NODE", false, sv);
    }

    @Override
    protected void doInitialize() throws UserException {
        super.doInitialize();
        ExternalUtil.initSchemaInfo(params, -1L, source.getTargetTable().getColumns());
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof FlussSplit) {
            setFlussParams(rangeDesc, (FlussSplit) split);
        }
    }

    private void setFlussParams(TFileRangeDesc rangeDesc, FlussSplit flussSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(TableFormatType.FLUSS.value());
        
        // For MVP, we'll pass basic file information
        // BE will use Rust bindings to read actual data
        String fileFormat = getFileFormat(flussSplit.getPathString());
        if (fileFormat.equals("orc")) {
            rangeDesc.setFormatType(TFileFormatType.FORMAT_ORC);
        } else if (fileFormat.equals("parquet")) {
            rangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
        } else {
            throw new RuntimeException("Unsupported file format: " + fileFormat);
        }
        
        // TODO: Add Fluss-specific parameters to TFlussFileDesc when Thrift definitions are added
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        List<Split> splits = new ArrayList<>();
        
        try {
            // For MVP, create a simple split - actual file reading will be handled by BE using Rust bindings
            // BE will use Fluss Rust C++ bindings to read data from Fluss storage
            org.apache.fluss.client.table.Table flussTable = source.getFlussTable();
            TableInfo tableInfo = flussTable.getTableInfo();
            
            // Create a placeholder split with table metadata
            // BE will use this information to connect to Fluss and read data
            FlussSplit split = new FlussSplit(
                    source.getTargetTable().getRemoteDbName(),
                    source.getTargetTable().getRemoteName(),
                    tableInfo.getTableId());
            splits.add(split);
        } catch (Exception e) {
            throw new UserException("Failed to get Fluss splits: " + e.getMessage(), e);
        }
        
        return splits;
    }

    @Override
    public void createScanRangeLocations() throws UserException {
        super.createScanRangeLocations();
    }
}

