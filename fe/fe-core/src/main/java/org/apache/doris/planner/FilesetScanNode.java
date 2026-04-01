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

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.FilesetTable;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.FederationBackendPolicy;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanNode;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Scan node for FilesetTable.
 *
 * Generates a single TFileScanRange with FORMAT_MULTIDATA format type and
 * fileset_params pointing to the table's object-storage path. The BE
 * FilesetReader is invoked via the file_scanner when table_format_type == "fileset".
 */
public class FilesetScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(FilesetScanNode.class);

    private final FilesetTable filesetTable;

    public FilesetScanNode(PlanNodeId id, TupleDescriptor desc, ScanContext scanContext) {
        super(id, desc, "SCAN FILESET", scanContext);
        this.filesetTable = (FilesetTable) desc.getTable();
    }

    @Override
    public void finalizeForNereids() throws UserException {
        createScanRangeLocations();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.FILE_SCAN_NODE;
        TFileScanNode fileScanNode = new TFileScanNode();
        fileScanNode.setTupleId(desc.getId().asInt());
        fileScanNode.setTableName(filesetTable.getName());
        msg.file_scan_node = fileScanNode;
    }

    @Override
    protected void createScanRangeLocations() throws UserException {
        FederationBackendPolicy backendPolicy = new FederationBackendPolicy();
        backendPolicy.init();

        // Build fileset_params: table_path + file_type + backend S3/HDFS credentials
        Map<String, String> filesetParams = filesetTable.getBackendProperties();
        filesetParams.put("table_path", filesetTable.getTablePath());
        filesetParams.put("file_type", filesetTable.getFileType().name());

        // Build TTableFormatFileDesc
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType("fileset");
        tableFormatFileDesc.setFilesetParams(filesetParams);

        // Build TFileRangeDesc – one range covering the entire fileset directory
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setPath(filesetTable.getTablePath());
        rangeDesc.setStartOffset(0);
        rangeDesc.setSize(-1);
        rangeDesc.setFileType(filesetTable.getFileType());
        rangeDesc.setFormatType(TFileFormatType.FORMAT_MULTIDATA);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);

        // Build TFileScanRangeParams
        TFileScanRangeParams scanParams = new TFileScanRangeParams();
        scanParams.setDestTupleId(desc.getId().asInt());
        scanParams.setNumOfColumnsFromFile(desc.getTable().getBaseSchema(false).size());
        scanParams.setSrcTupleId(-1);
        for (org.apache.doris.analysis.SlotDescriptor slot : desc.getSlots()) {
            TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
            slotInfo.setSlotId(slot.getId().asInt());
            slotInfo.setIsFileSlot(true);
            scanParams.addToRequiredSlots(slotInfo);
        }

        // Assemble TFileScanRange
        TFileScanRange fileScanRange = new TFileScanRange();
        fileScanRange.addToRanges(rangeDesc);
        fileScanRange.setParams(scanParams);

        TExternalScanRange externalScanRange = new TExternalScanRange();
        externalScanRange.setFileScanRange(fileScanRange);

        TScanRange scanRange = new TScanRange();
        scanRange.setExtScanRange(externalScanRange);

        TScanRangeLocations locations = createSingleScanRangeLocations(backendPolicy);
        locations.setScanRange(scanRange);

        scanRangeLocations = Lists.newArrayList(locations);
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocations;
    }

    @Override
    public int getNumInstances() {
        return 1;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("TABLE: ").append(filesetTable.getName()).append("\n");
        output.append(prefix).append("LOCATION: ").append(filesetTable.getLocation()).append("\n");
        output.append(prefix).append(String.format("cardinality=%s", cardinality)).append("\n");
        return output.toString();
    }
}
