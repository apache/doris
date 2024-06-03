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
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The BackendSchemaScanNode used for those SchemaTable which data are need to acquire from backends.
 * BackendSchemaScanNode create dynamic `List` type Partition for BACKEND_ID field,
 * it will create partitionItems for each alive be node.
 * So, we can use partitionInfo to select the necessary `be` to send query.
 */
public class BackendPartitionedSchemaScanNode extends SchemaScanNode {

    public static final Set<String> BACKEND_TABLE = new HashSet<>();
    // NOTE: when add a new schema table for BackendPartitionedSchemaScanNode,
    // you need to the table's backend column id name to BACKEND_ID_COLUMN_SET
    // it's used to backend pruner, see computePartitionInfo;
    public static final Set<String> BEACKEND_ID_COLUMN_SET = new HashSet<>();

    static {
        BACKEND_TABLE.add("rowsets");
        BEACKEND_ID_COLUMN_SET.add("backend_id");

        BACKEND_TABLE.add("backend_active_tasks");
        BEACKEND_ID_COLUMN_SET.add("be_id");
    }

    public static boolean isBackendPartitionedSchemaTable(String tableName) {
        if (BACKEND_TABLE.contains(tableName.toLowerCase())) {
            return true;
        }
        return false;
    }

    // backendPartitionInfo is set in generatePartitionInfo().
    // `backendPartitionInfo` is `List Partition` of Backend_ID, one PartitionItem only have one partitionKey
    // for example: if the alive be are: 10001, 10002, 10003, `backendPartitionInfo` like
    // partition_0: ["10001"]
    // partition_1: ["10002"]
    // partition_2: ["10003"]
    private PartitionInfo backendPartitionInfo;
    // partitionID -> backendID
    private Map<Long, Long> partitionIDToBackendID;
    private Collection<Long> selectedPartitionIds = Lists.newArrayList();

    public BackendPartitionedSchemaScanNode(PlanNodeId id, TupleDescriptor desc) {
        super(id, desc);
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        computeColumnsFilter();
        computePartitionInfo();
    }

    @Override
    public void finalize(Analyzer analyzer) throws UserException {
        super.finalize(analyzer);
        createScanRangeLocations();
    }

    @Override
    public void finalizeForNereids() throws UserException {
        computeColumnsFilter();
        computePartitionInfo();
        createScanRangeLocations();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRangeLocations;
    }

    @Override
    protected void createScanRangeLocations() throws UserException {
        scanRangeLocations = new ArrayList<>();
        for (Long partitionID : selectedPartitionIds) {
            Long backendId = partitionIDToBackendID.get(partitionID);
            Backend be = Env.getCurrentSystemInfo().getIdToBackend().get(backendId);
            if (!be.isAlive()) {
                throw new AnalysisException("backend " + be.getId() + " is not alive.");
            }
            TScanRangeLocations locations = new TScanRangeLocations();
            TScanRangeLocation location = new TScanRangeLocation();
            location.setBackendId(be.getId());
            location.setServer(new TNetworkAddress(be.getHost(), be.getBePort()));
            locations.addToLocations(location);
            locations.setScanRange(new TScanRange());
            scanRangeLocations.add(locations);
        }
    }

    private void computePartitionInfo() throws AnalysisException {
        List<Column> partitionColumns = new ArrayList<>();
        for (SlotDescriptor slotDesc : desc.getSlots()) {
            if (BEACKEND_ID_COLUMN_SET.contains(slotDesc.getColumn().getName().toLowerCase())) {
                partitionColumns.add(slotDesc.getColumn());
                break;
            }
        }
        createPartitionInfo(partitionColumns);
        Map<Long, PartitionItem> keyItemMap = backendPartitionInfo.getIdToItem(false);
        PartitionPruner partitionPruner = new ListPartitionPrunerV2(keyItemMap,
                backendPartitionInfo.getPartitionColumns(),
                columnNameToRange);
        selectedPartitionIds = partitionPruner.prune();
    }

    /**
     * create PartitionInfo for partitionColumn
     * @param partitionColumns The Columns we want to create partitionInfo
     * @throws AnalysisException
     */
    private void createPartitionInfo(List<Column> partitionColumns) throws AnalysisException {
        backendPartitionInfo = new PartitionInfo(PartitionType.LIST, partitionColumns);
        partitionIDToBackendID = new HashMap<>();
        long partitionID = 0;
        for (Backend be : Env.getCurrentSystemInfo().getIdToBackend().values()) {
            if (be.isAlive()) {
                // create partition key
                PartitionKey partitionKey = new PartitionKey();
                for (Column partitionColumn : partitionColumns) {
                    LiteralExpr expr = LiteralExpr.create(String.valueOf(be.getId()), partitionColumn.getType());
                    partitionKey.pushColumn(expr, partitionColumn.getDataType());
                }
                // create partition Item
                List<PartitionKey> partitionKeys = new ArrayList<>();
                partitionKeys.add(partitionKey);
                PartitionItem partitionItem = new ListPartitionItem(partitionKeys);
                backendPartitionInfo.setItem(partitionID, false, partitionItem);
                partitionIDToBackendID.put(partitionID, be.getId());
                ++partitionID;
            }
        }
    }
}
