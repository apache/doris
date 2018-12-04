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

package org.apache.doris.task;

import org.apache.doris.catalog.Column;
import org.apache.doris.thrift.TAlterTabletReq;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TCreateTabletReq;
import org.apache.doris.thrift.TKeysType;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletSchema;
import org.apache.doris.thrift.TTaskType;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CreateRollupTask extends AgentTask {

    private long baseTableId;
    private long baseTabletId;

    private long rollupReplicaId;

    private int rollupSchemaHash;
    private int baseSchemaHash;

    private short shortKeyColumnCount;
    private TStorageType storageType;
    private TKeysType keysType;

    private List<Column> rollupColumns;

    // bloom filter columns
    private Set<String> bfColumns;
    private double bfFpp;

    public CreateRollupTask(TResourceInfo resourceInfo, long backendId, long dbId, long tableId,
                            long partitionId, long rollupIndexId, long baseIndexId, long rollupTabletId,
                            long baseTabletId, long rollupReplicaId, short shortKeyColumnCount,
                            int rollupSchemaHash, int baseSchemaHash, TStorageType storageType,
                            List<Column> rollupColumns, Set<String> bfColumns, double bfFpp, TKeysType keysType) {
        super(resourceInfo, backendId, TTaskType.ROLLUP, dbId, tableId, partitionId, rollupIndexId, rollupTabletId);

        this.baseTableId = baseIndexId;
        this.baseTabletId = baseTabletId;
        this.rollupReplicaId = rollupReplicaId;

        this.rollupSchemaHash = rollupSchemaHash;
        this.baseSchemaHash = baseSchemaHash;

        this.shortKeyColumnCount = shortKeyColumnCount;
        this.storageType = storageType;
        this.keysType = keysType;

        this.rollupColumns = rollupColumns;

        this.bfColumns = bfColumns;
        this.bfFpp = bfFpp;
    }

    public TAlterTabletReq toThrift() {
        TAlterTabletReq tAlterTabletReq = new TAlterTabletReq();
        tAlterTabletReq.setBase_tablet_id(baseTabletId);
        tAlterTabletReq.setBase_schema_hash(baseSchemaHash);

        // make 1 TCreateTableReq
        TCreateTabletReq createTabletReq = new TCreateTabletReq();
        createTabletReq.setTablet_id(tabletId);

        // no need to set version
        // schema
        TTabletSchema tSchema = new TTabletSchema();
        tSchema.setShort_key_column_count(shortKeyColumnCount);
        tSchema.setSchema_hash(rollupSchemaHash);
        tSchema.setStorage_type(storageType);
        tSchema.setKeys_type(keysType);

        List<TColumn> tColumns = new ArrayList<TColumn>();
        for (Column column : rollupColumns) {
            TColumn tColumn = column.toThrift();
            // is bloom filter column
            if (bfColumns != null && bfColumns.contains(column.getName())) {
                tColumn.setIs_bloom_filter_column(true);
            }
            tColumns.add(tColumn);
        }
        tSchema.setColumns(tColumns);

        if (bfColumns != null) {
            tSchema.setBloom_filter_fpp(bfFpp);
        }
        createTabletReq.setTablet_schema(tSchema);
        createTabletReq.setTable_id(tableId);
        createTabletReq.setPartition_id(partitionId);

        tAlterTabletReq.setNew_tablet_req(createTabletReq);

        return tAlterTabletReq;
    }

    public long getBaseTableId() {
        return baseTableId;
    }

    public long getBaseTabletId() {
        return baseTabletId;
    }

    public long getRollupReplicaId() {
        return rollupReplicaId;
    }

    public int getRollupSchemaHash() {
        return rollupSchemaHash;
    }

    public int getBaseSchemaHash() {
        return baseSchemaHash;
    }

    public short getShortKeyColumnCount() {
        return shortKeyColumnCount;
    }

    public TStorageType getStorageType() {
        return storageType;
    }

    public List<Column> getRollupColumns() {
        return rollupColumns;
    }
}
