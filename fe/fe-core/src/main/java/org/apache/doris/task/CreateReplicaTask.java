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

import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.analysis.DataSortInfo;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Status;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TCompressionType;
import org.apache.doris.thrift.TCreateTabletReq;
import org.apache.doris.thrift.TOlapTableIndex;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletSchema;
import org.apache.doris.thrift.TTabletType;
import org.apache.doris.thrift.TTaskType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CreateReplicaTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(CreateReplicaTask.class);

    private long replicaId;
    private short shortKeyColumnCount;
    private int schemaHash;

    private long version;

    private KeysType keysType;
    private TStorageType storageType;
    private TStorageMedium storageMedium;
    private TCompressionType compressionType;

    private List<Column> columns;

    // bloom filter columns
    private Set<String> bfColumns;
    private double bfFpp;

    // indexes
    private List<Index> indexes;

    private boolean isInMemory;

    private TTabletType tabletType;

    // used for synchronous process
    private MarkedCountDownLatch<Long, Long> latch;

    private boolean inRestoreMode = false;

    // if base tablet id is set, BE will create the replica on same disk as this base tablet
    private long baseTabletId = -1;
    private int baseSchemaHash = -1;

    // V2 is beta rowset, v1 is alpha rowset
    // TODO should unify the naming of v1(alpha rowset), v2(beta rowset), it is very confused to read code
    private TStorageFormat storageFormat = TStorageFormat.V2;

    // true if this task is created by recover request(See comment of Config.recover_with_empty_tablet)
    private boolean isRecoverTask = false;

    private DataSortInfo dataSortInfo;
    private static String storagePolicy;

    private boolean enableUniqueKeyMergeOnWrite;

    private boolean disableAutoCompaction;

    public CreateReplicaTask(long backendId, long dbId, long tableId, long partitionId, long indexId, long tabletId,
                             long replicaId, short shortKeyColumnCount, int schemaHash, long version,
                             KeysType keysType, TStorageType storageType,
                             TStorageMedium storageMedium, List<Column> columns,
                             Set<String> bfColumns, double bfFpp, MarkedCountDownLatch<Long, Long> latch,
                             List<Index> indexes,
                             boolean isInMemory,
                             TTabletType tabletType,
                             DataSortInfo dataSortInfo,
                             TCompressionType compressionType,
                             boolean enableUniqueKeyMergeOnWrite,
                             String storagePolicy, boolean disableAutoCompaction) {
        super(null, backendId, TTaskType.CREATE, dbId, tableId, partitionId, indexId, tabletId);

        this.replicaId = replicaId;
        this.shortKeyColumnCount = shortKeyColumnCount;
        this.schemaHash = schemaHash;

        this.version = version;

        this.keysType = keysType;
        this.storageType = storageType;
        this.storageMedium = storageMedium;
        this.compressionType = compressionType;

        this.columns = columns;

        this.bfColumns = bfColumns;
        this.indexes = indexes;
        this.bfFpp = bfFpp;

        this.latch = latch;

        this.isInMemory = isInMemory;
        this.tabletType = tabletType;
        this.dataSortInfo = dataSortInfo;
        this.enableUniqueKeyMergeOnWrite = (keysType == KeysType.UNIQUE_KEYS && enableUniqueKeyMergeOnWrite);
        this.storagePolicy = storagePolicy;
        this.disableAutoCompaction = disableAutoCompaction;
    }

    public void setIsRecoverTask(boolean isRecoverTask) {
        this.isRecoverTask = isRecoverTask;
    }

    public boolean isRecoverTask() {
        return isRecoverTask;
    }

    public void countDownLatch(long backendId, long tabletId) {
        if (this.latch != null) {
            if (latch.markedCountDown(backendId, tabletId)) {
                LOG.debug("CreateReplicaTask current latch count: {}, backend: {}, tablet:{}",
                          latch.getCount(), backendId, tabletId);
            }
        }
    }

    // call this always means one of tasks is failed. count down to zero to finish entire task
    public void countDownToZero(String errMsg) {
        if (this.latch != null) {
            latch.countDownToZero(new Status(TStatusCode.CANCELLED, errMsg));
            LOG.debug("CreateReplicaTask download to zero. error msg: {}", errMsg);
        }
    }

    public void setLatch(MarkedCountDownLatch<Long, Long> latch) {
        this.latch = latch;
    }

    public void setInRestoreMode(boolean inRestoreMode) {
        this.inRestoreMode = inRestoreMode;
    }

    public void setBaseTablet(long baseTabletId, int baseSchemaHash) {
        this.baseTabletId = baseTabletId;
        this.baseSchemaHash = baseSchemaHash;
    }

    public void setStorageFormat(TStorageFormat storageFormat) {
        this.storageFormat = storageFormat;
    }

    public TCreateTabletReq toThrift() {
        TCreateTabletReq createTabletReq = new TCreateTabletReq();
        createTabletReq.setTabletId(tabletId);

        TTabletSchema tSchema = new TTabletSchema();
        tSchema.setShortKeyColumnCount(shortKeyColumnCount);
        tSchema.setSchemaHash(schemaHash);
        tSchema.setKeysType(keysType.toThrift());
        tSchema.setStorageType(storageType);
        if (dataSortInfo != null) {
            tSchema.setSortType(dataSortInfo.getSortType());
            tSchema.setSortColNum(dataSortInfo.getColNum());
        }
        int deleteSign = -1;
        int sequenceCol = -1;
        List<TColumn> tColumns = new ArrayList<TColumn>();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            TColumn tColumn = column.toThrift();
            // is bloom filter column
            if (bfColumns != null && bfColumns.contains(column.getName())) {
                tColumn.setIsBloomFilterColumn(true);
            }
            // when doing schema change, some modified column has a prefix in name.
            // this prefix is only used in FE, not visible to BE, so we should remove this prefix.
            if (column.getName().startsWith(SchemaChangeHandler.SHADOW_NAME_PRFIX)) {
                tColumn.setColumnName(column.getName().substring(SchemaChangeHandler.SHADOW_NAME_PRFIX.length()));
            }
            tColumn.setVisible(column.isVisible());
            tColumns.add(tColumn);
            if (column.isDeleteSignColumn()) {
                deleteSign = i;
            }
            if (column.isSequenceColumn()) {
                sequenceCol = i;
            }
        }
        tSchema.setColumns(tColumns);
        tSchema.setDeleteSignIdx(deleteSign);
        tSchema.setSequenceColIdx(sequenceCol);

        if (CollectionUtils.isNotEmpty(indexes)) {
            List<TOlapTableIndex> tIndexes = new ArrayList<>();
            for (Index index : indexes) {
                tIndexes.add(index.toThrift());
            }
            tSchema.setIndexes(tIndexes);
            storageFormat = TStorageFormat.V2;
        }

        if (bfColumns != null) {
            tSchema.setBloomFilterFpp(bfFpp);
        }
        tSchema.setIsInMemory(isInMemory);
        tSchema.setDisableAutoCompaction(disableAutoCompaction);
        createTabletReq.setTabletSchema(tSchema);

        createTabletReq.setVersion(version);

        createTabletReq.setStorageMedium(storageMedium);
        createTabletReq.setStoragePolicy(storagePolicy);
        if (inRestoreMode) {
            createTabletReq.setInRestoreMode(true);
        }
        createTabletReq.setTableId(tableId);
        createTabletReq.setPartitionId(partitionId);
        createTabletReq.setReplicaId(replicaId);

        if (baseTabletId != -1) {
            createTabletReq.setBaseTabletId(baseTabletId);
            createTabletReq.setBaseSchemaHash(baseSchemaHash);
        }

        if (storageFormat != null) {
            createTabletReq.setStorageFormat(storageFormat);
        }

        createTabletReq.setTabletType(tabletType);
        createTabletReq.setCompressionType(compressionType);
        createTabletReq.setEnableUniqueKeyMergeOnWrite(enableUniqueKeyMergeOnWrite);
        return createTabletReq;
    }
}
