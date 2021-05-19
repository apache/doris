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

package org.apache.doris.persist;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.RangeUtils;

import com.google.common.collect.Range;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PartitionPersistInfo implements Writable {
    private Long dbId;
    private Long tableId;
    private Partition partition;

    private Range<PartitionKey> range;
    private PartitionItem listPartitionItem;
    private DataProperty dataProperty;
    private ReplicaAllocation replicaAlloc;
    private boolean isInMemory = false;
    private boolean isTempPartition = false;
    
    public PartitionPersistInfo() {
    }

    public PartitionPersistInfo(long dbId, long tableId, Partition partition, Range<PartitionKey> range,
                                PartitionItem listPartitionItem, DataProperty dataProperty, ReplicaAllocation replicaAlloc,
                                boolean isInMemory, boolean isTempPartition) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partition = partition;

        this.range = range;
        this.listPartitionItem = listPartitionItem;
        this.dataProperty = dataProperty;

        this.replicaAlloc = replicaAlloc;
        this.isInMemory = isInMemory;
        this.isTempPartition = isTempPartition;
    }
    
    public Long getDbId() {
        return dbId;
    }
    
    public Long getTableId() {
        return tableId;
    }

    public Partition getPartition() {
        return partition;
    }

    public Range<PartitionKey> getRange() {
        return range;
    }

    public PartitionItem getListPartitionItem() {
        return listPartitionItem;
    }

    public DataProperty getDataProperty() {
        return dataProperty;
    }

    public ReplicaAllocation getReplicaAlloc() {
        return replicaAlloc;
    }

    public boolean isInMemory() {
        return isInMemory;
    }

    public boolean isTempPartition() {
        return isTempPartition;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        partition.write(out);

        RangeUtils.writeRange(out, range);
        listPartitionItem.write(out);
        dataProperty.write(out);
        replicaAlloc.write(out);
        out.writeBoolean(isInMemory);
        out.writeBoolean(isTempPartition);
    }

    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        partition = Partition.read(in);

        range = RangeUtils.readRange(in);
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_98) {
            listPartitionItem = ListPartitionItem.read(in);
        } else {
            listPartitionItem = ListPartitionItem.DUMMY_ITEM;
        }

        dataProperty = DataProperty.read(in);
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_100) {
            this.replicaAlloc = new ReplicaAllocation(in.readShort());
        } else {
            this.replicaAlloc = ReplicaAllocation.read(in);
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_72) {
            isInMemory = in.readBoolean();
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_74) {
            isTempPartition = in.readBoolean();
        }
    }
    
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PartitionPersistInfo)) {
            return false;
        }
        
        PartitionPersistInfo info = (PartitionPersistInfo) obj;
        
        return dbId.equals(info.dbId)
                   && tableId.equals(info.tableId)
                   && partition.equals(info.partition);
    }
}
