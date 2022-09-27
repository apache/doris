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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecoverInfo implements Writable {
    private long dbId;
    private String newDbName;
    private long tableId;
    private String newTableName;
    private long partitionId;
    private String newPartitionName;

    public RecoverInfo() {
        // for persist
    }

    public RecoverInfo(long dbId, long tableId, long partitionId, String newDbName, String newTableName,
                       String newPartitionName) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.newDbName = newDbName;
        this.newTableName = newTableName;
        this.newPartitionName = newPartitionName;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public String getNewDbName() {
        return newDbName;
    }

    public String getNewTableName() {
        return newTableName;
    }

    public String getNewPartitionName() {
        return newPartitionName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        out.writeLong(partitionId);
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_113) {
            Text.writeString(out, newDbName);
            Text.writeString(out, newTableName);
            Text.writeString(out, newPartitionName);
        }
    }

    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        partitionId = in.readLong();
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_113) {
            newDbName = Text.readString(in);
            newTableName = Text.readString(in);
            newPartitionName = Text.readString(in);
        }
    }

}
