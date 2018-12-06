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

import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * PersistInfo for ColocateTableIndex
 */
public class ColocatePersistInfo implements Writable {
    private long tableId;
    private long groupId;
    private long dbId;
    private List<List<Long>> backendsPerBucketSeq;

    public ColocatePersistInfo() {

    }

    public static ColocatePersistInfo CreateForAddTable(long tableId, long groupId, long dbId, List<List<Long>> backendsPerBucketSeq) {
        return new ColocatePersistInfo(tableId, groupId, dbId, backendsPerBucketSeq);
    }

    public static ColocatePersistInfo CreateForBackendsPerBucketSeq(long groupId, List<List<Long>> backendsPerBucketSeq) {
        return new ColocatePersistInfo(-1L, groupId, -1L, backendsPerBucketSeq);
    }

    public static ColocatePersistInfo CreateForMarkBalancing(long groupId) {
        return new ColocatePersistInfo(-1L, groupId, -1L, new ArrayList<>());
    }

    public static ColocatePersistInfo CreateForMarkStable(long groupId) {
        return new ColocatePersistInfo(-1L, groupId, -1L, new ArrayList<>());
    }

    public static ColocatePersistInfo CreateForRemoveTable(long tableId) {
        return new ColocatePersistInfo(tableId, -1L, -1L, new ArrayList<>());
    }

    public ColocatePersistInfo(long tableId, long groupId, long dbId, List<List<Long>> backendsPerBucketSeq) {
        this.tableId = tableId;
        this.groupId = groupId;
        this.dbId = dbId;
        this.backendsPerBucketSeq = backendsPerBucketSeq;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public long getGroupId() {
        return groupId;
    }

    public void setGroupId(long groupId) {
        this.groupId = groupId;
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public List<List<Long>> getBackendsPerBucketSeq() {
        return backendsPerBucketSeq;
    }

    public void setBackendsPerBucketSeq(List<List<Long>> backendsPerBucketSeq) {
        this.backendsPerBucketSeq = backendsPerBucketSeq;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(tableId);
        out.writeLong(groupId);
        out.writeLong(dbId);
        int size = backendsPerBucketSeq.size();
        out.writeInt(size);
        for (List<Long> beList : backendsPerBucketSeq) {
            out.writeInt(beList.size());
            for (Long be : beList) {
                out.writeLong(be);
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tableId = in.readLong();
        groupId = in.readLong();
        dbId = in.readLong();

        int size = in.readInt();
        backendsPerBucketSeq = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            int beListSize = in.readInt();
            List<Long> beLists = new ArrayList<>();
            for (int j = 0; j < beListSize; j++) {
                beLists.add(in.readLong());
            }
            backendsPerBucketSeq.add(beLists);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof ColocatePersistInfo)) {
            return false;
        }

        ColocatePersistInfo info = (ColocatePersistInfo) obj;

        return tableId == info.tableId
                && groupId == info.groupId
                && dbId == info.dbId
                && backendsPerBucketSeq.equals(info.backendsPerBucketSeq);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("table id: ").append(tableId);
        sb.append(" group id: ").append(groupId);
        sb.append(" db id: ").append(dbId);
        sb.append(" backendsPerBucketSeq: ").append(backendsPerBucketSeq);
        return sb.toString();
    }
}
