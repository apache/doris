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
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Writable;

import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * PersistInfo for ColocateTableIndex
 */
public class ColocatePersistInfo implements Writable {
    private GroupId groupId;
    private long tableId;
    private List<List<Long>> backendsPerBucketSeq = Lists.newArrayList();

    public ColocatePersistInfo() {

    }

    public static ColocatePersistInfo createForAddTable(GroupId groupId, long tableId, List<List<Long>> backendsPerBucketSeq) {
        return new ColocatePersistInfo(groupId, tableId, backendsPerBucketSeq);
    }

    public static ColocatePersistInfo createForBackendsPerBucketSeq(GroupId groupId,
            List<List<Long>> backendsPerBucketSeq) {
        return new ColocatePersistInfo(groupId, -1L, backendsPerBucketSeq);
    }

    public static ColocatePersistInfo createForMarkUnstable(GroupId groupId) {
        return new ColocatePersistInfo(groupId, -1L, new ArrayList<>());
    }

    public static ColocatePersistInfo createForMarkStable(GroupId groupId) {
        return new ColocatePersistInfo(groupId, -1L, new ArrayList<>());
    }

    public static ColocatePersistInfo createForRemoveTable(long tableId) {
        return new ColocatePersistInfo(new GroupId(-1, -1), tableId, new ArrayList<>());
    }

    private ColocatePersistInfo(GroupId groupId, long tableId, List<List<Long>> backendsPerBucketSeq) {
        this.groupId = groupId;
        this.tableId = tableId;
        this.backendsPerBucketSeq = backendsPerBucketSeq;
    }

    public long getTableId() {
        return tableId;
    }

    public GroupId getGroupId() {
        return groupId;
    }

    public List<List<Long>> getBackendsPerBucketSeq() {
        return backendsPerBucketSeq;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(tableId);
        groupId.write(out);
        // out.writeLong(groupId);
        // out.writeLong(dbId);
        int size = backendsPerBucketSeq.size();
        out.writeInt(size);
        for (List<Long> beList : backendsPerBucketSeq) {
            out.writeInt(beList.size());
            for (Long be : beList) {
                out.writeLong(be);
            }
        }
    }

    public void readFields(DataInput in) throws IOException {
        tableId = in.readLong();
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_55) {
            long grpId = in.readLong();
            long dbId = in.readLong();
            groupId = new GroupId(dbId, grpId);
        } else {
            groupId = GroupId.read(in);
        }

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
                && groupId.equals(info.groupId)
                && backendsPerBucketSeq.equals(info.backendsPerBucketSeq);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("table id: ").append(tableId);
        sb.append(" group id: ").append(groupId);
        sb.append(" backendsPerBucketSeq: ").append(backendsPerBucketSeq);
        return sb.toString();
    }
}
