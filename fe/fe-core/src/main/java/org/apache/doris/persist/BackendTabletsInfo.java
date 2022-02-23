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

import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Writable;

import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

@Deprecated
// replaced by BackendReplicaInfo
public class BackendTabletsInfo implements Writable {

    private long backendId;
    // tablet id , schema hash
    // this structure is deprecated and be replaced by 'replicaPersistInfos'
    @Deprecated
    private List<Pair<Long, Integer>> tabletSchemaHash = Lists.newArrayList();

    private boolean bad;

    private List<ReplicaPersistInfo> replicaPersistInfos = Lists.newArrayList();

    private BackendTabletsInfo() {

    }

    public BackendTabletsInfo(long backendId) {
        this.backendId = backendId;
    }

    public void addReplicaInfo(ReplicaPersistInfo info) {
        replicaPersistInfos.add(info);
    }

    public List<ReplicaPersistInfo> getReplicaPersistInfos() {
        return replicaPersistInfos;
    }

    public long getBackendId() {
        return backendId;
    }

    public List<Pair<Long, Integer>> getTabletSchemaHash() {
        return tabletSchemaHash;
    }

    public void setBad(boolean bad) {
        this.bad = bad;
    }

    public boolean isBad() {
        return bad;
    }

    public boolean isEmpty() {
        return tabletSchemaHash.isEmpty() && replicaPersistInfos.isEmpty();
    }

    public static BackendTabletsInfo read(DataInput in) throws IOException {
        BackendTabletsInfo backendTabletsInfo = new BackendTabletsInfo();
        backendTabletsInfo.readFields(in);
        return backendTabletsInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(backendId);
        out.writeInt(tabletSchemaHash.size());
        for (Pair<Long, Integer> pair : tabletSchemaHash) {
            out.writeLong(pair.first);
            out.writeInt(pair.second);
        }

        out.writeBoolean(bad);

        // this is for further extension
        out.writeBoolean(true);
        out.writeInt(replicaPersistInfos.size());
        for (ReplicaPersistInfo info : replicaPersistInfos) {
            info.write(out);
        }
        // this is for further extension
        out.writeBoolean(false);
    }

    public void readFields(DataInput in) throws IOException {
        backendId = in.readLong();

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            long tabletId = in.readLong();
            int schemaHash = in.readInt();
            tabletSchemaHash.add(Pair.create(tabletId, schemaHash));
        }

        bad = in.readBoolean();

        if (in.readBoolean()) {
            size = in.readInt();
            for (int i = 0; i < size; i++) {
                ReplicaPersistInfo replicaPersistInfo = ReplicaPersistInfo.read(in);
                replicaPersistInfos.add(replicaPersistInfo);
            }
        } else {
            replicaPersistInfos = Lists.newArrayList();
        }

        if (in.readBoolean()) {

        }
    }

}
