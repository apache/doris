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

package org.apache.doris.load;

import com.google.common.collect.Sets;
import org.apache.doris.catalog.Replica;
import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

public class TabletDeleteInfo implements Writable {
    private long tabletId;
    private Set<Replica> finishedReplicas;

    public TabletDeleteInfo() {
        // for persist
    }

    public TabletDeleteInfo(long tabletId) {
        this.tabletId = tabletId;
        this.finishedReplicas = Sets.newHashSet();
    }

    public long getTabletId() {
        return tabletId;
    }

    public Set<Replica> getFinishedReplicas() {
        return finishedReplicas;
    }

    public boolean addFinishedReplica(Replica replica) {
        finishedReplicas.add(replica);
        return true;
    }

    public static TabletDeleteInfo read(DataInput in) throws IOException {
        TabletDeleteInfo tabletDeleteInfo = new TabletDeleteInfo();
        tabletDeleteInfo.readFields(in);
        return tabletDeleteInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(tabletId);
        short size = (short) finishedReplicas.size();
        out.writeShort(size);
        for (Replica replica : finishedReplicas) {
            replica.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        tabletId = in.readLong();
        short size = in.readShort();
        for (short i = 0; i < size; i++) {
            Replica replica = Replica.read(in);
            finishedReplicas.add(replica);
        }
    }
}
