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

import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/*
 * Author: Chenmingyu
 * Date: Feb 14, 2019
 */

public class BackendTabletsInfo implements Writable {

    private long backendId;
    // tablet id -> schema hash
    private Map<Long, Integer> tabletIds = Maps.newHashMap();

    private boolean bad;

    private BackendTabletsInfo() {

    }

    public BackendTabletsInfo(long backendId) {
        this.backendId = backendId;
    }

    public void addTabletId(long tabletId, int schemaHash) {
        tabletIds.put(tabletId, schemaHash);
    }

    public long getBackendId() {
        return backendId;
    }

    public Map<Long, Integer> getTabletIds() {
        return tabletIds;
    }

    public void setBad(boolean bad) {
        this.bad = bad;
    }

    public boolean isBad() {
        return bad;
    }

    public boolean isEmpty() {
        return tabletIds.isEmpty();
    }

    public static BackendTabletsInfo read(DataInput in) throws IOException {
        BackendTabletsInfo backendTabletsInfo = new BackendTabletsInfo();
        backendTabletsInfo.readFields(in);
        return backendTabletsInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(backendId);
        out.writeInt(tabletIds.size());
        for (Map.Entry<Long, Integer> entry : tabletIds.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeInt(entry.getValue());
        }

        out.writeBoolean(bad);

        // this is for further extension
        out.writeBoolean(false);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        backendId = in.readLong();
        int size = in.readInt();

        for (int i = 0; i < size; i++) {
            long tabletId = in.readLong();
            int schemaHash = in.readInt();
            tabletIds.put(tabletId, schemaHash);
        }

        bad = in.readBoolean();

        if (in.readBoolean()) {

        }
    }

}
