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

package org.apache.doris.transaction;

import org.apache.doris.common.io.Writable;
import org.apache.doris.thrift.TTabletCommitInfo;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class TabletCommitInfo implements Writable {

    private long tabletId;
    private long backendId;

    public TabletCommitInfo(long tabletId, long backendId) {
        super();
        this.tabletId = tabletId;
        this.backendId = backendId;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getBackendId() {
        return backendId;
    }

    public static List<TabletCommitInfo> fromThrift(List<TTabletCommitInfo> tTabletCommitInfos) {
        List<TabletCommitInfo> commitInfos = Lists.newArrayList();
        for (TTabletCommitInfo tTabletCommitInfo : tTabletCommitInfos) {
            commitInfos.add(new TabletCommitInfo(tTabletCommitInfo.getTabletId(), tTabletCommitInfo.getBackendId()));
        }
        return commitInfos;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(tabletId);
        out.writeLong(backendId);
    }

    public void readFields(DataInput in) throws IOException {
        tabletId = in.readLong();
        backendId = in.readLong();
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
