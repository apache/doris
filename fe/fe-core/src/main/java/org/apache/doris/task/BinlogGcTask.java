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

import org.apache.doris.thrift.TGcBinlogReq;
import org.apache.doris.thrift.TTabletGcBinlogInfo;
import org.apache.doris.thrift.TTaskType;

import java.util.ArrayList;
import java.util.List;

public class BinlogGcTask extends AgentTask {
    private List<TTabletGcBinlogInfo> tabletGcBinlogInfos = new ArrayList<>();

    public BinlogGcTask(long backendId, long signature) {
        super(null, backendId, TTaskType.GC_BINLOG, -1, -1, -1, -1, -1, signature);
    }

    public void addTask(long tabletId, long version) {
        TTabletGcBinlogInfo tabletGcBinlogInfo = new TTabletGcBinlogInfo();
        tabletGcBinlogInfo.setTabletId(tabletId);
        tabletGcBinlogInfo.setVersion(version);
        tabletGcBinlogInfos.add(tabletGcBinlogInfo);
    }

    public TGcBinlogReq toThrift() {
        TGcBinlogReq req = new TGcBinlogReq();
        req.setTabletGcBinlogInfos(tabletGcBinlogInfos);
        return req;
    }
}
