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

import org.apache.doris.thrift.TClearAutoIncCacheReq;
import org.apache.doris.thrift.TTaskType;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ClearAutoIncCacheTask extends AgentTask {
    private Map<Long, List<Long>> tables;
    CountDownLatch latch;

    public ClearAutoIncCacheTask(long backendId, Map<Long, List<Long>> tables) {
        super(null, backendId, TTaskType.CLEAR_AUTO_INC_CACHE, -1, -1, -1, -1, -1, -1, -1);
        this.tables = tables;
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public void countDown() {
        if (latch != null) {
            latch.countDown();
        }
    }

    public TClearAutoIncCacheReq toThrift() {
        TClearAutoIncCacheReq req = new TClearAutoIncCacheReq();
        req.setTables(tables);
        return req;
    }
}
