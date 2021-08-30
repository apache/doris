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

import org.apache.doris.load.sync.SyncChannelCallback;
import org.apache.doris.task.SerialExecutorService.SerialRunnable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class SyncTask implements SerialRunnable {
    private static final Logger LOG = LogManager.getLogger(SyncTask.class);

    protected long signature;
    protected int index;
    protected SyncChannelCallback callback;

    public SyncTask(long signature, int index, SyncChannelCallback callback) {
        this.signature = signature;
        this.index = index;
        this.callback = callback;
    }

    @Override
    public void run() {
        try {
            exec();
        } catch (Exception e) {
            String errMsg = "channel " + signature + ", " + "msg: " + e.getMessage();
            LOG.error("sync task exec error: {}", errMsg);
            callback.onFailed(errMsg);
        }
    }

    public int getIndex() {
        return this.index;
    }

    /**
     * implement in child
     */
    protected abstract void exec() throws Exception;
}
