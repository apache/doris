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

package org.apache.doris.load.sync;

import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Status;
import org.apache.doris.thrift.TStatusCode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SyncChannelHandle implements SyncChannelCallback {
    private Logger LOG = LogManager.getLogger(SyncChannelHandle.class);

    // channel id -> dummy value(-1)
    private MarkedCountDownLatch<Long, Long> latch;

    public void reset(int size) {
        this.latch = new MarkedCountDownLatch<>(size);
    }

    public void mark(SyncChannel channel) {
        latch.addMark(channel.getId(), -1L);
    }

    @Override
    public void onFinished(long channelId) {
        this.latch.markedCountDown(channelId, -1L);
    }

    @Override
    public void onFailed(String errMsg) {
        this.latch.countDownToZero(new Status(TStatusCode.CANCELLED, errMsg));
    }

    public void join() throws InterruptedException {
        this.latch.await();
    }

    public Status getStatus() {
        return latch.getStatus();
    }
}