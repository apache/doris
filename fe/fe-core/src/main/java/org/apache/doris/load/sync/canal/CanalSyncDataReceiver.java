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

package org.apache.doris.load.sync.canal;

import org.apache.doris.load.sync.SyncDataReceiver;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class CanalSyncDataReceiver extends SyncDataReceiver {
    private static Logger LOG = LogManager.getLogger(CanalSyncDataReceiver.class);

    private CanalSyncJob syncJob;
    private CanalConnector connector;
    private ReentrantLock getLock;
    private CanalSyncDataConsumer consumer;
    private String destination;
    private String filter;
    private long sleepTimeMs;

    public CanalSyncDataReceiver(CanalSyncJob syncJob, CanalConnector connector, String destination, String filter,
                                 CanalSyncDataConsumer consumer, int readBatchSize, ReentrantLock getLock) {
        super(readBatchSize);
        this.syncJob = syncJob;
        this.connector = connector;
        this.consumer = consumer;
        this.destination = destination;
        this.filter = filter;
        this.getLock = getLock;
        this.sleepTimeMs = 20L;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    @Override
    public void start() {
        super.start();
        LOG.info("receiver has been started. destination: {}, filter: {}, batch size: {}",
                destination, filter, readBatchSize);
    }

    @Override
    public void process() {
        while (running) {
            try {
                connector.connect();
                connector.subscribe(filter);
                connector.rollback();
                while (running) {
                    int size;
                    //  get one batch
                    Message message;
                    holdGetLock();
                    try {
                        message = connector.getWithoutAck(readBatchSize,
                                CanalConfigs.getWaitingTimeoutMs, TimeUnit.MILLISECONDS);
                    } finally {
                        releaseGetLock();
                    }

                    if (message.isRaw()) {
                        size = message.getRawEntries().size();
                    } else {
                        size = message.getEntries().size();
                    }
                    if (message.getId() == -1 || size == 0) {
                        try {
                            Thread.sleep(sleepTimeMs);
                        } catch (InterruptedException e) {
                            // do nothing
                        }
                    } else {
                        consumer.put(message, size); // submit batch to consumer
                    }
                }
            } catch (Throwable e) {
                LOG.error("Receiver is error. {}", e.getMessage());
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e1) {
                    // ignore
                }
            } finally {
                connector.disconnect();
            }
        }
    }

    private void holdGetLock() {
        getLock.lock();
    }

    private void releaseGetLock() {
        getLock.unlock();
    }
}
