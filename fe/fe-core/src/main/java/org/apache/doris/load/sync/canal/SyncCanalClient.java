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

import org.apache.doris.load.sync.SyncChannel;

import com.alibaba.otter.canal.client.CanalConnector;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class SyncCanalClient {
    protected static Logger logger = LogManager.getLogger(SyncCanalClient.class);

    private final CanalConnector connector;
    private final CanalSyncDataReceiver receiver;
    private final CanalSyncDataConsumer consumer;

    // channel id -> channel
    private final Map<Long, CanalSyncChannel> idToChannels;

    protected ReentrantLock lock = new ReentrantLock(true);
    protected ReentrantLock getLock = new ReentrantLock();

    protected void lock() {
        lock.lock();
    }

    protected void unlock() {
        lock.unlock();
    }

    public SyncCanalClient(CanalSyncJob syncJob, String destination, CanalConnector connector,
                           int batchSize, boolean debug) {
        this(syncJob, destination, connector, batchSize, debug, ".*\\..*");
    }

    public SyncCanalClient(CanalSyncJob syncJob, String destination, CanalConnector connector,
                           int batchSize, boolean debug, String filter) {
        this.connector = connector;
        this.consumer = new CanalSyncDataConsumer(syncJob, connector, getLock, debug);
        this.receiver = new CanalSyncDataReceiver(syncJob, connector, destination, filter, consumer, batchSize, getLock);
        this.idToChannels = Maps.newHashMap();
    }

    public void startup() {
        Preconditions.checkNotNull(connector, "connector is null");
        Preconditions.checkState(!idToChannels.isEmpty(), "no channel is registered");
        lock();
        try {
            // 1. start executor
            consumer.start();
            // 2. start receiver
            receiver.start();
        } finally {
            unlock();
        }
        logger.info("canal client has been started.");
    }

    public void shutdown(boolean needCleanUp) {
        lock();
        try {
            // 1. stop receiver
            receiver.stop();
            // 2. stop executor
            consumer.stop(needCleanUp);
        } finally {
            unlock();
        }
        logger.info("canal client has been stopped.");
    }

    public void registerChannels(List<SyncChannel> channels) {
        StringBuilder channelFilters = new StringBuilder();
        for (int i = 0; i < channels.size(); i++) {
            CanalSyncChannel channel = (CanalSyncChannel) channels.get(i);
            String filter = channel.getSrcDataBase() + "." + channel.getSrcTable();
            String targetTable = channel.getTargetTable();
            channelFilters.append(filter);
            if (i < channels.size() - 1) {
                channelFilters.append(",");
            }
            idToChannels.put(channel.getId(), channel);
            logger.info("register channel, filter: {}, target table: {}", filter, targetTable);
        }
        receiver.setFilter(channelFilters.toString());
        consumer.setChannels(idToChannels);
    }

    public String getPositionInfo() {
        return consumer.getPositionInfo();
    }
}