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

package org.apache.doris.load.sync.debezium;

import org.apache.doris.load.sync.SyncChannel;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class SyncMySQLClient {
    protected static Logger logger = LogManager.getLogger(SyncMySQLClient.class);

    private final DebeziumSyncDataReceiver receiver;
    private final DebeziumSyncDataConsumer consumer;

    // channel id -> channel
    private final Map<Long, DebeziumSyncChannel> idToChannels;

    protected ReentrantLock lock = new ReentrantLock(true);
    protected ReentrantLock getLock = new ReentrantLock();

    protected void lock() {
        lock.lock();
    }

    protected void unlock() {
        lock.unlock();
    }

    public SyncMySQLClient(DebeziumSyncJob syncJob, boolean debug) {
        this.consumer = new DebeziumSyncDataConsumer(syncJob, debug);
        this.receiver = new DebeziumSyncDataReceiver(syncJob, consumer);
        this.idToChannels = Maps.newHashMap();
    }

    public void startup() {
        Preconditions.checkState(!idToChannels.isEmpty(), "no channel is registered");
        lock();
        try {
            receiver.start();
            consumer.start();
        } finally {
            unlock();
        }
        logger.info("mysql client has been started.");
    }

    public void shutdown(boolean needCleanUp) {
        lock();
        try {
            receiver.stop();
            consumer.stop();
        } finally {
            unlock();
        }
        logger.info("mysql client has been stopped.");
    }

    public void registerChannels(List<SyncChannel> channels) {
        StringBuilder channelFilters = new StringBuilder();
        for (int i = 0; i < channels.size(); i++) {
            DebeziumSyncChannel channel = (DebeziumSyncChannel) channels.get(i);
            String filter = channel.getSrcDataBase() + "." + channel.getSrcTable();
            String targetTable = channel.getTargetTable();
            channelFilters.append(filter);
            if (i < channels.size() - 1) {
                channelFilters.append(",");
            }
            idToChannels.put(channel.getId(), channel);
            logger.info("register channel, filter: {}, target table: {}", filter, targetTable);
        }
        consumer.setChannels(idToChannels);
    }
}