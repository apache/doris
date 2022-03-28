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

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DebeziumSyncDataReceiver {
    private static final Logger LOG = LogManager.getLogger(DebeziumSyncDataReceiver.class);

    private final String ip;
    private final int port;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;

    public DebeziumSyncDataReceiver(DebeziumSyncJob syncJob, DebeziumSyncDataConsumer consumer) {
        this.ip = syncJob.getRemote().getIp();
        this.port = syncJob.getRemote().getPort();
        Properties props = new Properties();
        props.setProperty("name", "mysql_test_engine");
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");

        props.setProperty("offset.storage", DebeziumOffsetBackingStore.class.getCanonicalName());
        String offsetState = syncJob.getStatus();
        if (offsetState != null) {
            props.setProperty(DebeziumOffsetBackingStore.OFFSET_STATE_VALUE, offsetState);
        }
        props.setProperty("database.history", DebeziumDatabaseHistory.class.getCanonicalName());
        String engineInstanceName = UUID.randomUUID().toString();
        DebeziumDatabaseHistory.registerEmptyHistoryRecord(engineInstanceName);
        props.setProperty(DebeziumDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME, engineInstanceName);
        props.setProperty("database.history.store.only.monitored.tables.ddl", "true");

        props.setProperty("offset.flush.interval.ms", String.valueOf(Long.MAX_VALUE));
        props.setProperty("tombstones.on.delete", "false");
        props.setProperty("include.schema.changes", "false");
        props.setProperty("key.converter.schemas.enable", "false");
        props.setProperty("value.converter.schemas.enable", "false");

        /* begin connector properties */
        props.setProperty("database.hostname", ip);
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("database.user", syncJob.getUsername());
        props.setProperty("database.password", syncJob.getPassword());
        props.setProperty("database.server.name", "my-app-connector");

        engine = DebeziumEngine.create(Connect.class)
                .using(props)
                .notifying(consumer)
                .build();
    }

    public void start() {
        LOG.info("receiver has been started. destination: {}:{}", ip, port);
        executor.execute(engine);
    }

    public void stop() {
        LOG.info("receiver has been stopped. destination: {}:{}", ip, port);
        try {
            engine.close();
            executor.shutdown();
        } catch (IOException e) {
            LOG.warn(e.toString());
        }
    }

}
