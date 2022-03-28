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

import io.debezium.embedded.EmbeddedEngine;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DebeziumOffsetBackingStore implements OffsetBackingStore {
    private static final Logger LOG = LoggerFactory.getLogger(DebeziumOffsetBackingStore.class);

    public static final String OFFSET_STATE_VALUE = "offset.storage.debezium.state.value";
    public static final int FLUSH_TIMEOUT_SECONDS = 10;

    protected Map<ByteBuffer, ByteBuffer> data = new HashMap<>();
    protected ExecutorService executor;

    @Override
    public void configure(WorkerConfig config) {
        // eagerly initialize the executor, because OffsetStorageWriter will use it later
        start();

        Map<String, ?> conf = config.originals();
        if (!conf.containsKey(OFFSET_STATE_VALUE)) {
            // a normal startup from clean state, not need to initialize the offset
            return;
        }

        String stateJson = (String) conf.get(OFFSET_STATE_VALUE);
        DebeziumStateSerializer serializer = new DebeziumStateSerializer();
        DebeziumState debeziumState;
        try {
            debeziumState = serializer.deserialize(stateJson.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            LOG.error("Can't deserialize debezium offset state from JSON: " + stateJson, e);
            throw new RuntimeException(e);
        }

        String engineName = (String) conf.get(EmbeddedEngine.ENGINE_NAME.name());
        Converter keyConverter = new JsonConverter();
        Converter valueConverter = new JsonConverter();
        keyConverter.configure(config.originals(), true);
        Map<String, Object> valueConfigs = new HashMap<>(conf);
        valueConfigs.put("schemas.enable", false);
        valueConverter.configure(valueConfigs, true);
        OffsetStorageWriter offsetWriter = new OffsetStorageWriter(
                this,
                // must use engineName as namespace to align with Debezium Engine implementation
                engineName,
                keyConverter,
                valueConverter);

        offsetWriter.offset(debeziumState.sourcePartition, debeziumState.sourceOffset);

        // flush immediately
        if (!offsetWriter.beginFlush()) {
            // if nothing is needed to be flushed, there must be something wrong with the initialization
            LOG.warn("Initialize DebeziumOffsetBackingStore from empty offset state, this shouldn't happen.");
            return;
        }

        // trigger flushing
        Future<Void> flushFuture = offsetWriter.doFlush((error, result) -> {
            if (error != null) {
                LOG.error("Failed to flush initial offset.", error);
            } else {
                LOG.debug("Successfully flush initial offset.");
            }
        });

        // wait until flushing finished
        try {
            flushFuture.get(FLUSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            LOG.info("Flush offsets successfully, partition: {}, offsets: {}",
                    debeziumState.sourcePartition,
                    debeziumState.sourceOffset);
        } catch (InterruptedException e) {
            LOG.warn("Flush offsets interrupted, cancelling.", e);
            offsetWriter.cancelFlush();
        } catch (ExecutionException e) {
            LOG.error("Flush offsets threw an unexpected exception.", e);
            offsetWriter.cancelFlush();
        } catch (TimeoutException e) {
            LOG.error("Timed out waiting to flush offsets to storage.", e);
            offsetWriter.cancelFlush();
        }
    }

    @Override
    public void start() {
        if (executor == null) {
            executor = Executors.newSingleThreadExecutor();
        }
    }

    @Override
    public void stop() {
        if (executor != null) {
            executor.shutdown();
            // Best effort wait for any get() and set() tasks (and caller's callbacks) to complete.
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (!executor.shutdownNow().isEmpty()) {
                throw new ConnectException("Failed to stop DebeziumOffsetBackingStore. Exiting without cleanly " +
                        "shutting down pending tasks and/or callbacks.");
            }
            executor = null;
        }
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys) {
        return executor.submit(() -> {
            Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
            for (ByteBuffer key : keys) {
                result.put(key, data.get(key));
            }
            return result;
        });

    }

    @Override
    public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values,
                            final Callback<Void> callback) {
        return executor.submit(() -> {
            for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
                data.put(entry.getKey(), entry.getValue());
            }
            if (callback != null) {
                callback.onCompletion(null, null);
            }
            return null;
        });
    }

}

