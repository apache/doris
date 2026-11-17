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

package org.apache.doris.connector.mongodb;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.thrift.TMongoSnapshot;

import org.bson.BsonTimestamp;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/** Owned source identity and a single statement-wide, server-selected snapshot timestamp. */
final class MongoSnapshot {
    private static final String STATE_KEY = "mongodb.statement.snapshot";

    static TMongoSnapshot prepare(ConnectorSession session, MongoCatalogProperties properties,
            MongoTableHandle table, Supplier<MongoCatalogClient> client, String concern,
            String preference, long timeoutMs) {
        var scope = session.getStatementScope();
        if (!"snapshot".equals(concern)) {
            if (scope != ConnectorStatementScope.NONE) {
                scope.computeIfAbsent(STATE_KEY, () -> new State(timeoutMs)).ordinaryRead();
            }
            return null;
        }
        if ("view".equals(table.getObjectKind())) {
            throw new IllegalArgumentException("MongoDB snapshot reads do not support standard views");
        }
        if (scope == ConnectorStatementScope.NONE) {
            throw new IllegalArgumentException("MongoDB snapshot reads require a live statement scope");
        }
        if ("collection".equals(table.getObjectKind()) && table.getUuid().isEmpty()) {
            throw new IllegalArgumentException("MongoDB snapshot reads require collection identity metadata");
        }
        if ("timeseries".equals(table.getObjectKind()) && !table.getOptions().containsKey("timeseries")) {
            throw new IllegalArgumentException("MongoDB snapshot reads require time-series definition metadata");
        }
        State state = scope.computeIfAbsent(STATE_KEY, () -> new State(timeoutMs));
        Source source = scope.computeIfAbsent("mongodb.snapshot.source:" + session.getCatalogId()
                + ":" + properties.configurationVersion(), () -> client.get().snapshotSource(state.remainingMillis()));
        BsonTimestamp time = state.pin(source, () -> client.get().selectSnapshot(table.getDatabase(), table.getName(),
                preference, state.remainingMillis()));
        // Copy plain values into each range. Driver sessions/cursors stay local, and neither BE nor
        // a different catalog should mutate the statement's selected timestamp or source identity.
        TMongoSnapshot result = new TMongoSnapshot();
        // BSON Timestamp has unsigned 32-bit seconds and an ordering counter, not date milliseconds.
        result.setTimestampSeconds(Integer.toUnsignedLong(time.getTime()));
        result.setTimestampIncrement(Integer.toUnsignedLong(time.getInc()));
        result.setSourceKind(source.kind());
        result.setSourceId(source.id());
        result.setTopologyVersion(source.topologyVersion());
        result.setMaxWireVersion(source.maxWireVersion());
        if ("timeseries".equals(table.getObjectKind())) {
            result.setTimeseriesOptionsJson(table.getOptions().toJson(
                    JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build()));
        }
        return result;
    }

    static long remainingMillis(ConnectorSession session) {
        State state = session.getStatementScope().getIfPresent(STATE_KEY);
        if (state == null) {
            throw new IllegalArgumentException("MongoDB statement snapshot is not pinned");
        }
        return state.remainingMillis();
    }

    record Source(String kind, String id, long topologyVersion, int maxWireVersion) {
        Source {
            if (!Set.of("replica_set", "mongos").contains(kind) || id == null || !id.matches("[0-9a-f]{24}")
                    || !Set.of(21, 25).contains(maxWireVersion)
                    || ("replica_set".equals(kind) ? topologyVersion <= 0 : topologyVersion != 0)) {
                throw new IllegalArgumentException(
                        "MongoDB snapshot source is not a qualified MongoDB 7.0/8.0 deployment");
            }
        }
    }

    static final class State {
        private final long deadlineNanos;
        private boolean ordinaryReads;
        private Source source;
        private CompletableFuture<BsonTimestamp> timestamp;

        State(long timeoutMs) {
            deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        }

        synchronized void ordinaryRead() {
            if (timestamp != null) {
                throw new IllegalArgumentException(
                        "MongoDB snapshot and non-snapshot scans cannot share a statement");
            }
            ordinaryReads = true;
        }

        long remainingMillis() {
            if (Thread.currentThread().isInterrupted()) {
                throw new CancellationException("MongoDB snapshot planning interrupted");
            }
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                throw new DorisConnectorException("MongoDB snapshot planning deadline exceeded");
            }
            return Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining));
        }

        BsonTimestamp pin(Source candidate, Supplier<BsonTimestamp> loader) {
            remainingMillis();
            CompletableFuture<BsonTimestamp> selected;
            boolean owner;
            synchronized (this) {
                if (ordinaryReads) {
                    throw new IllegalArgumentException(
                        "MongoDB snapshot and non-snapshot scans cannot share a statement");
                }
                if (source != null && !source.equals(candidate)) {
                    throw new IllegalArgumentException(
                            "MongoDB statement snapshot source identity or topology changed");
                }
                source = candidate;
                owner = timestamp == null;
                if (owner) {
                    timestamp = new CompletableFuture<>();
                }
                selected = timestamp;
            }
            // Only reservation is synchronized. Remote selection must not hold the shared state lock:
            // other scan planners still need to validate source identity or reject mixed policies.
            if (owner) {
                try {
                    BsonTimestamp value = loader.get();
                    if (value == null || value.getTime() == 0) {
                        throw new DorisConnectorException("MongoDB did not return a usable snapshot timestamp");
                    }
                    selected.complete(value);
                } catch (RuntimeException failure) {
                    // A failed selection stays failed for the whole statement; never select a newer T.
                    selected.completeExceptionally(failure);
                }
            }
            try {
                return selected.get(remainingMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new CancellationException("MongoDB snapshot planning interrupted");
            } catch (TimeoutException timeout) {
                throw new DorisConnectorException("MongoDB snapshot planning deadline exceeded");
            } catch (ExecutionException failure) {
                if (failure.getCause() instanceof RuntimeException runtime) {
                    throw runtime;
                }
                throw new DorisConnectorException("MongoDB snapshot selection failed");
            }
        }
    }
}
