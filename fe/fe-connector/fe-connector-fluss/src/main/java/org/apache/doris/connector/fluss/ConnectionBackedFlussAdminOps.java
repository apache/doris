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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metadata.TableStats;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link FlussAdminOps} over a live fluss {@link Connection}: the one place in this connector that
 * deals with futures, timeouts and error wrapping.
 *
 * <p>The connection is owned by {@link FlussConnector} and outlives this object, which is created per
 * statement. {@code Connection#getAdmin()} is a memoized, connection-scoped singleton, so it is fetched
 * per call and never closed here — closing it would pull the admin out from under every other statement
 * on the same catalog.
 */
public class ConnectionBackedFlussAdminOps implements FlussAdminOps {

    /**
     * How long any single metadata call may take before it is abandoned. These are small RPCs against
     * the coordinator, so a wait this long means the cluster is unreachable rather than busy, and the
     * caller is a query that should say so instead of hanging.
     */
    private static final long TIMEOUT_MS = 60_000L;

    private final Connection connection;
    private final String catalogName;
    private final String bootstrapServers;

    public ConnectionBackedFlussAdminOps(Connection connection, String catalogName, String bootstrapServers) {
        this.connection = connection;
        this.catalogName = catalogName;
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public List<String> listDatabases() {
        return await(admin().listDatabases(), "listDatabases");
    }

    @Override
    public boolean databaseExists(String databaseName) {
        return await(admin().databaseExists(databaseName), "databaseExists(" + databaseName + ")");
    }

    @Override
    public List<String> listTables(String databaseName) {
        return await(admin().listTables(databaseName), "listTables(" + databaseName + ")");
    }

    @Override
    public boolean tableExists(TablePath tablePath) {
        return await(admin().tableExists(tablePath), "tableExists(" + tablePath + ")");
    }

    @Override
    public TableInfo getTableInfo(TablePath tablePath) {
        return await(admin().getTableInfo(tablePath), "getTableInfo(" + tablePath + ")");
    }

    @Override
    public List<PartitionInfo> listPartitionInfos(TablePath tablePath) {
        return await(admin().listPartitionInfos(tablePath), "listPartitionInfos(" + tablePath + ")");
    }

    @Override
    public List<PartitionInfo> listPartitionInfos(TablePath tablePath, PartitionSpec partialPartitionSpec) {
        return await(admin().listPartitionInfos(tablePath, partialPartitionSpec),
                "listPartitionInfos(" + tablePath + ", " + partialPartitionSpec + ")");
    }

    @Override
    public TableStats getTableStats(TablePath tablePath) {
        return await(admin().getTableStats(tablePath), "getTableStats(" + tablePath + ")");
    }

    @Override
    public KvSnapshots getLatestKvSnapshots(TablePath tablePath) {
        return await(admin().getLatestKvSnapshots(tablePath), "getLatestKvSnapshots(" + tablePath + ")");
    }

    @Override
    public KvSnapshots getLatestKvSnapshots(TablePath tablePath, String partitionName) {
        return await(admin().getLatestKvSnapshots(tablePath, partitionName),
                "getLatestKvSnapshots(" + tablePath + ", " + partitionName + ")");
    }

    @Override
    public LakeSnapshot getReadableLakeSnapshot(TablePath tablePath) {
        return await(admin().getReadableLakeSnapshot(tablePath), "getReadableLakeSnapshot(" + tablePath + ")");
    }

    @Override
    public Map<Integer, Long> listOffsets(TablePath tablePath, Collection<Integer> buckets, OffsetSpec offsetSpec) {
        return await(admin().listOffsets(tablePath, buckets, offsetSpec).all(),
                "listOffsets(" + tablePath + ", " + buckets + ")");
    }

    @Override
    public Map<Integer, Long> listOffsets(TablePath tablePath, String partitionName,
            Collection<Integer> buckets, OffsetSpec offsetSpec) {
        return await(admin().listOffsets(tablePath, partitionName, buckets, offsetSpec).all(),
                "listOffsets(" + tablePath + ", " + partitionName + ", " + buckets + ")");
    }

    private Admin admin() {
        return connection.getAdmin();
    }

    private <T> T await(CompletableFuture<T> future, String operation) {
        try {
            return future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DorisConnectorException(describe(operation, "was interrupted"), e);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new DorisConnectorException(describe(operation, "timed out after " + TIMEOUT_MS + " ms"), e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                // Fluss's own errors (ApiException subclasses) are RuntimeExceptions, and callers
                // discriminate on their type — LakeTableSnapshotNotExistException is what tells scan
                // planning that this table has nothing readable in the lake yet. Wrapping them here
                // would turn that decision into string matching, so they travel unchanged.
                throw (RuntimeException) cause;
            }
            throw new DorisConnectorException(describe(operation, "failed"), cause == null ? e : cause);
        }
    }

    private String describe(String operation, String outcome) {
        return "fluss catalog '" + catalogName + "': " + operation + " " + outcome
                + " (" + FlussCatalogProperties.BOOTSTRAP_SERVERS + "=" + bootstrapServers + ")";
    }
}
