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

import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metadata.TableStats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Programmable {@link FlussAdminOps} for unit tests: canned answers plus a log of the calls made.
 *
 * <p>This is the connector's substitute for a mocking framework (which the fe-connector modules do not
 * carry). Two things it buys that a live cluster cannot: a test can put the cluster in states that are
 * awkward or slow to reach for real — a bucket whose lake offset has caught up with its log, a partition
 * present only in the lake — and it can assert <em>which</em> remote calls were made, which is how
 * per-statement memoization and partition pushdown are pinned down.
 *
 * <p>Anything a test has not programmed throws rather than returning a neutral value: silently answering
 * "empty" would let a test pass while the code under test called something nobody meant it to call.
 */
class RecordingFlussAdminOps implements FlussAdminOps {

    /** Every call made, in order, as {@code method(args)} — the assertion target for call-count tests. */
    final List<String> calls = new ArrayList<>();

    List<String> databases = Collections.emptyList();
    final Map<String, List<String>> tablesByDatabase = new HashMap<>();
    final Map<TablePath, TableInfo> tableInfos = new HashMap<>();
    final Map<TablePath, List<PartitionInfo>> partitionsByTable = new HashMap<>();
    final Map<TablePath, TableStats> statsByTable = new HashMap<>();
    /** When set, every call throws this instead of answering — the "cluster is unreachable" case. */
    RuntimeException failure;

    @Override
    public List<String> listDatabases() {
        calls.add("listDatabases()");
        return databases;
    }

    @Override
    public boolean databaseExists(String databaseName) {
        calls.add("databaseExists(" + databaseName + ")");
        return databases.contains(databaseName);
    }

    @Override
    public List<String> listTables(String databaseName) {
        calls.add("listTables(" + databaseName + ")");
        List<String> tables = tablesByDatabase.get(databaseName);
        if (tables == null) {
            throw new IllegalStateException("no tables programmed for database '" + databaseName + "'");
        }
        return tables;
    }

    @Override
    public boolean tableExists(TablePath tablePath) {
        throw notProgrammed("tableExists");
    }

    @Override
    public TableInfo getTableInfo(TablePath tablePath) {
        calls.add("getTableInfo(" + tablePath + ")");
        if (failure != null) {
            throw failure;
        }
        TableInfo tableInfo = tableInfos.get(tablePath);
        if (tableInfo == null) {
            // What a real fluss cluster answers for an unknown table; the connector discriminates on it.
            throw new TableNotExistException("Table '" + tablePath + "' does not exist.");
        }
        return tableInfo;
    }

    @Override
    public List<PartitionInfo> listPartitionInfos(TablePath tablePath) {
        calls.add("listPartitionInfos(" + tablePath + ")");
        List<PartitionInfo> partitions = partitionsByTable.get(tablePath);
        if (partitions == null) {
            throw new IllegalStateException("no partitions programmed for table '" + tablePath + "'");
        }
        return partitions;
    }

    @Override
    public List<PartitionInfo> listPartitionInfos(TablePath tablePath, PartitionSpec partialPartitionSpec) {
        throw notProgrammed("listPartitionInfos");
    }

    @Override
    public TableStats getTableStats(TablePath tablePath) {
        calls.add("getTableStats(" + tablePath + ")");
        if (failure != null) {
            throw failure;
        }
        TableStats stats = statsByTable.get(tablePath);
        if (stats == null) {
            throw new IllegalStateException("no stats programmed for table '" + tablePath + "'");
        }
        return stats;
    }

    @Override
    public KvSnapshots getLatestKvSnapshots(TablePath tablePath) {
        throw notProgrammed("getLatestKvSnapshots");
    }

    @Override
    public KvSnapshots getLatestKvSnapshots(TablePath tablePath, String partitionName) {
        throw notProgrammed("getLatestKvSnapshots");
    }

    @Override
    public LakeSnapshot getReadableLakeSnapshot(TablePath tablePath) {
        throw notProgrammed("getReadableLakeSnapshot");
    }

    @Override
    public Map<Integer, Long> listOffsets(TablePath tablePath, Collection<Integer> buckets, OffsetSpec offsetSpec) {
        throw notProgrammed("listOffsets");
    }

    @Override
    public Map<Integer, Long> listOffsets(TablePath tablePath, String partitionName,
            Collection<Integer> buckets, OffsetSpec offsetSpec) {
        throw notProgrammed("listOffsets");
    }

    private static UnsupportedOperationException notProgrammed(String method) {
        return new UnsupportedOperationException(
                method + " was called but this test programmed no answer for it");
    }
}
