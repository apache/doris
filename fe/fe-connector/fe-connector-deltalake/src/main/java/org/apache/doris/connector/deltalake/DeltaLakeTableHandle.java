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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.engine.Engine;

/** Immutable statement coordinates; the in-memory snapshot avoids replay throughout planning. */
final class DeltaLakeTableHandle implements ConnectorTableHandle {
    private static final long serialVersionUID = 1L;

    private final String database;
    private final String table;
    private final String path;
    private final String tableId;
    private final long version;
    private final transient Snapshot snapshot;

    DeltaLakeTableHandle(String database, String table, Snapshot snapshot) {
        this.database = database;
        this.table = table;
        this.path = snapshot.getPath();
        this.tableId = DeltaKernelCompat.tableId(snapshot);
        this.version = snapshot.getVersion();
        this.snapshot = snapshot;
    }

    String getDatabase() {
        return database;
    }

    String getTable() {
        return table;
    }

    String getPath() {
        return path;
    }

    String getTableId() {
        return tableId;
    }

    long getVersion() {
        return version;
    }

    DeltaLakeTableHandle atVersion(Engine engine, long requestedVersion) {
        if (version == requestedVersion) {
            return this;
        }
        Snapshot pinned = TableManager.loadSnapshot(path).atVersion(requestedVersion).build(engine);
        if (!tableId.equals(DeltaKernelCompat.tableId(pinned))) {
            throw new DorisConnectorException("Delta table was recreated during the statement: " + path);
        }
        return new DeltaLakeTableHandle(database, table, pinned);
    }

    Snapshot snapshot(Engine engine) {
        if (snapshot != null) {
            return snapshot;
        }
        Snapshot reloaded = TableManager.loadSnapshot(path).atVersion(version).build(engine);
        // A recreated table can reuse version 0 at the same path; it is not the statement's table.
        if (!tableId.equals(DeltaKernelCompat.tableId(reloaded))) {
            throw new DorisConnectorException("Delta table was recreated during the statement: " + path);
        }
        return reloaded;
    }
}
