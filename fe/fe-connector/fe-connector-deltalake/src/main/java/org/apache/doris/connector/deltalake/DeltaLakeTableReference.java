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

import io.delta.kernel.Snapshot;
import io.delta.kernel.SnapshotBuilder;
import io.delta.kernel.TableManager;
import io.delta.kernel.engine.Engine;

import java.io.Serializable;
import java.util.Optional;

/** A registration identity travels with pinned handles. It contains no access credentials. */
final class DeltaLakeTableReference implements Serializable {
    private static final long serialVersionUID = 1L;
    final String path;
    final String registrationId;
    final boolean managed;
    // Each resolved handle belongs to its caller. Neither cloud credentials nor their engine are serialized.
    transient Engine vendedEngine;

    DeltaLakeTableReference(String path, String registrationId, boolean managed) {
        this.path = path;
        this.registrationId = registrationId;
        this.managed = managed;
    }

    static DeltaLakeTableReference filesystem(String path) {
        return new DeltaLakeTableReference(path, null, false);
    }

    Snapshot load(Engine engine, DeltaLakeTableResolver resolver, String database, String table,
            Optional<Long> version) {
        Snapshot snapshot;
        if (managed) {
            snapshot = resolver.managedSnapshot(engine, database, table, this, version, Optional.empty());
        } else {
            SnapshotBuilder builder = TableManager.loadSnapshot(path);
            snapshot = version.map(builder::atVersion).orElse(builder).build(engine);
        }
        DeltaLakeFeatureCheck.check(snapshot, managed);
        return snapshot;
    }

    Snapshot loadAtTimestamp(Engine engine, DeltaLakeTableResolver resolver, String database, String table,
            long timestampMillis, String expectedTableId) {
        Snapshot snapshot;
        if (managed) {
            snapshot = resolver.managedSnapshot(engine, database, table, this, Optional.empty(),
                    Optional.of(timestampMillis));
        } else {
            // Load latest fresh so a cached statement handle does not hide newer commits or an ICT boundary.
            Snapshot latest = TableManager.loadSnapshot(path).build(engine);
            if (!expectedTableId.equals(DeltaKernelCompat.tableId(latest))) {
                throw new DorisConnectorException("Delta table was recreated during the statement: " + path);
            }
            snapshot = TableManager.loadSnapshot(path).atTimestamp(timestampMillis, latest).build(engine);
        }
        DeltaLakeFeatureCheck.check(snapshot, managed);
        return snapshot;
    }
}
