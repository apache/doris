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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import java.util.Objects;
import java.util.OptionalLong;

/**
 * One Lance table, and the dataset version a query pinned on it. A handle without a pin reads the newest
 * version in storage; the engine pins one at query begin ({@code beginQuerySnapshot} or a time-travel
 * clause) and threads it through {@code applySnapshot}, so every step of the statement reads the same
 * version, as BE does.
 */
final class LanceTableHandle implements ConnectorTableHandle {
    private static final long serialVersionUID = 1L;
    /** No version pinned: read the newest one in storage. */
    private static final long LATEST = -1;

    private final String dbName;
    private final String tableName;
    private final long version;

    LanceTableHandle(String dbName, String tableName) {
        this(dbName, tableName, LATEST);
    }

    private LanceTableHandle(String dbName, String tableName, long version) {
        this.dbName = Objects.requireNonNull(dbName, "dbName");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.version = version;
    }

    /** The same table pinned to {@code pinnedVersion}; this handle itself when it is already pinned there. */
    LanceTableHandle withVersion(long pinnedVersion) {
        return pinnedVersion == version ? this : new LanceTableHandle(dbName, tableName, pinnedVersion);
    }

    /** The same table without a pin, reading the newest version in storage. */
    LanceTableHandle unpinned() {
        return withVersion(LATEST);
    }

    /** The remote database name, which for Lance is the escaped namespace Doris shows. */
    String getDbName() {
        return dbName;
    }

    String getTableName() {
        return tableName;
    }

    /** The pinned version; empty for an unpinned handle. Lance versions start at 1. */
    OptionalLong getVersion() {
        return version > 0 ? OptionalLong.of(version) : OptionalLong.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LanceTableHandle)) {
            return false;
        }
        LanceTableHandle that = (LanceTableHandle) o;
        return version == that.version && dbName.equals(that.dbName) && tableName.equals(that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, tableName, version);
    }

    @Override
    public String toString() {
        return dbName + "." + tableName + (version > 0 ? "@" + version : "");
    }
}
