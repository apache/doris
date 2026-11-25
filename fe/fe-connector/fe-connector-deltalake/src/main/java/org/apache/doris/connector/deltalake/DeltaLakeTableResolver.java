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

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** Resolves catalog names to table roots; schema and partitions are always read from the Delta log. */
interface DeltaLakeTableResolver extends Closeable {
    List<String> listDatabases() throws IOException;

    List<String> listTables(String database) throws IOException;

    Optional<DeltaLakeTableReference> tableLocation(String database, String table) throws IOException;

    default Snapshot managedSnapshot(Engine engine, String database, String table, DeltaLakeTableReference reference,
            Optional<Long> version) {
        throw new UnsupportedOperationException("Catalog-managed snapshots require Unity Catalog");
    }

    default CommitRange managedCommitRange(Engine engine, String database, String table,
            DeltaLakeTableReference reference, long start, long end) {
        throw new UnsupportedOperationException("Catalog-managed commit history requires Unity Catalog");
    }

    @Override
    default void close() throws IOException {
    }
}
