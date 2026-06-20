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

package org.apache.doris.connector.paimon;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;

/**
 * Single sys-aware handle-to-{@link Table} resolver shared by the metadata read path
 * ({@link PaimonConnectorMetadata}) and the scan path ({@link PaimonScanPlanProvider}).
 *
 * <p>Both call sites used to carry their own reload-fallback. They diverged: the metadata twin was
 * made sys-aware (T17) while the scan twin still reloaded the BASE table for every handle — so a
 * deserialized system handle (transient {@link Table} lost) would silently resolve and scan the
 * base table, returning wrong rows. Collapsing both into THIS one method removes that trap: there
 * is exactly one reload rule and it is sys-aware.
 *
 * <p>Contract: prefer the handle's transient {@link Table}; on null reload from the catalog seam —
 * a {@linkplain PaimonTableHandle#isSystemTable() system handle} via the 4-arg sys
 * {@link Identifier} {@code (db, table, "main", sysName)} (so the SYSTEM table is re-fetched, not
 * the base table), a {@linkplain PaimonTableHandle#getBranchName() branch handle} via the 3-arg
 * branch {@link Identifier} {@code (db, table, branch)} (so the BRANCH table — independent schema +
 * snapshots — is fetched, not the base table), a normal handle via the 2-arg
 * {@code Identifier.create(db, table)}.
 *
 * <p>NOTE: this resolver only picks the correct (sys) Table on reload. It does NOT do
 * {@code forceJni} native-vs-JNI routing or fail-loud guards — those remain T19.
 */
final class PaimonTableResolver {

    private PaimonTableResolver() {
    }

    /**
     * Returns the handle's transient Paimon {@link Table}, or reloads it from {@code catalogOps}
     * when the transient reference is null (e.g. after a serialization round-trip across the FE/BE
     * boundary or plan reuse). A system handle reloads via the 4-arg sys {@link Identifier}; a
     * branch handle via the 3-arg branch {@link Identifier}; a normal handle via the 2-arg base
     * {@link Identifier}.
     *
     * <p>This method does NOT wrap the reload failure: each call site keeps its own
     * exception-handling/wrapping. The only checked surface is the seam's
     * {@link org.apache.paimon.catalog.Catalog.TableNotExistException}.
     *
     * @throws org.apache.paimon.catalog.Catalog.TableNotExistException if the seam reports the
     *         table is gone (callers wrap/translate as they did before).
     */
    static Table resolve(PaimonCatalogOps catalogOps, PaimonTableHandle handle)
            throws org.apache.paimon.catalog.Catalog.TableNotExistException {
        Table table = handle.getPaimonTable();
        if (table != null) {
            return table;
        }
        // Fallback reload. A sys handle MUST reload via the 4-arg sys Identifier so the SYSTEM
        // table is re-fetched, not the base table. A branch handle MUST reload via the 3-arg branch
        // Identifier so the BRANCH table (independent schema/snapshots) is fetched, not the base.
        Identifier id;
        if (handle.isSystemTable()) {
            id = new Identifier(handle.getDatabaseName(), handle.getTableName(),
                    "main", handle.getSysTableName());
        } else if (handle.getBranchName() != null) {
            // A branch read loads a DIFFERENT table (independent schema/snapshots) via the 3-arg
            // branch Identifier, mirroring legacy
            // PaimonExternalCatalog.getPaimonTable(mapping, branch, null).
            id = new Identifier(handle.getDatabaseName(), handle.getTableName(),
                    handle.getBranchName());
        } else {
            id = Identifier.create(handle.getDatabaseName(), handle.getTableName());
        }
        return catalogOps.getTable(id);
    }
}
