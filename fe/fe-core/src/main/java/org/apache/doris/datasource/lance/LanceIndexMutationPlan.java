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

package org.apache.doris.datasource.lance;

import org.apache.doris.datasource.lance.index.LanceIndexMutationExecutor;
import org.apache.doris.thrift.TLanceIndexMutationType;

import java.util.Map;

/**
 * The execution plan one admitted Lance index mutation statement carries out of admission
 * (design v6 sections 2.2 and 5.1): every input the synchronous executor needs, pinned at
 * capture time, plus the entry-internal final preflight that re-decides the authoritative
 * no-op/existence verdict against current metadata after the active-name entry is acquired
 * and before the single dispatch.
 *
 * <p>The plan is the handoff artifact of the three-phase statement shape:
 * <ol>
 * <li><b>Capture</b> (admission, inside the {@code withLanceIndexAdmission} critical
 *     section): static validation, one pinned snapshot, name resolution, IF preflight —
 *     the rejection semantics are unchanged, and the plan is produced by purely local work
 *     so the critical section stays short.
 * <li><b>Execution</b> (statement layer, outside every catalog lock): the statement builds
 *     the executor's {@link LanceIndexMutationExecutor.MutationRequest} from this plan plus
 *     the statement-owned inputs (budget deadline, cancel signal, mutated table) and runs it.
 * <li><b>Final preflight</b> (inside the executor entry, before any budget is spent): the
 *     carried supplier re-reads authoritative metadata and re-decides with exactly the
 *     admission semantics — a no-op completes with zero dispatches, a mismatch keeps its own
 *     typed rejection, and only a fresh PROCEED verdict may dispatch.
 * </ol>
 *
 * <p>Immutable; built only by {@link LanceIndexAdmission}, which has already validated every
 * field, so the plan trusts its inputs exactly like the executor request does.
 */
public final class LanceIndexMutationPlan {

    private final LanceExternalCatalog catalog;
    private final TLanceIndexMutationType mutationType;
    private final String datasetUri;
    private final long admittedDatasetVersion;
    private final String normalizedIndexName;
    private final String columnName;
    private final String indexType;
    private final Map<String, String> properties;
    private final boolean ifNotExists;
    private final boolean ifExists;
    private final LanceIndexSchemaContract schemaContract;
    private final LanceIndexMutationExecutor.FinalPreflight finalPreflight;

    LanceIndexMutationPlan(LanceExternalCatalog catalog, TLanceIndexMutationType mutationType,
            String datasetUri, long admittedDatasetVersion, String normalizedIndexName,
            String columnName, String indexType, Map<String, String> properties,
            boolean ifNotExists, boolean ifExists, LanceIndexSchemaContract schemaContract,
            LanceIndexMutationExecutor.FinalPreflight finalPreflight) {
        this.catalog = catalog;
        this.mutationType = mutationType;
        this.datasetUri = datasetUri;
        this.admittedDatasetVersion = admittedDatasetVersion;
        this.normalizedIndexName = normalizedIndexName;
        this.columnName = columnName;
        this.indexType = indexType;
        this.properties = properties;
        this.ifNotExists = ifNotExists;
        this.ifExists = ifExists;
        this.schemaContract = schemaContract;
        this.finalPreflight = finalPreflight;
    }

    /** The catalog owning the dataset; also the busy-guard namespace. */
    public LanceExternalCatalog getCatalog() {
        return catalog;
    }

    public TLanceIndexMutationType getMutationType() {
        return mutationType;
    }

    /** The dataset locator of the pinned admission snapshot (never echoed into user text). */
    public String getDatasetUri() {
        return datasetUri;
    }

    /**
     * The dataset version of the pinned admission snapshot: the version the whole statement
     * validated against and the version the dispatch admits.
     */
    public long getAdmittedDatasetVersion() {
        return admittedDatasetVersion;
    }

    /** The normalized (case-folded) index name — the durable logical identity. */
    public String getNormalizedIndexName() {
        return normalizedIndexName;
    }

    /** The stored column name for CREATE/REPLACE (never the raw user spelling); empty for DROP. */
    public String getColumnName() {
        return columnName;
    }

    /** The requested algorithm identity (the ANN index_type, or the BTREE/BITMAP literal). */
    public String getIndexType() {
        return indexType;
    }

    /** The validated request properties for CREATE/REPLACE; null for DROP. */
    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    /** The schema contract for CREATE/REPLACE; null for DROP (which carries none). */
    public LanceIndexSchemaContract getSchemaContract() {
        return schemaContract;
    }

    /**
     * The entry-internal final preflight, run once inside the held active-name entry before
     * any budget is spent: it re-decides the authoritative no-op/existence verdict against
     * current metadata with the admission semantics.
     */
    public LanceIndexMutationExecutor.FinalPreflight getFinalPreflight() {
        return finalPreflight;
    }

    /**
     * Fills the admission-pinned inputs into the executor request builder. The statement-owned
     * inputs — budget deadline, cancel signal, and the mutated table (from which the refresh
     * obligation derives) — stay with the caller; everything else the request carries comes
     * from here.
     */
    public LanceIndexMutationExecutor.MutationRequest.Builder populate(
            LanceIndexMutationExecutor.MutationRequest.Builder builder) {
        return builder.setColumnName(columnName)
                .setIndexType(indexType)
                .setProperties(properties)
                .setIfNotExists(ifNotExists)
                .setIfExists(ifExists)
                .setSchemaContract(schemaContract)
                .setFinalPreflight(finalPreflight);
    }
}
