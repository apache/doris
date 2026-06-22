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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@link ConnectorScanPlanProvider} for Iceberg tables, mirroring the paimon connector's
 * {@code PaimonScanPlanProvider}. The generic, engine-neutral {@code PluginDrivenScanNode} drives split
 * generation through this provider once iceberg is in {@code SPI_READY_TYPES} (P6.6 cutover).
 *
 * <p>P6.2-T01 (this task) is the skeleton: it wires the collaborators ({@code properties} /
 * {@link IcebergCatalogOps} seam / {@link ConnectorContext}) and pins the predicate-driven semantics
 * ({@link #ignorePartitionPruneShortCircuit()} = {@code true}). The real split planning — self-contained
 * predicate pushdown, {@code FileScanTask} enumeration, native-vs-JNI classification, merge-on-read
 * delete files, the field-id history-schema dictionary, COUNT/batch, and vended credentials — lands in
 * P6.2-T02..T09. Iceberg is NOT yet in {@code SPI_READY_TYPES}, so {@link #planScan} is not exercised at
 * runtime this phase (iceberg queries still route to the legacy {@code IcebergScanNode}); the parity is
 * verified by offline unit tests until the P6.6 cutover.</p>
 */
public class IcebergScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(IcebergScanPlanProvider.class);

    private final Map<String, String> properties;
    private final IcebergCatalogOps catalogOps;
    // Engine seam: executeAuthenticated (Kerberos UGI), storage properties, vended credentials. Nullable —
    // null in offline unit tests via the 2-arg ctor, in which case resolveTable resolves directly.
    private final ConnectorContext context;

    public IcebergScanPlanProvider(Map<String, String> properties, IcebergCatalogOps catalogOps) {
        this(properties, catalogOps, null);
    }

    public IcebergScanPlanProvider(Map<String, String> properties, IcebergCatalogOps catalogOps,
            ConnectorContext context) {
        this.properties = properties;
        this.catalogOps = catalogOps;
        this.context = context;
    }

    /**
     * Iceberg is predicate-driven: it re-plans through its own SDK from the pushed predicate and never
     * consults {@code requiredPartitions} (mirrors the legacy {@code IcebergScanNode} and paimon). The engine
     * must therefore map a genuine FE prune-to-zero to scan-all instead of short-circuiting to zero rows —
     * otherwise {@code WHERE col IS NULL} on a genuine-null partition rendered as a non-null sentinel would
     * drop rows once T02 wires the predicate path.
     */
    @Override
    public boolean ignorePartitionPruneShortCircuit() {
        return true;
    }

    @Override
    public List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        Table table = resolveTable(iceHandle);
        // SKELETON (P6.2-T01): resolving the table establishes the seam (catalogOps + auth context) that the
        // real split planning builds on. Predicate pushdown -> FileScanTask enumeration -> IcebergScanRange,
        // delete files, the field-id history dict, COUNT/batch, and vended credentials land in P6.2-T02..T09
        // (mirroring PaimonScanPlanProvider). No ranges are produced yet.
        LOG.debug("Iceberg planScan skeleton resolved table {} (no splits yet)", table.name());
        return Collections.emptyList();
    }

    /**
     * Loads the live Iceberg {@link Table} through the {@link IcebergCatalogOps} seam, wrapped in the
     * FE-injected authentication context when present — so the Kerberos UGI applies (mirrors
     * {@code IcebergConnectorMetadata} and paimon's {@code PaimonScanPlanProvider.resolveTable}). A
     * {@code null} context (offline unit tests / simple-auth) resolves directly.
     */
    private Table resolveTable(IcebergTableHandle handle) {
        if (context == null) {
            return catalogOps.loadTable(handle.getDbName(), handle.getTableName());
        }
        try {
            return context.executeAuthenticated(
                    () -> catalogOps.loadTable(handle.getDbName(), handle.getTableName()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load table for scan, error message is:" + e.getMessage(), e);
        }
    }
}
