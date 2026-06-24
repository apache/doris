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
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.procedure.ConnectorProcedureOps;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.connector.spi.ConnectorContext;

import java.util.List;
import java.util.Map;

/**
 * Executes iceberg's {@code ALTER TABLE EXECUTE} procedures (the 9 legacy
 * {@code datasource/iceberg/action/*} actions) behind the {@link ConnectorProcedureOps} SPI.
 *
 * <p>Mirrors {@link IcebergWritePlanProvider}: a fresh instance per call over the lazily-built live
 * catalog, threading the same {@code properties} / {@link IcebergCatalogOps} / {@link ConnectorContext}
 * seams. The SDK table is loaded (inside {@code context.executeAuthenticated}) and the procedure body
 * runs in the connector; argument validation is connector-local (the engine cannot reach
 * {@code org.apache.doris.common.NamedArguments} across the import gate).</p>
 *
 * <p><b>Dormant (P6.4-T02).</b> This skeleton wires the SPI seam; the per-procedure dispatch and bodies
 * are ported in P6.4-T03 (base + factory) / T04 (the 8 pure-SDK procedures) / T05–T06
 * ({@code rewrite_data_files}). Inert pre-cutover regardless: iceberg tables are not
 * {@code PluginDrivenExternalTable} until P6.6, so {@code ExecuteActionCommand} still routes them to the
 * legacy fe-core actions and never reaches this class.</p>
 */
public class IcebergProcedureOps implements ConnectorProcedureOps {

    private final Map<String, String> properties;
    private final IcebergCatalogOps catalogOps;
    private final ConnectorContext context;

    public IcebergProcedureOps(Map<String, String> properties, IcebergCatalogOps catalogOps,
            ConnectorContext context) {
        this.properties = properties;
        this.catalogOps = catalogOps;
        this.context = context;
    }

    @Override
    public List<String> getSupportedProcedures() {
        throw new UnsupportedOperationException(
                "iceberg procedures are not yet wired (P6.4-T03 ports the action factory)");
    }

    @Override
    public ConnectorProcedureResult execute(ConnectorSession session, ConnectorTableHandle table,
            String procedureName, Map<String, String> properties,
            ConnectorPredicate whereCondition, List<String> partitionNames) {
        throw new UnsupportedOperationException(
                "iceberg procedure '" + procedureName + "' is not yet wired (P6.4-T04 ports the bodies)");
    }
}
