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

package org.apache.doris.datasource;

import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.datasource.systable.SysTable;

import java.util.Map;
import java.util.Optional;

/**
 * Generic {@link PluginDrivenExternalTable} for a connector system table (e.g. {@code tbl$snapshots}).
 *
 * <p>Created transiently by {@link org.apache.doris.datasource.systable.PluginDrivenSysTable} during
 * planning/describe (via {@code createSysExternalTable}); it is NEVER added to a persisted table map
 * and is NOT GSON-registered, mirroring legacy sys ExternalTables (e.g.
 * {@link org.apache.doris.datasource.paimon.PaimonSysExternalTable}).</p>
 *
 * <p>It reports {@link org.apache.doris.catalog.TableIf.TableType#PLUGIN_EXTERNAL_TABLE} (inherited);
 * no connector-specific table type is introduced. The whole schema/partition/row-count path is reused
 * from the base class; the only behavioral change is {@link #resolveConnectorTableHandle}, which threads
 * the connector's system-table handle (not the base handle) through every base-class site.</p>
 */
public class PluginDrivenSysExternalTable extends PluginDrivenExternalTable {

    private final PluginDrivenExternalTable sourceTable;
    private final String sysTableName;

    /**
     * @param source the underlying base table being wrapped
     * @param sysName the bare system-table name (e.g. "snapshots"), no "$" prefix
     */
    public PluginDrivenSysExternalTable(PluginDrivenExternalTable source, String sysName) {
        super(generateSysTableId(source.getId(), sysName),
                source.getName() + "$" + sysName,
                source.getRemoteName() + "$" + sysName,
                source.getCatalog(),
                source.getDb());
        this.sourceTable = source;
        this.sysTableName = sysName;
    }

    /**
     * Generate a unique ID from the source table ID and system table name (legacy parity with
     * {@code PaimonSysExternalTable.generateSysTableId}).
     */
    private static long generateSysTableId(long sourceTableId, String sysName) {
        return sourceTableId ^ (sysName.hashCode() * 31L);
    }

    /**
     * Resolve the connector handle for THIS system table: first acquire the BASE table handle using the
     * source's remote name (NOT this sys table's "$"-suffixed remote name), then ask the connector for
     * the system-table handle. Returning the sys handle here threads it through
     * {@code initSchema}/{@code getNameToPartitionItems}/{@code fetchRowCount} automatically, so a sys
     * query reads the sys table rather than the base.
     */
    @Override
    protected Optional<ConnectorTableHandle> resolveConnectorTableHandle(
            ConnectorSession session, ConnectorMetadata metadata) {
        String dbName = db != null ? db.getRemoteName() : "";
        Optional<ConnectorTableHandle> baseHandle =
                metadata.getTableHandle(session, dbName, sourceTable.getRemoteName());
        if (!baseHandle.isPresent()) {
            return Optional.empty();
        }
        return metadata.getSysTableHandle(session, baseHandle.get(), sysTableName);
    }

    /**
     * Delegate to the source table so DESCRIBE/SHOW on a system table still lists its sibling system
     * tables (legacy parity with {@code PaimonSysExternalTable.getSupportedSysTables}).
     */
    @Override
    public Map<String, SysTable> getSupportedSysTables() {
        return sourceTable.getSupportedSysTables();
    }

    @Override
    public String getComment() {
        return "Plugin system table: " + sysTableName + " for " + sourceTable.getName();
    }

    public PluginDrivenExternalTable getSourceTable() {
        return sourceTable;
    }

    public String getSysTableName() {
        return sysTableName;
    }
}
