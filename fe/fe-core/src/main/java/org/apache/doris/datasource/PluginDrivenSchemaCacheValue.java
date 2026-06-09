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

import org.apache.doris.catalog.Column;

import java.util.List;

/**
 * {@link SchemaCacheValue} for plugin-driven external tables.
 *
 * <p>In addition to the full schema, it caches which columns are partition
 * columns so that {@link PluginDrivenExternalTable#getPartitionColumns()},
 * {@link PluginDrivenExternalTable#isPartitionedTable()} and partition pruning
 * can be served from the schema cache (mirroring {@code MaxComputeSchemaCacheValue}
 * / {@code HMSSchemaCacheValue}) instead of re-fetching the table schema from the
 * connector on every call.</p>
 *
 * <p>Two views of the partition columns are kept:
 * <ul>
 *   <li>{@code partitionColumns} — the Doris {@link Column}s (with the local,
 *       identifier-mapped names) used by {@code getPartitionColumns()} and to derive
 *       partition-column types.</li>
 *   <li>{@code partitionColumnRemoteNames} — the raw remote (e.g. ODPS) partition
 *       column names, aligned by index with {@code partitionColumns}, used to index
 *       the raw-keyed partition-value maps returned by the connector SPI
 *       ({@code ConnectorPartitionInfo.getPartitionValues()}).</li>
 * </ul>
 */
public class PluginDrivenSchemaCacheValue extends SchemaCacheValue {

    private final List<Column> partitionColumns;
    private final List<String> partitionColumnRemoteNames;

    public PluginDrivenSchemaCacheValue(List<Column> schema, List<Column> partitionColumns,
            List<String> partitionColumnRemoteNames) {
        super(schema);
        this.partitionColumns = partitionColumns;
        this.partitionColumnRemoteNames = partitionColumnRemoteNames;
    }

    public List<Column> getPartitionColumns() {
        return partitionColumns;
    }

    public List<String> getPartitionColumnRemoteNames() {
        return partitionColumnRemoteNames;
    }
}
