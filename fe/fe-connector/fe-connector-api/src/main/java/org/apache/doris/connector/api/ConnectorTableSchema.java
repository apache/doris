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

package org.apache.doris.connector.api;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Describes the schema of a connector table, including its columns
 * and table-level properties.
 */
public final class ConnectorTableSchema {

    /**
     * Reserved property key carrying the table location string for SHOW CREATE TABLE rendering.
     * Connectors emit it here (rather than as a user-facing property) so the FE renders it as the
     * {@code LOCATION '...'} clause and strips it from the PROPERTIES(...) block. Distinct from a
     * connector's own user-facing location property (e.g. paimon's SDK {@code path} option, which
     * legitimately stays in PROPERTIES).
     */
    public static final String SHOW_LOCATION_KEY = "show.location";

    /**
     * Reserved property key carrying the fully-rendered {@code PARTITION BY ...} clause (Doris SQL,
     * including transform terms like {@code BUCKET(8, `c`)} / {@code DAY(`c`)}) for SHOW CREATE TABLE.
     * The connector pre-renders it (the transform-aware logic is connector-specific); the FE appends
     * it verbatim and strips it from PROPERTIES.
     */
    public static final String SHOW_PARTITION_CLAUSE_KEY = "show.partition-clause";

    /**
     * Reserved property key carrying the fully-rendered {@code ORDER BY (...)} clause for SHOW CREATE
     * TABLE. The connector pre-renders it; the FE appends it verbatim and strips it from PROPERTIES.
     */
    public static final String SHOW_SORT_CLAUSE_KEY = "show.sort-clause";

    /**
     * Reserved property key carrying a CSV of {@link ConnectorCapability#name()} values that THIS specific
     * table supports, refining the connector-wide {@link Connector#getCapabilities()} set per-table.
     *
     * <p>A uniform-format connector (e.g. iceberg — every table orc/parquet) declares a scan capability for all
     * its tables connector-wide. A heterogeneous connector (e.g. hive — orc/parquet/text/json/csv/view/hudi in
     * one catalog) whose eligibility is per-table file-format gated cannot: Top-N lazy materialization and
     * nested-column pruning are orc/parquet-only, and blanket-declaring them connector-wide would over-admit a
     * text/json table (a correctness bug for nested-column pruning, which reads NULL leaves without field ids).
     * Such a connector instead emits the capability name here, per-table, computed from that table's format.</p>
     *
     * <p>fe-core reads it ADDITIVELY (a capability counts as supported if it is in the connector-wide set OR in
     * this per-table list) from the already-cached schema — no remote round-trip and no file-format inspection
     * in fe-core. Single-format connectors never emit it and are unaffected. Stripped from the user-facing
     * SHOW CREATE TABLE PROPERTIES(...) block.</p>
     */
    public static final String PER_TABLE_CAPABILITIES_KEY = "connector.per-table-capabilities";

    /**
     * Reserved property key carrying a CSV of the table's distribution (bucketing) column names, already
     * lowercased. A heterogeneous connector (hive) whose bucketing varies per table cannot express it as a
     * connector-wide trait, so it emits it here per-table.
     *
     * <p>fe-core's {@code PluginDrivenExternalTable.getDistributionColumnNames()} reads it from the cached schema
     * (no remote round-trip) so a flipped bucketed hive table picks the same NDV estimator as legacy
     * {@code HMSExternalTable.getDistributionColumnNames} (a single bucket column selects the linear estimator in
     * sampled analyze). A non-bucketed table omits it and connectors with no bucketing concept never emit it.
     * Stripped from the user-facing SHOW CREATE TABLE PROPERTIES(...) block.</p>
     */
    public static final String DISTRIBUTION_COLUMNS_KEY = "connector.distribution-columns";

    private final String tableName;
    private final List<ConnectorColumn> columns;
    private final String tableFormatType;
    private final Map<String, String> properties;

    public ConnectorTableSchema(String tableName,
            List<ConnectorColumn> columns,
            String tableFormatType,
            Map<String, String> properties) {
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.columns = columns == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(columns);
        this.tableFormatType = tableFormatType;
        this.properties = properties == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(properties);
    }

    public String getTableName() {
        return tableName;
    }

    public List<ConnectorColumn> getColumns() {
        return columns;
    }

    public String getTableFormatType() {
        return tableFormatType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorTableSchema)) {
            return false;
        }
        ConnectorTableSchema that = (ConnectorTableSchema) o;
        return tableName.equals(that.tableName)
                && columns.equals(that.columns)
                && Objects.equals(tableFormatType, that.tableFormatType)
                && properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, columns, tableFormatType, properties);
    }

    @Override
    public String toString() {
        return "ConnectorTableSchema{tableName='" + tableName
                + "', columns=" + columns.size()
                + ", format=" + tableFormatType + "}";
    }
}
