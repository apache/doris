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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.ConnectorCapability;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

/**
 * Pins the exact connector-wide capability set the hive connector declares (HMS cutover §4.2, dormant).
 *
 * <p>Each capability is either a faithful port of a legacy HMSExternalTable/HMS admission (declared) or a
 * deliberate deferral (withheld). The set is inert until hms enters SPI_READY_TYPES, so these assertions are
 * a Rule-9 guard: flipping any capability without the supporting machinery (or dropping a legacy-parity one)
 * would silently change post-flip behavior, and this test fails loud instead.</p>
 */
public class HiveConnectorCapabilitiesTest {

    private Set<ConnectorCapability> capabilities() {
        return new HiveConnector(Collections.emptyMap(), new FakeConnectorContext()).getCapabilities();
    }

    @Test
    public void declaresLegacyParityCapabilities() {
        Set<ConnectorCapability> caps = capabilities();
        // Legacy resolved isView() from the remote view text; the plugin view path is gated on this.
        Assertions.assertTrue(caps.contains(ConnectorCapability.SUPPORTS_VIEW),
                "SUPPORTS_VIEW: legacy hive views are queryable/droppable/listed");
        // Legacy HMSExternalTable.supportsExternalMetadataPreload() returned true.
        Assertions.assertTrue(caps.contains(ConnectorCapability.SUPPORTS_METADATA_PRELOAD),
                "SUPPORTS_METADATA_PRELOAD: legacy HMS tables were preload-eligible");
        // The mixed hms catalog needs MVCC (iceberg/hudi-on-HMS are MvccTable; GSON maps HMSExternalTable ->
        // PluginDrivenMvccExternalTable, and buildTableInternal selects the Mvcc subclass from this
        // catalog-level flag). Declared TOGETHER with its MTMV freshness machinery
        // (HiveConnectorMetadata.getTableFreshness / getPartitionFreshnessMillis surface hive's last-modified
        // freshness), so a plain-hive base table's MV detects change instead of pinning a constant.
        Assertions.assertTrue(caps.contains(ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT),
                "SUPPORTS_MVCC_SNAPSHOT: the mixed hms catalog needs it; freshness is served last-modified");
    }

    @Test
    public void withholdsCapabilitiesWithoutSupportingSpi() {
        Set<ConnectorCapability> caps = capabilities();
        // Needs the connector to emit the table location (show.location) + a rendering-parity decision first.
        Assertions.assertFalse(caps.contains(ConnectorCapability.SUPPORTS_SHOW_CREATE_DDL),
                "SUPPORTS_SHOW_CREATE_DDL needs location emission + a SHOW CREATE rendering decision");
        // Hive exposes no query() TVF (no getColumnsFromQuery).
        Assertions.assertFalse(caps.contains(ConnectorCapability.SUPPORTS_PASSTHROUGH_QUERY),
                "hive has no passthrough query()");
        // Legacy SHOW PARTITIONS lists names only; listPartitions emits UNKNOWN stats.
        Assertions.assertFalse(caps.contains(ConnectorCapability.SUPPORTS_PARTITION_STATS),
                "hive SHOW PARTITIONS is names-only");
    }

    @Test
    public void perTableScanCapabilitiesAreNotConnectorWide() {
        Set<ConnectorCapability> caps = capabilities();
        // Both are per-table markers emitted in getTableSchema (orc/parquet only), never connector-wide flags,
        // otherwise a text/json/csv/view/hudi table would be wrongly eligible.
        Assertions.assertFalse(caps.contains(ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE),
                "Top-N lazy is a per-table marker, not connector-wide");
        Assertions.assertFalse(caps.contains(ConnectorCapability.SUPPORTS_NESTED_COLUMN_PRUNE),
                "nested-column-prune is a per-table marker, not connector-wide");
        // SUPPORTS_COLUMN_AUTO_ANALYZE is likewise per-table (getTableSchema emits it for every plain-hive table).
        // A connector-wide flag would over-admit hudi-on-HMS, which legacy StatisticsUtil.supportAutoAnalyze
        // excluded (it admitted only dlaType HIVE || ICEBERG). MUTATION: re-declaring it connector-wide silently
        // re-admits hudi-on-HMS -> red here.
        Assertions.assertFalse(caps.contains(ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE),
                "auto-analyze is a per-table marker (excludes hudi-on-HMS), not connector-wide");
        // SUPPORTS_SAMPLE_ANALYZE is likewise per-table (getTableSchema emits it for plain-hive tables only,
        // any format). A connector-wide flag would over-admit iceberg/hudi-on-HMS to sampled ANALYZE, which
        // legacy gated on dlaType==HIVE. MUTATION: declaring it connector-wide -> red here.
        Assertions.assertFalse(caps.contains(ConnectorCapability.SUPPORTS_SAMPLE_ANALYZE),
                "sample-analyze is a per-table marker (plain-hive only), not connector-wide");
    }
}
