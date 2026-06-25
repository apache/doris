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

import org.apache.doris.connector.api.scan.ConnectorColumnCategory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Guards {@link IcebergScanPlanProvider#classifyColumn(String)}, the iceberg half of P6.6-C2
 * (WS-SYNTH-READ): the generic {@code PluginDrivenScanNode} delegates connector-owned special columns here so
 * no iceberg knowledge leaks into fe-core.
 *
 * <p>WHY this matters: the hidden row-id column must be SYNTHESIZED (never read from the data file — the
 * IcebergParquet/OrcReader materializes it) and the v3 row-lineage columns must be GENERATED (read from the
 * file when present, otherwise backfilled). Misreporting either as DEFAULT demotes it to a REGULAR file slot,
 * so the BE reads a column the file does not contain / loses the row-lineage backfill. The engine-wide
 * {@code __DORIS_GLOBAL_ROWID_COL__} is intentionally NOT claimed here — it is a generic Doris mechanism the
 * generic node owns — so this asserts the connector returns DEFAULT for it.</p>
 *
 * <p>{@code classifyColumn} is pure (no instance state), so the provider is built with an empty config and a
 * null catalog seam.</p>
 */
public class IcebergScanPlanProviderClassifyColumnTest {

    private static final IcebergScanPlanProvider PROVIDER =
            new IcebergScanPlanProvider(Collections.emptyMap(), null);

    @Test
    public void hiddenRowIdIsSynthesized() {
        // MUTATION: removing the __DORIS_ICEBERG_ROWID_COL__ branch -> DEFAULT -> the generic node tags it
        // REGULAR (a file slot) -> the BE reads a non-existent file column -> red.
        Assertions.assertEquals(ConnectorColumnCategory.SYNTHESIZED,
                PROVIDER.classifyColumn("__DORIS_ICEBERG_ROWID_COL__"));
        // Case-insensitive, mirroring legacy IcebergScanNode (Column.ICEBERG_ROWID_COL.equalsIgnoreCase).
        Assertions.assertEquals(ConnectorColumnCategory.SYNTHESIZED,
                PROVIDER.classifyColumn("__doris_iceberg_rowid_col__"));
    }

    @Test
    public void rowLineageColumnsAreGenerated() {
        // MUTATION: removing the row-lineage branch -> DEFAULT -> REGULAR -> loses GENERATED backfill -> red.
        Assertions.assertEquals(ConnectorColumnCategory.GENERATED, PROVIDER.classifyColumn("_row_id"));
        Assertions.assertEquals(ConnectorColumnCategory.GENERATED,
                PROVIDER.classifyColumn("_last_updated_sequence_number"));
    }

    @Test
    public void globalRowIdIsNotClaimedByConnector() {
        // WHY: GLOBAL_ROWID is a generic Doris lazy-mat mechanism owned by the generic node, NOT iceberg.
        // The connector must return DEFAULT so the split (fe-core handles GLOBAL_ROWID) holds. MUTATION:
        // adding a GLOBAL_ROWID branch here would double-own it -> this assertion -> red.
        Assertions.assertEquals(ConnectorColumnCategory.DEFAULT,
                PROVIDER.classifyColumn("__DORIS_GLOBAL_ROWID_COL__my_tbl"));
    }

    @Test
    public void regularColumnIsDefault() {
        // WHY: ordinary data columns must fall through to the generic node's partition-key/regular logic.
        Assertions.assertEquals(ConnectorColumnCategory.DEFAULT, PROVIDER.classifyColumn("id"));
        Assertions.assertEquals(ConnectorColumnCategory.DEFAULT, PROVIDER.classifyColumn("name"));
    }
}
