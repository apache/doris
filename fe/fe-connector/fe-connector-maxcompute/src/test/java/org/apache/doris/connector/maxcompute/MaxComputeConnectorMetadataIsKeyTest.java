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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * FIX-ISKEY-METADATA (P4-T06e / NG-6 / F3 / F10) — guards
 * {@link MaxComputeConnectorMetadata#buildColumn}, the single seam through which both
 * {@code getTableSchema} column loops construct their {@link ConnectorColumn}s.
 *
 * <p><b>Why this matters:</b> legacy {@code MaxComputeExternalTable.initSchema} marked every column
 * {@code isKey=true}; the cutover's 5-arg ctor defaulted it to {@code false}, regressing
 * {@code DESCRIBE <mc_table>} to {@code Key=NO} (and silently changing the non-OLAP-guarded planning
 * /BE paths that read {@code Column.isKey()}). This pins the {@code isKey=true} invariant in the
 * MaxCompute module.</p>
 *
 * <p><b>Coverage scope:</b> this pins the {@code buildColumn} helper invariant only. The
 * {@code getTableSchema → buildColumn} wiring is NOT unit-tested here because {@code getTableSchema}
 * dereferences a live {@code com.aliyun.odps.Table}, whose only constructor is package-private and
 * this connector module has no Mockito (driving it offline would require a {@code com.aliyun.odps}
 * -package fixture subclass overriding {@code getSchema()} — no precedent in this repo). A future
 * call site that bypasses {@code buildColumn} (reverting to the 5-arg ctor) would not be caught
 * here — the e2e {@code DESCRIBE} assertion is the load-bearing regression gate for the wiring
 * (recorded as a deviation).</p>
 */
public class MaxComputeConnectorMetadataIsKeyTest {

    @Test
    public void testBuildColumnMarksKeyTrue() {
        // The core regression guard: every MaxCompute column must be isKey=true (legacy parity).
        ConnectorColumn col = MaxComputeConnectorMetadata.buildColumn(
                "c1", ConnectorType.of("INT"), "a comment", true);
        Assertions.assertTrue(col.isKey());
    }

    @Test
    public void testBuildColumnPreservesOtherFields() {
        // Non-vacuous: the helper must build a correct column, not just flip the key flag.
        ConnectorColumn col = MaxComputeConnectorMetadata.buildColumn(
                "c1", ConnectorType.of("INT"), "a comment", true);
        Assertions.assertEquals("c1", col.getName());
        Assertions.assertEquals(ConnectorType.of("INT"), col.getType());
        Assertions.assertEquals("a comment", col.getComment());
        Assertions.assertTrue(col.isNullable());
        Assertions.assertNull(col.getDefaultValue());
        // External tables never carry auto-increment columns; mirrors legacy.
        Assertions.assertFalse(col.isAutoInc());
    }

    @Test
    public void testBuildColumnKeyIndependentOfNullable() {
        // Guards against accidentally wiring isKey to the nullable arg: a non-nullable
        // (e.g. partition-style) column is still a key column.
        ConnectorColumn col = MaxComputeConnectorMetadata.buildColumn(
                "pt", ConnectorType.of("STRING"), null, false);
        Assertions.assertTrue(col.isKey());
        Assertions.assertFalse(col.isNullable());
    }
}
