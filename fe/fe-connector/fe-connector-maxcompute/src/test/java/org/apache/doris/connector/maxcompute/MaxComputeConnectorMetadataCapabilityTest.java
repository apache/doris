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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * P2-6 FIX-CREATE-DB-PRECHECK (clean-room re-review DG-4 / F26, F23) — pins the
 * MaxCompute schema-op capability declaration the FE CREATE DATABASE precheck depends on.
 *
 * <p><b>WHY this matters:</b> the fix for DG-4 gates the FE
 * {@code CREATE DATABASE IF NOT EXISTS} remote-existence precheck on
 * {@code ConnectorSchemaOps.supportsCreateDatabase()} (default false) so that jdbc/es/trino —
 * which cannot create databases — keep their existing "not supported" behavior. MaxCompute CAN
 * create databases and MUST declare {@code true}, otherwise the precheck is skipped for it and
 * the very regression DG-4 describes (CREATE DATABASE IF NOT EXISTS on a remotely-existing db
 * surfacing ODPS "already exists") returns. The fe-core routing tests use a mocked connector, so
 * this is the only test that pins the real MaxCompute override. MUTATION: flipping the override
 * to {@code return false} makes this red. The capability getter touches no instance field, so a
 * {@code null} odps/helper keeps the test offline (same pattern as
 * {@link MaxComputeBuildTableDescriptorTest}).</p>
 */
public class MaxComputeConnectorMetadataCapabilityTest {

    @Test
    public void maxComputeDeclaresSupportsCreateDatabase() {
        MaxComputeConnectorMetadata metadata = new MaxComputeConnectorMetadata(
                null, null, "proj", "ep", "quota", Collections.emptyMap(),
                null); // null: partition cache unused by this test

        Assertions.assertTrue(metadata.supportsCreateDatabase(),
                "MaxCompute must declare supportsCreateDatabase()=true so the FE "
                        + "CREATE DATABASE IF NOT EXISTS remote precheck applies to it (DG-4)");
    }

    /**
     * F9 FIX-CAST-PUSHDOWN — pins that MaxCompute disables CAST-predicate pushdown.
     *
     * <p><b>WHY this matters:</b> the shared converter unwraps CAST shells, so if this returned
     * {@code true} (the SPI default), a predicate like {@code CAST(str_col AS INT)=5} would be pushed
     * to ODPS as {@code str_col="5"} and silently drop rows like {@code "05"}/{@code " 5"} at the
     * source (BE re-eval cannot recover source-dropped rows). Returning {@code false} makes
     * {@code PluginDrivenScanNode.buildRemainingFilter} keep CAST conjuncts BE-only, mirroring legacy
     * (which never pushed CAST predicates). MUTATION: flipping the override to {@code true} (or
     * removing it, reverting to the default {@code true}) makes this red. Offline: the getter touches
     * no instance field, so null odps/helper/session is fine.</p>
     */
    @Test
    public void maxComputeDisablesCastPredicatePushdown() {
        MaxComputeConnectorMetadata metadata = new MaxComputeConnectorMetadata(
                null, null, "proj", "ep", "quota", Collections.emptyMap(),
                null); // null: partition cache unused by this test

        Assertions.assertFalse(metadata.supportsCastPredicatePushdown(null),
                "MaxCompute must disable CAST-predicate pushdown (F9): the converter unwraps CAST "
                        + "shells, and pushing the stripped predicate to ODPS under-matches at the "
                        + "source and silently drops rows BE re-eval cannot recover");
    }
}
