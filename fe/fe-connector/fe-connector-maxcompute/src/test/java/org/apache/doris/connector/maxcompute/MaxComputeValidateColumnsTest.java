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
import org.apache.doris.connector.api.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * P2-8 FIX-AUTOINC-REJECT (clean-room re-review DG-5 / F24) — pins that MaxCompute
 * CREATE TABLE rejects AUTO_INCREMENT columns.
 *
 * <p><b>WHY this matters:</b> MaxCompute cannot store auto-increment columns. Legacy
 * {@code MaxComputeMetadataOps.validateColumns:422-425} threw a clear error; after the SPI
 * cutover the flag was dropped silently (the {@code ConnectorColumn} carrier had no
 * {@code isAutoInc} field), so {@code CREATE TABLE (id INT AUTO_INCREMENT)} silently created a
 * plain column — a data-model regression where the user's intent vanishes without warning. This
 * fix re-carries the flag and re-rejects it connector-side. These tests lock that in.</p>
 *
 * <p>{@code validateColumns} is package-private (reached only via {@code createTable} in
 * production, which needs a live ODPS handle); this connector test module has no Mockito, so the
 * test constructs the metadata offline with {@code null} odps/structureHelper and calls
 * {@code validateColumns} directly — it dereferences neither (only the static
 * {@code MCTypeMapping.toMcType}). Same offline idiom as {@link MaxComputeBuildTableDescriptorTest}.</p>
 */
public class MaxComputeValidateColumnsTest {

    private MaxComputeConnectorMetadata metadata() {
        return new MaxComputeConnectorMetadata(
                null, null, "proj", "ep", "quota", Collections.emptyMap());
    }

    @Test
    public void autoIncColumnIsRejected() {
        ConnectorColumn autoInc = new ConnectorColumn(
                "id", ConnectorType.of("INT"), "", false, null, false, true);

        // WHY (Rule 9): silent acceptance drops the user's AUTO_INCREMENT intent (MaxCompute can't
        // store it); legacy rejected it loudly. MUTATION: removing the `if (col.isAutoInc()) throw`
        // block makes this go red (no exception).
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().validateColumns(Collections.singletonList(autoInc)));
        Assertions.assertTrue(
                ex.getMessage().contains("Auto-increment columns are not supported for MaxCompute tables: id"),
                "rejection message must name the offending column, mirroring legacy validateColumns");
    }

    @Test
    public void nonAutoIncColumnPasses() {
        ConnectorColumn plain = new ConnectorColumn(
                "id", ConnectorType.of("INT"), "", false, null, false, false);

        // WHY: guards against over-rejection -- a normal column must still validate; the gate must
        // key on the auto-inc flag, not reject every column.
        Assertions.assertDoesNotThrow(
                () -> metadata().validateColumns(Collections.singletonList(plain)));
    }
}
