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

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;

import com.aliyun.odps.type.TypeInfoFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * G7 FIX-VOID-TYPE-MAPPING — pins the ODPS {@code TypeInfo} -> {@link ConnectorType} mapping for
 * the two cases that diverged from legacy {@code MaxComputeExternalTable.mcTypeToDorisType}.
 *
 * <p><b>WHY this matters:</b> VOID must emit the {@code "NULL_TYPE"} token, which
 * {@code ScalarType.createType} turns into {@code Type.NULL} (legacy parity). The prior bug emitted
 * {@code "NULL"}, which {@code ScalarType.createType} does NOT recognize -> it throws ->
 * {@code ConnectorColumnConverter} swallowed it to {@code Type.UNSUPPORTED}, so a VOID column
 * silently became unusable. Separately, a genuinely unknown OdpsType ({@code OdpsType.UNKNOWN} or a
 * future type) must fail-fast (legacy threw "Cannot transform unknown type"), not silently degrade
 * to UNSUPPORTED — while the known-unsupported types (BINARY/INTERVAL) keep their explicit
 * UNSUPPORTED mapping.</p>
 */
public class MCTypeMappingTest {

    @Test
    public void voidMapsToNullTypeToken() {
        // WHY (Rule 9): VOID must emit the token that yields Type.NULL downstream. MUTATION:
        // reverting to of("NULL") makes this red ("NULL" is rejected by ScalarType.createType).
        ConnectorType t = MCTypeMapping.toConnectorType(TypeInfoFactory.VOID);
        Assertions.assertEquals("NULL_TYPE", t.getTypeName(),
                "ODPS VOID must map to the NULL_TYPE token (-> Type.NULL), not NULL");
    }

    @Test
    public void arrayOfVoidMapsElementToNullType() {
        // The VOID branch is shared by nested element mapping; ARRAY<VOID> must carry NULL_TYPE.
        ConnectorType arr = MCTypeMapping.toConnectorType(
                TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.VOID));
        Assertions.assertEquals("NULL_TYPE", arr.getChildren().get(0).getTypeName(),
                "ARRAY<VOID> element must map to NULL_TYPE");
    }

    @Test
    public void binaryStaysUnsupportedNotThrown() {
        // WHY: known-unsupported types have explicit UNSUPPORTED cases; the fail-fast default
        // (for unknown future types) must NOT swallow them. If BINARY fell through to the default
        // it would throw instead of returning UNSUPPORTED.
        ConnectorType t = MCTypeMapping.toConnectorType(TypeInfoFactory.BINARY);
        Assertions.assertEquals("UNSUPPORTED", t.getTypeName(),
                "BINARY is a known-unsupported type: explicit UNSUPPORTED, not a fail-fast throw");
    }

    @Test
    public void unknownTypeFailsFast() {
        // WHY (Rule 9): a genuinely unknown OdpsType must fail-fast, mirroring legacy
        // MaxComputeExternalTable.mcTypeToDorisType:294, instead of silently becoming UNSUPPORTED
        // (which masks the problem). MUTATION: reverting the default to of("UNSUPPORTED") makes
        // this red (no exception). OdpsType.UNKNOWN reaches the switch default (no explicit case).
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> MCTypeMapping.toConnectorType(TypeInfoFactory.UNKNOWN));
        Assertions.assertTrue(ex.getMessage().toLowerCase().contains("unknown"),
                "unknown-type rejection message should mention 'unknown'");
    }

    @Test
    public void knownScalarTokensAreStable() {
        // Guards against token drift for the common scalars.
        Assertions.assertEquals("INT",
                MCTypeMapping.toConnectorType(TypeInfoFactory.INT).getTypeName());
        Assertions.assertEquals("STRING",
                MCTypeMapping.toConnectorType(TypeInfoFactory.STRING).getTypeName());
        Assertions.assertEquals("BOOLEAN",
                MCTypeMapping.toConnectorType(TypeInfoFactory.BOOLEAN).getTypeName());
    }
}
