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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.ConnectorType;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * P5-fix FIX-VARCHAR-BOUNDARY (review §5 N10.1) — pins the read-direction VARCHAR length boundary
 * in {@link PaimonTypeMapping#toConnectorType} to byte-parity with legacy
 * {@code PaimonUtil.paimonPrimitiveTypeToDorisType}.
 */
public class PaimonTypeMappingReadTest {

    private static ConnectorType mapVarchar(int len) {
        return PaimonTypeMapping.toConnectorType(new VarCharType(len), PaimonTypeMapping.Options.DEFAULT);
    }

    @Test
    public void varcharMaxLengthBoundaryMatchesLegacy() {
        // WHY: 65533 (== ScalarType.MAX_VARCHAR_LENGTH) is the legal exact-fit max VARCHAR, NOT the
        // STRING wildcard. Legacy uses `> 65533`, so 65533 must stay VARCHAR(65533); only a length
        // strictly greater overflows to STRING. The plugin path must agree byte-for-byte so that
        // DESCRIBE / SHOW CREATE TABLE report the same column type as legacy paimon.
        // MUTATION: reverting the boundary to `>= 65533` widens the 65533 case to STRING -> red.

        ConnectorType below = mapVarchar(65532);
        Assertions.assertEquals("VARCHAR", below.getTypeName());
        Assertions.assertEquals(65532, below.getPrecision());

        ConnectorType exact = mapVarchar(65533);
        Assertions.assertEquals("VARCHAR", exact.getTypeName(),
                "VARCHAR(65533) is the exact-fit max VARCHAR and must not widen to STRING");
        Assertions.assertEquals(65533, exact.getPrecision());

        ConnectorType above = mapVarchar(65534);
        Assertions.assertEquals("STRING", above.getTypeName(), "length > 65533 overflows to STRING");
    }

    @Test
    public void nestedStructFieldCommentAndNullabilityCarried() {
        // WHY: reading a paimon STRUCT must carry each nested field's COMMENT and NOT NULL constraint
        // into the ConnectorType (parity with the write path and with the iceberg read path) so that
        // DESCRIBE / SHOW CREATE TABLE report them rather than defaulting every nested field to
        // nullable / no comment.
        // MUTATION: reverting toStructType to the 2-arg ConnectorType.structOf(names, types) drops the
        // comment (getChildComment -> null) and the nullability (isChildNullable -> default true) -> red.
        RowType row = new RowType(Arrays.asList(
                new DataField(0, "x", new IntType(false), "note on x"),
                new DataField(1, "y", new VarCharType(true, 100), null)));
        ConnectorType struct = PaimonTypeMapping.toConnectorType(row, PaimonTypeMapping.Options.DEFAULT);

        Assertions.assertEquals("STRUCT", struct.getTypeName());
        Assertions.assertEquals("note on x", struct.getChildComment(0),
                "a nested struct field comment must be carried back on read");
        Assertions.assertFalse(struct.isChildNullable(0),
                "a NOT NULL nested struct field must map to a non-null ConnectorType child");
        Assertions.assertTrue(struct.isChildNullable(1),
                "a nullable nested struct field stays nullable");
    }
}
