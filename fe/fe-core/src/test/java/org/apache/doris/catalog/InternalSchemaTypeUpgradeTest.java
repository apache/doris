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

package org.apache.doris.catalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The FRACTIONAL-SECOND PRECISION of the SPM internal-table timestamp columns is part of the
 * column type.
 *
 * <p>Round-79 #10: the columns were created at second precision, so two status flips inside one
 * second stored EQUAL instants - the durable pick then resolved them by its DISABLED-first
 * tie-break and the flip bump had to advance whole SECONDS, pushing stored instants far ahead of
 * wall time (past every wall-clock-bounded publication fence). The columns are now
 * TIMESTAMPTZ({@link InternalSchema#SPM_TIMESTAMP_SCALE}), and a pre-existing column of another
 * precision must NOT count as the published schema - a scale-blind comparison accepted the
 * second-precision column and the module kept comparing truncated instants forever. (A
 * differing table is REBUILT, see InternalSchemaInitializer#rebuildSpmTable.)
 */
public class InternalSchemaTypeUpgradeTest {

    /** The create-time type of every SPM timestamp column: TIMESTAMPTZ with microsecond scale. */
    @Test
    public void testTimestampColumnsAreCreatedAtMicrosecondPrecision() {
        Assertions.assertEquals(6, InternalSchema.SPM_TIMESTAMP_SCALE,
                "the internal tables keep MICROSECOND precision");
        Assertions.assertEquals(InternalSchema.SPM_TIMESTAMP_SCALE,
                ScalarType.createTimeStampTzType(InternalSchema.SPM_TIMESTAMP_SCALE)
                        .getScalarScale(),
                "the scale reaches the column type");
    }

    /** A column of ANOTHER scale must not match the declaration. */
    @Test
    public void testAnotherTimestampScaleIsNotTheDeclaredType() {
        ScalarType target = ScalarType.createTimeStampTzType(InternalSchema.SPM_TIMESTAMP_SCALE);
        Assertions.assertFalse(InternalSchemaInitializer.spmColumnTypeMatches(
                ScalarType.createTimeStampTzType(0), target),
                "a second-precision TIMESTAMPTZ column is NOT the create-time type");
        Assertions.assertFalse(InternalSchemaInitializer.spmColumnTypeMatches(
                ScalarType.createTimeStampTzType(3), target),
                "millisecond precision is NOT the create-time type");
        Assertions.assertTrue(InternalSchemaInitializer.spmColumnTypeMatches(
                ScalarType.createTimeStampTzType(InternalSchema.SPM_TIMESTAMP_SCALE), target),
                "the create-time type matches itself");
    }

    /** The legacy DATETIME spelling differs too. */
    @Test
    public void testLegacyDatetimeIsNotTheDeclaredType() {
        Assertions.assertFalse(InternalSchemaInitializer.spmColumnTypeMatches(
                ScalarType.createDatetimeType(),
                ScalarType.createTimeStampTzType(InternalSchema.SPM_TIMESTAMP_SCALE)),
                "a DATETIME column is not the declared TIMESTAMPTZ");
    }

    /** Types without a fractional second compare their primitive and width. */
    @Test
    public void testNonTimestampTypesKeepTheNameOnlyComparison() {
        Assertions.assertTrue(InternalSchemaInitializer.spmColumnTypeMatches(
                ScalarType.createVarcharType(10), ScalarType.createVarcharType(10)),
                "a plain type matches on its name");
        Assertions.assertFalse(InternalSchemaInitializer.spmColumnTypeMatches(
                ScalarType.createVarcharType(10), ScalarType.createVarcharType(20)),
                "different string lengths differ");
        Assertions.assertFalse(InternalSchemaInitializer.spmColumnTypeMatches(
                ScalarType.createVarcharType(10), ScalarType.createType(PrimitiveType.BIGINT)),
                "different primitives differ");
    }
}
