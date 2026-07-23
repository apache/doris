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

package org.apache.doris.connector.api.mvcc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Contracts for the additive {@code schemaId} field on {@link ConnectorMvccSnapshot}
 * (B5b schema-at-pinned-snapshot support).
 *
 * <p><b>WHY this matters:</b> {@code schemaId} carries the resolved schema version of a
 * pinned snapshot so a time-travel read under schema evolution can fetch the schema AS OF
 * that snapshot. The unset default MUST be {@code -1} (= unknown) so every pre-existing
 * builder caller keeps reading the latest schema with zero behavior change; the existing
 * fields must round-trip unchanged so adding the field did not perturb them.</p>
 */
public class ConnectorMvccSnapshotTest {

    @Test
    public void schemaIdDefaultsToMinusOneWhenUnset() {
        // WHY: -1 is the "unknown => fall back to latest schema" sentinel. Every existing builder
        // caller (which never calls schemaId(..)) must observe -1, i.e. zero behavior change.
        // MUTATION: defaulting the builder field to 0 makes this red and would wrongly pin schema 0.
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder()
                .snapshotId(7L)
                .build();
        Assertions.assertEquals(-1L, snapshot.getSchemaId(),
                "unset schemaId must default to -1 (unknown => latest schema)");
    }

    @Test
    public void builderSetsSchemaId() {
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder()
                .schemaId(3L)
                .build();
        // MUTATION: a builder that ignored schemaId (returned -1) makes this red.
        Assertions.assertEquals(3L, snapshot.getSchemaId());
    }

    @Test
    public void existingFieldsRoundTripUnaffectedBySchemaId() {
        // WHY: the schemaId addition is purely additive; the other fields must carry through
        // exactly as before so no existing consumer regresses.
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder()
                .snapshotId(11L)
                .timestampMillis(1700000000000L)
                .description("d")
                .property("scan.snapshot-id", "11")
                .schemaId(2L)
                .build();

        Assertions.assertEquals(11L, snapshot.getSnapshotId());
        Assertions.assertEquals(1700000000000L, snapshot.getTimestampMillis());
        Assertions.assertEquals("d", snapshot.getDescription());
        Assertions.assertEquals("11", snapshot.getProperties().get("scan.snapshot-id"));
        Assertions.assertEquals(2L, snapshot.getSchemaId());
    }

    @Test
    public void equalsAndHashCodeCoverAllSixFields() {
        // WHY: ConnectorMvccSnapshot joins its value-object family (ConnectorMvccPartition,
        // ConnectorTimeTravelSpec, ConnectorTableFreshness, ...) which all define value equality.
        // Every one of the 6 fields must participate, so two snapshots differing in ANY single
        // field compare unequal. MUTATION: dropping a field from equals()/hashCode() makes the
        // matching assertNotEquals below fail (the differing pair would wrongly compare equal).
        ConnectorMvccSnapshot base = fullSnapshot();
        ConnectorMvccSnapshot same = fullSnapshot();
        Assertions.assertEquals(base, same);
        Assertions.assertEquals(base.hashCode(), same.hashCode());

        Assertions.assertNotEquals(base, fullSnapshotBuilder().snapshotId(999L).build());
        Assertions.assertNotEquals(base, fullSnapshotBuilder().timestampMillis(999L).build());
        Assertions.assertNotEquals(base, fullSnapshotBuilder().schemaId(999L).build());
        Assertions.assertNotEquals(base, fullSnapshotBuilder().lastModifiedFreshness(true).build());
        Assertions.assertNotEquals(base, fullSnapshotBuilder().description("other").build());
        Assertions.assertNotEquals(base, fullSnapshotBuilder().property("k2", "v2").build());
    }

    @Test
    public void toStringExposesEveryField() {
        // WHY: toString feeds EXPLAIN/log diagnostics; a field silently omitted hides drift.
        String s = fullSnapshot().toString();
        Assertions.assertTrue(s.contains("snapshotId=11"), s);
        Assertions.assertTrue(s.contains("timestampMillis=1700000000000"), s);
        Assertions.assertTrue(s.contains("schemaId=2"), s);
        Assertions.assertTrue(s.contains("lastModifiedFreshness=false"), s);
        Assertions.assertTrue(s.contains("description='d'"), s);
        Assertions.assertTrue(s.contains("k=v"), s);
    }

    private static ConnectorMvccSnapshot.Builder fullSnapshotBuilder() {
        // lastModifiedFreshness defaults false; each call returns a fresh, fully-populated builder.
        return ConnectorMvccSnapshot.builder()
                .snapshotId(11L)
                .timestampMillis(1700000000000L)
                .description("d")
                .schemaId(2L)
                .property("k", "v");
    }

    private static ConnectorMvccSnapshot fullSnapshot() {
        return fullSnapshotBuilder().build();
    }
}
