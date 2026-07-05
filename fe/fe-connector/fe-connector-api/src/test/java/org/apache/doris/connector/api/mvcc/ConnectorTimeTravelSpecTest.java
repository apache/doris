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

import java.util.HashMap;
import java.util.Map;

/**
 * Contracts for {@link ConnectorTimeTravelSpec}, the source-agnostic carrier fe-core uses
 * to hand an explicit time-travel request to a connector for resolution.
 *
 * <p><b>WHY this matters:</b> each factory must stamp exactly one {@link
 * ConnectorTimeTravelSpec.Kind} and leave the irrelevant fields null/empty &mdash; the
 * connector dispatches on {@code kind} and reads only the field that kind owns, so a wrong
 * kind or a leaked field silently routes a query to the wrong time-travel branch. The map
 * must be defensively copied and unmodifiable so a later mutation of the caller's map cannot
 * change an already-resolved spec, and equals/hashCode must include every field so a spec
 * cannot be confused with a same-named spec of a different kind/flag.</p>
 */
public class ConnectorTimeTravelSpecTest {

    @Test
    public void snapshotIdFactorySetsOnlySnapshotKind() {
        ConnectorTimeTravelSpec spec = ConnectorTimeTravelSpec.snapshotId("42");
        // MUTATION: a factory that stamped TIMESTAMP/TAG here would route the digits down the
        // wrong connector branch.
        Assertions.assertEquals(ConnectorTimeTravelSpec.Kind.SNAPSHOT_ID, spec.getKind());
        Assertions.assertEquals("42", spec.getStringValue());
        Assertions.assertFalse(spec.isDigital(), "digital is only meaningful for TIMESTAMP");
        Assertions.assertTrue(spec.getIncrementalParams().isEmpty(),
                "non-incremental specs carry no incremental params");
    }

    @Test
    public void timestampFactoryCarriesDigitalFlagBothWays() {
        // WHY: digital decides whether the connector treats the value as epoch-millis or as a
        // datetime string to parse; flipping it changes the resolved instant. Lock both states.
        ConnectorTimeTravelSpec epoch = ConnectorTimeTravelSpec.timestamp("1700000000000", true);
        ConnectorTimeTravelSpec text = ConnectorTimeTravelSpec.timestamp("2024-01-01 00:00:00", false);

        Assertions.assertEquals(ConnectorTimeTravelSpec.Kind.TIMESTAMP, epoch.getKind());
        Assertions.assertTrue(epoch.isDigital(), "epoch-millis literal must be digital=true");
        Assertions.assertEquals(ConnectorTimeTravelSpec.Kind.TIMESTAMP, text.getKind());
        Assertions.assertFalse(text.isDigital(), "datetime string must be digital=false");
    }

    @Test
    public void tagAndBranchFactoriesAreDistinctKinds() {
        // WHY: tag and branch carry the same shape (a name in stringValue) but resolve via
        // different SDK paths; if the factory collapsed them to one kind the connector would
        // pick the wrong resolution path.
        ConnectorTimeTravelSpec tag = ConnectorTimeTravelSpec.tag("v1");
        ConnectorTimeTravelSpec branch = ConnectorTimeTravelSpec.branch("v1");

        Assertions.assertEquals(ConnectorTimeTravelSpec.Kind.TAG, tag.getKind());
        Assertions.assertEquals(ConnectorTimeTravelSpec.Kind.BRANCH, branch.getKind());
        Assertions.assertEquals("v1", tag.getStringValue());
        Assertions.assertEquals("v1", branch.getStringValue());
        // Same name, different kind => must not be equal (else a tag query reuses a branch result).
        Assertions.assertNotEquals(tag, branch);
    }

    @Test
    public void versionRefFactoryIsDistinctFromTag() {
        // WHY: a non-numeric FOR VERSION AS OF '<name>' is VERSION_REF (the connector resolves it as a
        // branch OR a tag), NOT the explicit @tag (TAG, tag-only). Same name shape, different kind: if the
        // factory collapsed VERSION_REF into TAG, iceberg would reject a branch ref (regression H-7).
        ConnectorTimeTravelSpec versionRef = ConnectorTimeTravelSpec.versionRef("v1");
        ConnectorTimeTravelSpec tag = ConnectorTimeTravelSpec.tag("v1");

        Assertions.assertEquals(ConnectorTimeTravelSpec.Kind.VERSION_REF, versionRef.getKind());
        Assertions.assertEquals("v1", versionRef.getStringValue());
        Assertions.assertFalse(versionRef.isDigital(), "digital is only meaningful for TIMESTAMP");
        Assertions.assertTrue(versionRef.getIncrementalParams().isEmpty());
        // Same name, different kind => must not be equal (else a @tag query reuses a VERSION_REF result).
        Assertions.assertNotEquals(versionRef, tag);
    }

    @Test
    public void incrementalFactoryHasNullStringValueAndParams() {
        Map<String, String> raw = new HashMap<>();
        raw.put("startSnapshotId", "1");
        raw.put("endSnapshotId", "5");
        ConnectorTimeTravelSpec spec = ConnectorTimeTravelSpec.incremental(raw);

        Assertions.assertEquals(ConnectorTimeTravelSpec.Kind.INCREMENTAL, spec.getKind());
        // MUTATION: stuffing a stringValue for INCREMENTAL would mislead a connector that keys
        // off stringValue presence.
        Assertions.assertNull(spec.getStringValue(),
                "INCREMENTAL carries its args in the params map, not stringValue");
        Assertions.assertEquals(raw, spec.getIncrementalParams());
    }

    @Test
    public void incrementalParamsAreDefensivelyCopiedAndUnmodifiable() {
        Map<String, String> raw = new HashMap<>();
        raw.put("startSnapshotId", "1");
        ConnectorTimeTravelSpec spec = ConnectorTimeTravelSpec.incremental(raw);

        // WHY (Rule 9): a spec is a resolved request; mutating the caller's source map afterwards
        // must NOT retroactively change the spec the engine already dispatched on.
        // MUTATION: storing the map by reference (no copy) makes this assertion red.
        raw.put("endSnapshotId", "5");
        Assertions.assertFalse(spec.getIncrementalParams().containsKey("endSnapshotId"),
                "spec must snapshot the params at construction, not alias the caller's map");

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> spec.getIncrementalParams().put("x", "y"),
                "exposed params map must be unmodifiable");
    }

    @Test
    public void equalsAndHashCodeIncludeAllFields() {
        // WHY: two specs that differ in digital alone (or kind alone) are genuinely different
        // time-travel targets; equals/hashCode must separate them or a cache could serve the wrong
        // pinned snapshot.
        ConnectorTimeTravelSpec a = ConnectorTimeTravelSpec.timestamp("100", true);
        ConnectorTimeTravelSpec b = ConnectorTimeTravelSpec.timestamp("100", true);
        ConnectorTimeTravelSpec digitalFlipped = ConnectorTimeTravelSpec.timestamp("100", false);

        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        // MUTATION: dropping `digital ==` from equals makes this red.
        Assertions.assertNotEquals(a, digitalFlipped,
                "specs differing only by the digital flag must not be equal");
    }

    @Test
    public void factoriesRejectNullMeaningfulArgs() {
        // WHY: a null where a snapshot id / name / params map is required is a programming error in
        // the fe-core extractor; fail loud at construction rather than NPE deep in the connector.
        Assertions.assertThrows(NullPointerException.class,
                () -> ConnectorTimeTravelSpec.snapshotId(null));
        Assertions.assertThrows(NullPointerException.class,
                () -> ConnectorTimeTravelSpec.timestamp(null, true));
        Assertions.assertThrows(NullPointerException.class,
                () -> ConnectorTimeTravelSpec.versionRef(null));
        Assertions.assertThrows(NullPointerException.class,
                () -> ConnectorTimeTravelSpec.tag(null));
        Assertions.assertThrows(NullPointerException.class,
                () -> ConnectorTimeTravelSpec.branch(null));
        Assertions.assertThrows(NullPointerException.class,
                () -> ConnectorTimeTravelSpec.incremental(null));
    }
}
