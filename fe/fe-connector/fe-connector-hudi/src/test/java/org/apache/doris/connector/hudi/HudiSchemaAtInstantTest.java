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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Tests the routing of the schema-at-instant surface (HD-C5a): the 3-arg
 * {@code getTableSchema(session, handle, snapshot)} override must resolve the schema AS OF the instant that
 * {@code applySnapshot} stamped onto the handle for a {@code FOR TIME AS OF} read, while a plain read (or a
 * non-{@code FOR TIME} pin) resolves the LATEST schema. Each assertion pins WHY the behavior matters:
 * <ul>
 *   <li>the 2-arg entry point always resolves LATEST (no time-travel pin exists), so a plain read stays
 *       byte-identical to before this step;</li>
 *   <li>a handle WITHOUT a {@code queryInstant} threads a {@code null} instant = LATEST — the shared build
 *       path means a non-{@code FOR TIME} 3-arg call cannot drift from the 2-arg latest path;</li>
 *   <li>a handle WITH a {@code queryInstant} threads exactly that instant into the metaClient read — proving
 *       the override keys off {@link HudiTableHandle#getQueryInstant()} (the {@code applySnapshot}-stamped
 *       pin) and NOT {@code snapshot.getSchemaId()} (which stays {@code -1} for hudi; the snapshot is
 *       deliberately passed as {@code null} here so a mutant that keyed off it would surface latest, not the
 *       instant).</li>
 * </ul>
 *
 * <p>The actual at-instant metaClient read is e2e (a live schema-evolved Hudi table); this same-loader unit
 * locks only the instant-threading routing by overriding the {@link HudiConnectorMetadata#getSchemaFromMetaClient}
 * seam, so no live metaClient / plugin auth is needed.</p>
 */
public class HudiSchemaAtInstantTest {

    /**
     * Recording fake: overrides the metaClient schema seam to capture the {@code queryInstant} the build path
     * threads down from the handle (no live metaClient). {@code null} = the LATEST path.
     */
    private static final class RecordingMetadata extends HudiConnectorMetadata {
        // Distinct from both null (= latest) and any real instant, so an un-invoked seam is detectable.
        String lastInstant = "SEAM-NOT-CALLED";
        int calls;

        RecordingMetadata() {
            super(null, Collections.emptyMap(), null);
        }

        @Override
        List<ConnectorColumn> getSchemaFromMetaClient(String basePath, String queryInstant) {
            this.lastInstant = queryInstant;
            this.calls++;
            return Collections.singletonList(
                    new ConnectorColumn("c", ConnectorType.of("INT"), "", true, null));
        }
    }

    private static HudiTableHandle handle(String queryInstant) {
        HudiTableHandle.Builder b = new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE");
        if (queryInstant != null) {
            b.queryInstant(queryInstant);
        }
        return b.build();
    }

    @Test
    void twoArgGetTableSchemaAlwaysResolvesLatest() {
        RecordingMetadata m = new RecordingMetadata();
        ConnectorTableSchema schema = m.getTableSchema(null, handle(null));
        Assertions.assertNull(m.lastInstant, "2-arg getTableSchema must resolve the LATEST schema (null instant)");
        Assertions.assertEquals(1, m.calls, "the metaClient seam must be reached exactly once");
        Assertions.assertEquals("t", schema.getTableName());
        Assertions.assertEquals("HUDI", schema.getTableFormatType());
    }

    @Test
    void threeArgWithoutPinResolvesLatest() {
        RecordingMetadata m = new RecordingMetadata();
        // No queryInstant on the handle (plain read / @incr pin): the shared build path threads a null instant.
        m.getTableSchema(null, handle(null), null);
        Assertions.assertNull(m.lastInstant,
                "a handle without a queryInstant pin must resolve the LATEST schema, not an at-instant one");
        Assertions.assertEquals(1, m.calls);
    }

    @Test
    void threeArgThreadsPinnedInstantFromHandle() {
        RecordingMetadata m = new RecordingMetadata();
        // FOR TIME AS OF: applySnapshot has stamped queryInstant onto the handle. getTableSchema must resolve AT
        // that instant. The snapshot arg is null on purpose: keying off it (schemaId) instead of the handle
        // would surface latest here, failing this assertion.
        m.getTableSchema(null, handle("20240101120000"), null);
        Assertions.assertEquals("20240101120000", m.lastInstant,
                "the handle's queryInstant must be threaded to the metaClient schema read");
        Assertions.assertEquals(1, m.calls);
    }
}
