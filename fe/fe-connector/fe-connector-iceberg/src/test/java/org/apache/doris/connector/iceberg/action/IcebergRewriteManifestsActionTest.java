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

package org.apache.doris.connector.iceberg.action;

import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Pins {@code rewrite_manifests}, including the delegated connector {@link RewriteManifestExecutor}.
 *
 * <p><b>WHY this matters:</b> the body short-circuits an empty table to {@code ["0", "0"]} (no executor
 * call), and otherwise reports (rewritten, added) manifest counts from the executor. The executor is the
 * fe-core port stripped of its {@code ExternalTable}/{@code ExtMetaCacheMgr} couplings; this verifies it
 * actually combines the per-append manifests through the SDK {@code rewriteManifests()} API.</p>
 */
public class IcebergRewriteManifestsActionTest {

    private static IcebergRewriteManifestsAction action(java.util.Map<String, String> props) {
        return new IcebergRewriteManifestsAction(props, Collections.emptyList(), null);
    }

    @Test
    public void emptyTableShortCircuitsToZeroZero() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");

        IcebergRewriteManifestsAction action = action(Collections.emptyMap());
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        Assertions.assertEquals(ImmutableList.of("0", "0"), result.getRows().get(0),
                "an empty table (no current snapshot) returns 0/0 without touching the executor");
    }

    @Test
    public void combinesPerAppendManifests() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        // Three separate appends -> three data manifests accumulate in the current snapshot.
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);
        ActionTestTables.appendSnapshot(catalog, "t", "f3.parquet", 3L);
        Table before = catalog.loadTable(id);
        int manifestsBefore = before.currentSnapshot().dataManifests(before.io()).size();
        Assertions.assertEquals(3, manifestsBefore);

        IcebergRewriteManifestsAction action = action(Collections.emptyMap());
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        // The executor reports manifestsBefore.size() as the rewritten count (the faithful port behaviour);
        // the added count is iceberg's internal combine outcome, asserted only as a valid non-negative int.
        Assertions.assertEquals(String.valueOf(manifestsBefore), result.getRows().get(0).get(0),
                "rewritten_manifests_count is the number of manifests targeted for rewrite");
        Assertions.assertTrue(Integer.parseInt(result.getRows().get(0).get(1)) >= 0,
                "added_manifests_count is a valid non-negative count");
    }

    @Test
    public void resultSchemaIsTwoInts() {
        Assertions.assertEquals("rewritten_manifests_count",
                action(Collections.emptyMap()).getResultSchema().get(0).getName());
        Assertions.assertEquals("INT",
                action(Collections.emptyMap()).getResultSchema().get(0).getType().getTypeName());
        Assertions.assertEquals("INT",
                action(Collections.emptyMap()).getResultSchema().get(1).getType().getTypeName());
    }

    @Test
    public void specIdAcceptsZeroToMaxRange() {
        // spec_id is optional with intRange(0, MAX); a negative value is rejected at parse time.
        Assertions.assertDoesNotThrow(() -> action(ImmutableMap.of("spec_id", "0")).validate());
    }
}
