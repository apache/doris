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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Pins {@code fast_forward}: advancing one branch to the tip of another.
 *
 * <p><b>WHY this matters:</b> the result row is (branch name, previous ref id, updated ref id) and the
 * schema mixes types — {@code branch_updated} is STRING, {@code previous_ref} is the one NULLABLE column
 * (legacy passed {@code isAllowNull = true}), {@code updated_ref} is NOT NULL BIGINT. Bug-for-bug: the
 * branch name is trimmed only in the output and the post-commit snapshot read is not null-guarded.</p>
 */
public class IcebergFastForwardActionTest {

    private static IcebergFastForwardAction action(String branch, String to) {
        return new IcebergFastForwardAction(
                ImmutableMap.of("branch", branch, "to", to), Collections.emptyList(), null);
    }

    @Test
    public void fastForwardsBranchToTargetTip() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        long snap1 = ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        // Create branch b1 at snap1, then advance main to snap2 (snap1 is an ancestor of snap2).
        catalog.loadTable(id).manageSnapshots().createBranch("b1", snap1).commit();
        long snap2 = ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);

        IcebergFastForwardAction action = action("b1", "main");
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        Assertions.assertEquals(
                java.util.Arrays.asList("b1", String.valueOf(snap1), String.valueOf(snap2)),
                result.getRows().get(0),
                "b1 fast-forwards from snap1 to main's tip snap2");
        Assertions.assertEquals(snap2, catalog.loadTable(id).snapshot(
                catalog.loadTable(id).refs().get("b1").snapshotId()).snapshotId());
    }

    @Test
    public void unknownTargetIsWrappedUnderFailedToFastForward() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);

        // fast-forward main to a non-existent source ref 'ghost' -> the SDK rejects the unknown ref, and the
        // body wraps it under "Failed to fast-forward branch <branch> to snapshot <to>: ...".
        IcebergFastForwardAction action = action("main", "ghost");
        action.validate();
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action.execute(catalog.loadTable(id), ActionTestTables.session("UTC")));
        Assertions.assertTrue(e.getMessage().startsWith("Failed to fast-forward branch main to snapshot ghost: "),
                e.getMessage());
    }

    @Test
    public void resultSchemaMixesTypesWithNullablePreviousRef() {
        List<ConnectorColumn> schema = action("a", "b").getResultSchema();
        Assertions.assertEquals("branch_updated", schema.get(0).getName());
        Assertions.assertEquals("STRING", schema.get(0).getType().getTypeName());
        Assertions.assertFalse(schema.get(0).isNullable());
        Assertions.assertEquals("previous_ref", schema.get(1).getName());
        Assertions.assertEquals("BIGINT", schema.get(1).getType().getTypeName());
        Assertions.assertTrue(schema.get(1).isNullable(), "previous_ref is the one nullable result column");
        Assertions.assertEquals("updated_ref", schema.get(2).getName());
        Assertions.assertEquals("BIGINT", schema.get(2).getType().getTypeName());
        Assertions.assertFalse(schema.get(2).isNullable());
    }

    @Test
    public void requiresBothBranchAndToArguments() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> new IcebergFastForwardAction(ImmutableMap.of("branch", "b1"),
                        Collections.emptyList(), null).validate());
        Assertions.assertEquals("Missing required argument: to", e.getMessage());
    }
}
