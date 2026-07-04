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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.StructType;
import org.apache.doris.datasource.iceberg.IcebergRowId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for IcebergDeleteCommand.
 *
 * Tests the query plan generation logic for Position Delete.
 */
public class IcebergDeleteCommandTest {

    @Test
    public void testPositionDeletePlanContainsRowId() {
        // This is a placeholder test
        // In real implementation, we would:
        // 1. Mock IcebergExternalTable
        // 2. Create a DELETE command with WHERE clause
        // 3. Generate query plan
        // 4. Verify that hidden row-id column is in the projection

        String rowIdColumnName = Column.ICEBERG_ROWID_COL;
        Assertions.assertEquals("__DORIS_ICEBERG_ROWID_COL__", rowIdColumnName);
        Assertions.assertFalse(rowIdColumnName.startsWith("$"));

        // TODO: Add full test when mock infrastructure is ready
        // Example:
        // IcebergDeleteCommand command = new IcebergDeleteCommand(
        //     nameParts, logicalQuery, table, partitions);
        // LogicalPlan plan = command.completeQueryPlan(ctx, logicalQuery, table);
        // Assertions.assertTrue(plan.getOutput().stream()
        //     .anyMatch(expr -> expr.getName().equals(Column.ICEBERG_ROWID_COL)));
    }

    @Test
    public void testRowIdStructFields() {
        // Verify that hidden row-id STRUCT has the correct 4 fields
        StructType structType = (StructType) IcebergRowId.getRowIdType();
        Assertions.assertEquals(4, structType.getFields().size());
    }
}
