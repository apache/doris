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

package org.apache.doris.nereids.trees.plans.commands.execute;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.qe.ResultSet;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

class BaseExecuteActionTest {
    @Test
    void testEmptyResultRetainsSchema() throws Exception {
        ResultSet result = new TestAction(Collections.emptyList(), true).execute(null);
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.getResultRows().isEmpty());
        Assertions.assertEquals(1, result.getMetaData().getColumnCount());
        Column column = result.getMetaData().getColumn(0);
        Assertions.assertEquals("value", column.getName());
        Assertions.assertEquals(Type.STRING, column.getType());
        Assertions.assertTrue(column.isAllowNull());
    }

    @Test
    void testNoResultStillExecutesAction() throws Exception {
        TestAction nullRows = new TestAction(null, true);
        Assertions.assertNull(nullRows.execute(null));
        Assertions.assertTrue(nullRows.executed);

        TestAction noSchema = new TestAction(Collections.singletonList(Collections.singletonList("x")), false);
        Assertions.assertNull(noSchema.execute(null));
        Assertions.assertTrue(noSchema.executed);
    }

    @Test
    void testRowsKeepOrderAndNullCells() throws Exception {
        List<List<String>> rows = Arrays.asList(
                Collections.singletonList("first"), Collections.singletonList(null), Collections.singletonList("last"));
        ResultSet result = new TestAction(rows, true).execute(null);
        Assertions.assertEquals(rows, result.getResultRows());
        Assertions.assertEquals(Collections.singletonList(Collections.singletonList("only")),
                new TestAction(Collections.singletonList(Collections.singletonList("only")), true)
                        .execute(null).getResultRows());
    }

    @Test
    void testRejectsMalformedRows() {
        for (List<String> row : Arrays.<List<String>>asList(
                null, Collections.emptyList(), Arrays.asList("extra", "column"))) {
            TestAction action = new TestAction(Collections.singletonList(row), true);
            Assertions.assertThrows(IllegalStateException.class, () -> action.execute(null));
        }
    }

    private static class TestAction extends BaseExecuteAction {
        private final List<List<String>> rows;
        private boolean executed;

        TestAction(List<List<String>> rows, boolean schema) {
            super("test", Collections.singletonMap("schema", Boolean.toString(schema)),
                    Optional.empty(), Optional.empty());
            this.rows = rows;
        }

        @Override
        protected void registerArguments() {
        }

        @Override
        protected List<Column> getResultSchema() {
            return Boolean.parseBoolean(properties.get("schema"))
                    ? Collections.singletonList(new Column("value", Type.STRING, true))
                    : Collections.emptyList();
        }

        @Override
        protected List<List<String>> executeAction(TableIf table) {
            executed = true;
            return rows;
        }

        @Override
        public boolean isSupported(TableIf table) {
            return true;
        }

        @Override
        public String getDescription() {
            return "test";
        }
    }
}
