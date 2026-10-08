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

import org.apache.doris.catalog.SchemaTable;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ShowProcessListCommandTest {

    @Test
    public void testMetaDataIsTheProcesslistSchemaTable() {
        List<String> names = Lists.newArrayList();
        new ShowProcessListCommand(false).getMetaData().getColumns().forEach(c -> names.add(c.getName()));
        List<String> expected = Lists.newArrayList();
        SchemaTable.TABLE_MAP.get("processlist").getBaseSchema().forEach(c -> expected.add(c.getName()));
        Assertions.assertEquals(expected, names);
        Assertions.assertEquals("Protocol", names.get(names.size() - 1));
    }

    // SHOW PROCESSLIST merges the rows the other frontends answer with; while the cluster runs two
    // versions those rows have another number of columns, and the result set must stay rectangular.
    @Test
    public void testRowOfAnotherFrontendIsFittedToTheLocalColumns() {
        List<String> row = Lists.newArrayList("a", "b", "c");

        Assertions.assertSame(row, ShowProcessListCommand.fitToColumns(row, 3));
        // An older frontend's row ends before the columns added since: padded with empty strings.
        Assertions.assertEquals(Lists.newArrayList("a", "b", "c", "", ""),
                ShowProcessListCommand.fitToColumns(row, 5));
        // A newer frontend's row has columns this one does not know: cut.
        Assertions.assertEquals(Lists.newArrayList("a", "b"), ShowProcessListCommand.fitToColumns(row, 2));
        // The caller's row is left as it was.
        Assertions.assertEquals(Lists.newArrayList("a", "b", "c"), row);
    }
}
