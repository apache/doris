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

package org.apache.doris.datasource.lance;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.WarmupSelectCommand;
import org.apache.doris.qe.ConnectContext;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

class LanceIndexPrewarmResolutionTest {
    private ConnectContext context;
    private ConnectContext previous;
    private List<String> resolved;

    private static class Resolved extends RuntimeException {
    }

    @BeforeEach
    void setUp() {
        previous = ConnectContext.get();
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getName()).thenReturn("lake");
        context = new ConnectContext() {
            @Override
            public CatalogIf getCurrentCatalog() {
                return catalog;
            }

            @Override
            public String getDefaultCatalog() {
                return catalog.getName();
            }
        };
        context.setDatabase("A");
        context.setThreadLocalInfo();
        // Stop after real name resolution, before any catalog or remote metadata IO.
        new MockUp<LanceIndexPrewarm>() {
            @Mock
            void checkPrivileges(ConnectContext ctx, List<String> table) {
                resolved = table;
                throw new Resolved();
            }
        };
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
        if (previous != null) {
            previous.setThreadLocalInfo();
        }
    }

    private WarmupSelectCommand command(String projection, String table) {
        return (WarmupSelectCommand) new NereidsParser().parseSingle(
                "WARM UP SELECT " + projection + " FROM " + table + " PROPERTIES (read_index_only=true)");
    }

    private List<String> resolve(WarmupSelectCommand command) {
        Assertions.assertThrows(Resolved.class, () -> command.run(context, null));
        return resolved;
    }

    @Test
    void rejectsQualifierFromTheDatabaseUsedOnlyDuringParsing() {
        WarmupSelectCommand command = (WarmupSelectCommand) ((LogicalPlanAdapter) new NereidsParser().parseSQL(
                "USE B; WARM UP SELECT A.items.embedding FROM items PROPERTIES (read_index_only=true)")
                .get(1)).getLogicalPlan();
        context.setDatabase("B");
        Exception error = Assertions.assertThrows(Exception.class, () -> command.run(context, null));
        Assertions.assertTrue(error.getMessage() != null && error.getMessage().contains("qualifier"));
    }

    @Test
    void acceptsQualifierForTheExecutionTimeDatabase() {
        WarmupSelectCommand command = command("B.items.embedding", "items");
        context.setDatabase("B");
        Assertions.assertEquals(Arrays.asList("lake", "B", "items"), resolve(command));
    }

    @Test
    void preparedExecutionsResolveDefaultsIndependently() {
        WarmupSelectCommand command = command("*", "items");
        Assertions.assertEquals(Arrays.asList("lake", "A", "items"), resolve(command));
        context.setDatabase("B");
        Assertions.assertEquals(Arrays.asList("lake", "B", "items"), resolve(command));
    }

    @Test
    void preparedExecutionsResolveCatalogDefaultsIndependently() {
        WarmupSelectCommand command = command("*", "items");
        Assertions.assertEquals(Arrays.asList("lake", "A", "items"), resolve(command));
        Mockito.when(context.getCurrentCatalog().getName()).thenReturn("other_lake");
        Assertions.assertEquals(Arrays.asList("other_lake", "A", "items"), resolve(command));
    }

    @Test
    void validatesStarsAndAliasesAtExecution() {
        WarmupSelectCommand qualifiedStar = command("B.items.*", "items");
        WarmupSelectCommand alias = command("i.embedding", "items i");
        WarmupSelectCommand hiddenTable = command("items.embedding", "items i");
        context.setDatabase("B");
        Assertions.assertEquals(Arrays.asList("lake", "B", "items"), resolve(qualifiedStar));
        Assertions.assertEquals(Arrays.asList("lake", "B", "items"), resolve(alias));
        Exception failure = Assertions.assertThrows(Exception.class, () -> hiddenTable.run(context, null));
        Assertions.assertTrue(failure.getMessage().contains("qualifier"));
    }

    @Test
    void rejectsOverqualifiedTablesWithoutDroppingLeadingParts() {
        WarmupSelectCommand command = command("*", "bogus.lake.db.items");
        IllegalStateException error = Assertions.assertThrows(IllegalStateException.class,
                () -> command.run(context, null));
        Assertions.assertTrue(error.getMessage().contains("bogus.lake.db.items"));
        Assertions.assertNull(resolved, "An invalid name must fail before accessing any table");
    }

    @Test
    void preservesTableSpellingForCatalogCasePolicy() {
        new MockUp<Env>() {
            @Mock
            public boolean isStoredTableNamesLowerCase() {
                return true;
            }
        };
        Assertions.assertEquals(Arrays.asList("lake", "db", "Items"),
                resolve(command("*", "lake.db.Items")));
    }
}
