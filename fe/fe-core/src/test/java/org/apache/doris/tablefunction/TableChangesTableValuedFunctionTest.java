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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

class TableChangesTableValuedFunctionTest {
    @Test
    void resolvesNamedAndQuotedTargetsAndKeepsIntervalsDistinct() {
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.doReturn(catalog).when(ctx).getCurrentCatalog();
        Mockito.when(catalog.getName()).thenReturn("cat");
        Mockito.when(ctx.getDatabase()).thenReturn("db");
        for (String table : List.of("events", "db.events", "cat.db.events")) {
            var changes = new TableChangesTableValuedFunction(Map.of("TABLE", table, "START_VERSION", "1"), ctx);
            Assertions.assertEquals(List.of("cat", "db", "events$changes"), changes.changesName());
            Assertions.assertEquals(Map.of("start_version", "1"), changes.scanParams().getMapParams());
        }
        var quoted = new TableChangesTableValuedFunction(Map.of("table", "`c.a`.`d`.`t``b`",
                "start_version", "2", "end_timestamp", "2000-01-01"), ctx);
        Assertions.assertEquals(List.of("c.a", "d", "t`b"), quoted.baseName());
        var scoped = new TableChangesTableValuedFunction(Map.of("catalog", "c", "db", "d", "table", "t",
                "start_timestamp", "2000-01-01"), ctx);
        Assertions.assertEquals(List.of("c", "d", "t"), scoped.baseName());
    }

    @Test
    void rejectsConflictsAndChecksSourceSelectBeforeDescribingMetadata() {
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        for (Map<String, String> args : List.of(Map.of("table", "c.d.t", "start_version", "1", "catalog", "c"),
                Map.of("table", "d.t", "start_version", "1", "database", "d"),
                Map.of("table", "t", "start_version", "1", "database", "d", "db", "d"),
                Map.of("table", "c.d.t", "start_version", "1", "START_VERSION", "2"),
                Map.of("table", "c.d.t", "start_timestamp", "x", "start_version", "1"),
                Map.of("table", "c.d.t", "start_version", "1", "end_timestamp", "x", "end_version", "2"),
                Map.of("table", "c.d.t", "end_version", "2"), Map.of("table", "c.d.t", "bogus", "x"),
                Map.of("table", "x..t", "start_version", "1"))) {
            Assertions.assertThrows(AnalysisException.class, () -> new TableChangesTableValuedFunction(args, ctx));
        }
        Env env = Mockito.mock(Env.class);
        Mockito.when(ctx.getEnv()).thenReturn(env);
        Mockito.when(env.getAccessManager()).thenReturn(Mockito.mock(AccessControllerManager.class));
        var changes = new TableChangesTableValuedFunction(Map.of("table", "cat.db.t", "start_version", "1"), ctx);
        try (var current = Mockito.mockStatic(ConnectContext.class)) {
            current.when(ConnectContext::get).thenReturn(ctx);
            Assertions.assertTrue(Assertions.assertThrows(AnalysisException.class, changes::getTableColumns)
                    .getMessage().contains("SELECT command denied on cat.db.t"));
        }
    }
}
