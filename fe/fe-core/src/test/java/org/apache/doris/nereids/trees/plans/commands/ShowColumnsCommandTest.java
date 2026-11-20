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

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ShowColumnsCommandTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("show_columns_db");
        createTable("create table show_columns_db.t1 (k int, v int) distributed by hash(k) buckets 1"
                + " properties('replication_num' = '1');");
    }

    private ShowResultSet run(String sql) throws Exception {
        return ((ShowColumnsCommand) new NereidsParser().parseSingle(sql)).doRun(connectContext, null);
    }

    @Test
    public void testCatalogQualifiedDatabase() throws Exception {
        List<List<String>> rows = run("SHOW COLUMNS FROM t1 FROM internal.show_columns_db").getResultRows();
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals("k", rows.get(0).get(0));
        Assertions.assertEquals("v", rows.get(1).get(0));
    }

    @Test
    public void testCatalogPartIsHonored() {
        // If the catalog part were dropped, this would read internal.show_columns_db.t1 and succeed.
        Exception e = Assertions.assertThrows(Exception.class,
                () -> run("SHOW COLUMNS FROM t1 FROM nonexist_ctl.show_columns_db"));
        Assertions.assertTrue(e.getMessage().contains("Unknown catalog 'nonexist_ctl'"), e.getMessage());
    }
}
