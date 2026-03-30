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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.ShowCreateMTMVInfo;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public class ShowCreateMTMVCommandTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_show_create_mtmv_db");
        useDatabase("test_show_create_mtmv_db");
        createTables(
                "CREATE TABLE IF NOT EXISTS t1 (\n"
                        + "    id INT,\n"
                        + "    name VARCHAR(50)\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
        createMvByNereids("CREATE MATERIALIZED VIEW mv1 BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT * FROM t1;");
    }

    @Test
    public void testShowCreateMTMVWithSessionVariables() throws Exception {
        // Verify show create materialized view works normally
        ShowCreateMTMVInfo info = new ShowCreateMTMVInfo(
                new TableNameInfo("test_show_create_mtmv_db", "mv1"));
        info.analyze(connectContext);
        ShowResultSet resultSet = info.getShowResultSet();

        List<List<String>> rows = resultSet.getResultRows();
        Assertions.assertEquals(1, rows.size());
        List<String> row = rows.get(0);
        // 3 columns: Materialized View, Create Materialized View, Session Variables
        Assertions.assertEquals(3, row.size());
        Assertions.assertEquals("mv1", row.get(0));
        // Session variables should not be null or empty
        Assertions.assertNotNull(row.get(2));
        Assertions.assertFalse(row.get(2).isEmpty());
    }

    @Test
    public void testShowCreateMTMVWithNullSessionVariables() throws Exception {
        // Simulate upgrade scenario: MTMVs created before sessionVariables field was introduced
        // have null sessionVariables after deserialization
        Database db = Env.getCurrentEnv().getInternalCatalog()
                .getDbOrAnalysisException("test_show_create_mtmv_db");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv1");

        // Use reflection to set sessionVariables to null, simulating pre-upgrade MTMV
        Field sessionVariablesField = MTMV.class.getDeclaredField("sessionVariables");
        sessionVariablesField.setAccessible(true);
        Map<String, String> originalSessionVars = (Map<String, String>) sessionVariablesField.get(mtmv);
        sessionVariablesField.set(mtmv, null);

        try {
            // This should NOT throw NPE after the fix
            ShowCreateMTMVInfo info = new ShowCreateMTMVInfo(
                    new TableNameInfo("test_show_create_mtmv_db", "mv1"));
            info.analyze(connectContext);
            ShowResultSet resultSet = info.getShowResultSet();

            List<List<String>> rows = resultSet.getResultRows();
            Assertions.assertEquals(1, rows.size());
            List<String> row = rows.get(0);
            Assertions.assertEquals(3, row.size());
            Assertions.assertEquals("mv1", row.get(0));
            // When sessionVariables is null, should display empty map representation
            Assertions.assertEquals("{}", row.get(2));
        } finally {
            // Restore original sessionVariables to avoid affecting other tests
            sessionVariablesField.set(mtmv, originalSessionVars);
        }
    }
}
