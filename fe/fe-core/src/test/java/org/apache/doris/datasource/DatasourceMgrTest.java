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

package org.apache.doris.datasource;

import org.apache.doris.analysis.AlterCatalogNameStmt;
import org.apache.doris.analysis.AlterCatalogPropertyStmt;
import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.ShowCatalogStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DatasourceMgrTest extends TestWithFeService {
    private DataSourceMgr mgr;

    @Override
    protected void runBeforeAll() throws Exception {
        Config.enable_multi_catalog = true;
        FeConstants.runningUnitTest = true;
        mgr = Catalog.getCurrentCatalog().getDataSourceMgr();
    }

    @Test
    public void testNormalCase() throws Exception {
        String createCatalogSql = "CREATE CATALOG hms_catalog "
                + "properties( \"type\" = \"hms\", \"hive.metastore.uris\"=\"thrift://localhost:9083\" )";
        CreateCatalogStmt createStmt = (CreateCatalogStmt) parseAndAnalyzeStmt(createCatalogSql);
        mgr.createCatalog(createStmt);

        String showCatalogSql = "SHOW CATALOGS";
        ShowCatalogStmt showStmt = (ShowCatalogStmt) parseAndAnalyzeStmt(showCatalogSql);
        ShowResultSet showResultSet = mgr.showCatalogs(showStmt);
        Assertions.assertEquals(2, showResultSet.getResultRows().size());

        String alterCatalogNameSql = "ALTER CATALOG hms_catalog RENAME my_catalog;";
        AlterCatalogNameStmt alterNameStmt = (AlterCatalogNameStmt) parseAndAnalyzeStmt(alterCatalogNameSql);
        mgr.alterCatalogName(alterNameStmt);

        String alterCatalogProps = "ALTER CATALOG my_catalog SET PROPERTIES"
                + " (\"type\" = \"hms\", \"k\" = \"v\");";
        AlterCatalogPropertyStmt alterPropStmt = (AlterCatalogPropertyStmt) parseAndAnalyzeStmt(alterCatalogProps);
        mgr.alterCatalogProps(alterPropStmt);

        showResultSet = mgr.showCatalogs(showStmt);
        for (List<String> row : showResultSet.getResultRows()) {
            if (row.get(1).equals("internal")) {
                continue;
            }
            Assertions.assertEquals("my_catalog", row.get(0));
        }

        String showDetailCatalog = "SHOW CATALOG my_catalog";
        ShowCatalogStmt showDetailStmt = (ShowCatalogStmt) parseAndAnalyzeStmt(showDetailCatalog);
        showResultSet = mgr.showCatalogs(showDetailStmt);

        for (List<String> row : showResultSet.getResultRows()) {
            Assertions.assertEquals(2, row.size());
            if (row.get(0).equalsIgnoreCase("type")) {
                Assertions.assertEquals("hms", row.get(1));
            } else if (row.get(0).equalsIgnoreCase("k")) {
                Assertions.assertEquals("v", row.get(1));
            } else {
                Assertions.fail();
            }
        }

        String dropCatalogSql = "DROP CATALOG my_catalog";
        DropCatalogStmt dropCatalogStmt = (DropCatalogStmt) parseAndAnalyzeStmt(dropCatalogSql);
        mgr.dropCatalog(dropCatalogStmt);
        showResultSet = mgr.showCatalogs(showStmt);
        Assertions.assertEquals(1, showResultSet.getResultRows().size());
    }
}
