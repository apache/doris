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

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

public class DatasourceMgrTest extends TestWithFeService {
    private DataSourceMgr mgr;
    private static final String MY_CATALOG = "my_catalog";

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

        String alterCatalogNameSql = "ALTER CATALOG hms_catalog RENAME " + MY_CATALOG + ";";
        AlterCatalogNameStmt alterNameStmt = (AlterCatalogNameStmt) parseAndAnalyzeStmt(alterCatalogNameSql);
        mgr.alterCatalogName(alterNameStmt);

        String alterCatalogProps = "ALTER CATALOG " + MY_CATALOG + " SET PROPERTIES"
                + " (\"type\" = \"hms\", \"k\" = \"v\");";
        AlterCatalogPropertyStmt alterPropStmt = (AlterCatalogPropertyStmt) parseAndAnalyzeStmt(alterCatalogProps);
        mgr.alterCatalogProps(alterPropStmt);

        showResultSet = mgr.showCatalogs(showStmt);
        for (List<String> row : showResultSet.getResultRows()) {
            if (row.get(1).equals(InternalDataSource.INTERNAL_DS_NAME)) {
                continue;
            }
            Assertions.assertEquals(MY_CATALOG, row.get(0));
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

        testDataSourceMgrPersist();

        String dropCatalogSql = "DROP CATALOG " + MY_CATALOG;
        DropCatalogStmt dropCatalogStmt = (DropCatalogStmt) parseAndAnalyzeStmt(dropCatalogSql);
        mgr.dropCatalog(dropCatalogStmt);
        showResultSet = mgr.showCatalogs(showStmt);
        Assertions.assertEquals(1, showResultSet.getResultRows().size());
    }

    private void testDataSourceMgrPersist() throws Exception {
        File file = new File("./CatalogMgrTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        mgr.write(dos);
        dos.flush();
        dos.close();

        DataSourceIf internalCatalog = mgr.getCatalog(InternalDataSource.INTERNAL_DS_ID);
        DataSourceIf internalCatalog2 = mgr.getInternalDataSource();
        Assert.assertTrue(internalCatalog == internalCatalog2);
        DataSourceIf myCatalog = mgr.getCatalog(MY_CATALOG);
        Assert.assertNotNull(myCatalog);

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        DataSourceMgr mgr2 = DataSourceMgr.read(dis);

        Assert.assertEquals(2, mgr2.listCatalogs().size());
        Assert.assertEquals(myCatalog.getId(), mgr2.getCatalog(MY_CATALOG).getId());
        Assert.assertEquals(0, mgr2.getInternalDataSource().getId());
        Assert.assertEquals(0, mgr2.getCatalog(InternalDataSource.INTERNAL_DS_ID).getId());
        Assert.assertEquals(0, mgr2.getCatalog(InternalDataSource.INTERNAL_DS_NAME).getId());

        DataSourceIf hms = mgr2.getCatalog(MY_CATALOG);
        Map<String, String> properties = hms.getProperties();
        Assert.assertEquals(2, properties.size());
        Assert.assertEquals("hms", properties.get("type"));
        Assert.assertEquals("v", properties.get("k"));

        // 3. delete files
        dis.close();
        file.delete();
    }
}
