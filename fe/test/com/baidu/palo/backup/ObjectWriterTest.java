// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.backup;

import com.baidu.palo.analysis.CreateTableStmt;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.common.FeConstants;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("org.apache.log4j.*")
@PrepareForTest(Catalog.class)
public class ObjectWriterTest {

    private static Catalog catalog;

    @BeforeClass
    public static void setUp() {
        // Config.meta_dir = "./palo-meta";

        catalog = CatalogMocker.fetchAdminCatalog();
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentCatalogJournalVersion()).andReturn(FeConstants.meta_version).anyTimes();
        PowerMock.replay(Catalog.class);
    }

    @Test
    public void test_write_and_read_createTableStmt() throws IOException {
        Database db = Catalog.getInstance().getDb(CatalogMocker.TEST_DB_ID);

        // write olap table
        Table olapTable = db.getTable(CatalogMocker.TEST_TBL_ID);
        int tableSignature = olapTable.getSignature(BackupVersion.VERSION_1);
        CreateTableStmt stmt = olapTable.toCreateTableStmt(db.getFullName());
        stmt.setTableSignature(tableSignature);

        PathBuilder pathBuilder =
                PathBuilder.createPathBuilder(Joiner.on("/").join("test_label", CatalogMocker.TEST_DB_NAME));
        String filePath = pathBuilder.createTableStmt(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        ObjectWriter.write(filePath, Lists.newArrayList(stmt));

        // read olap table
        stmt = ObjectWriter.readCreateTableStmt(filePath);
        System.out.println(stmt.toSql());

        System.out.println("get signature: " + stmt.getTableSignature());
        System.out.println("table signature: " + tableSignature);
        if (stmt.getTableSignature() == tableSignature) {
            System.out.println("get same signature: " + tableSignature);
        } else {
            Assert.fail();
        }

        // write mysql table
        Table mysqlTable = db.getTable(CatalogMocker.MYSQL_TABLE_NAME);
        tableSignature = mysqlTable.getSignature(BackupVersion.VERSION_1);
        stmt = mysqlTable.toCreateTableStmt(db.getFullName());
        stmt.setTableSignature(tableSignature);

        filePath = pathBuilder.createTableStmt(CatalogMocker.TEST_DB_NAME, CatalogMocker.MYSQL_TABLE_NAME);
        ObjectWriter.write(filePath, Lists.newArrayList(stmt));

        // read mysql table
        stmt = ObjectWriter.readCreateTableStmt(filePath);
        System.out.println(stmt.toSql());

        System.out.println("get signature: " + stmt.getTableSignature());
        System.out.println("table signature: " + tableSignature);
        if (stmt.getTableSignature() == tableSignature) {
            System.out.println("get same signature: " + tableSignature);
        } else {
            Assert.fail();
        }

        pathBuilder.getRoot().getTopParent().print(" ");
    }
}
