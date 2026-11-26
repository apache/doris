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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.RecoverDatabaseCommand;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DropDbTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseWithSql("create database test1;");
        createDatabaseWithSql("create database test2;");
        createDatabaseWithSql("create database test3;");
        createTable("create table test1.tbl1(k1 int, k2 bigint) duplicate key(k1) "
                + "distributed by hash(k2) buckets 1" + " properties('replication_num' = '1');");
        createTable("create table test2.tbl1" + "(k1 int, k2 bigint)" + " duplicate key(k1) "
                + "distributed by hash(k2) buckets 1 " + "properties('replication_num' = '1');");
    }

    @Test
    public void testNormalDropDb() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test1");
        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl1");
        Partition partition = table.getAllPartitions().iterator().next();
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        String dropDbSql = "drop database test1";
        dropDatabaseWithSql(dropDbSql);
        db = Env.getCurrentInternalCatalog().getDbNullable("test1");
        Assertions.assertNull(db);
        List<Replica> replicaList = Env.getCurrentEnv().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        Assertions.assertEquals(1, replicaList.size());
        String recoverDbSql = "recover database test1";
        NereidsParser nereidsParser = new NereidsParser();
        RecoverDatabaseCommand command = (RecoverDatabaseCommand) nereidsParser.parseSingle(recoverDbSql);
        command.run(connectContext, null);
        db = Env.getCurrentInternalCatalog().getDbNullable("test1");
        Assertions.assertNotNull(db);
        Assertions.assertEquals("test1", db.getFullName());
        table = (OlapTable) db.getTableOrMetaException("tbl1");
        Assertions.assertNotNull(table);
        Assertions.assertEquals("tbl1", table.getName());

        dropDbSql = "drop schema test1";
        dropDatabaseWithSql(dropDbSql);
        db = Env.getCurrentInternalCatalog().getDbNullable("test1");
        Assertions.assertNull(db);
        command.run(connectContext, null);
        db = Env.getCurrentInternalCatalog().getDbNullable("test1");
        Assertions.assertNotNull(db);

        dropDbSql = "drop schema if exists test1";
        dropDatabaseWithSql(dropDbSql);
        db = Env.getCurrentInternalCatalog().getDbNullable("test1");
        Assertions.assertNull(db);
    }

    @Test
    public void testForceDropDb() throws Exception {
        String dropDbSql = "drop database test2 force";
        dropDatabaseWithSql(dropDbSql);
        Database db = Env.getCurrentInternalCatalog().getDbNullable("test2");
        Assertions.assertNull(db);
        // After unify force and non-force drop db, the replicas will be recycled eventually.
        //
        // Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test2");
        // OlapTable table = (OlapTable) db.getTableOrMetaException("tbl1");
        // Partition partition = table.getAllPartitions().iterator().next();
        // long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        // ...
        // List<Replica> replicaList = Env.getCurrentEnv().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        // Assert.assertTrue(replicaList.isEmpty());
        String recoverDbSql = "recover database test2";
        NereidsParser nereidsParser = new NereidsParser();
        RecoverDatabaseCommand command = (RecoverDatabaseCommand) nereidsParser.parseSingle(recoverDbSql);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Unknown database 'test2' or database id '-1'",
                () -> command.run(connectContext, null));
        dropDbSql = "drop schema test3 force";
        db = Env.getCurrentInternalCatalog().getDbOrMetaException("test3");
        Assertions.assertNotNull(db);
        dropDatabaseWithSql(dropDbSql);
        db = Env.getCurrentInternalCatalog().getDbNullable("test3");
        Assertions.assertNull(db);
        recoverDbSql = "recover database test3";
        RecoverDatabaseCommand command2 = (RecoverDatabaseCommand) nereidsParser.parseSingle(recoverDbSql);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Unknown database 'test3'",
                () -> command2.run(connectContext, null));

        dropDbSql = "drop schema if exists test3 force";
        dropDatabaseWithSql(dropDbSql);
    }
}
