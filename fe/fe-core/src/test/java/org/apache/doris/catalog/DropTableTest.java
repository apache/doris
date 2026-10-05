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
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DropTableTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.tbl1(k1 int, k2 bigint) duplicate key(k1) "
                + "distributed by hash(k2) buckets 1 properties('replication_num' = '1');");
        createTable("create table test.tbl2(k1 int, k2 bigint)" + "duplicate key(k1) "
                + "distributed by hash(k2) buckets 1 " + "properties('replication_num' = '1');");
    }

    @Test
    public void testNormalDropTable() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl1");
        Partition partition = table.getAllPartitions().iterator().next();
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        dropTableWithSql("drop table test.tbl1");
        List<Replica> replicaList = Env.getCurrentEnv().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        Assertions.assertEquals(1, replicaList.size());
        recoverTable("recover table test.tbl1");
        table = (OlapTable) db.getTableOrMetaException("tbl1");
        Assertions.assertNotNull(table);
        Assertions.assertEquals("tbl1", table.getName());
    }

    @Test
    public void testForceDropTable() throws Exception {
        dropTableWithSql("drop table test.tbl2 force");
        // After unify force and non-force drop table, the replicas will be recycled eventually.
        //
        // Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        // OlapTable table = (OlapTable) db.getTableOrMetaException("tbl2");
        // Partition partition = table.getAllPartitions().iterator().next();
        // long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        // ...
        // List<Replica> replicaList = Env.getCurrentEnv().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        // Assert.assertTrue(replicaList.isEmpty());
        String recoverDbSql = "recover table test.tbl2";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Unknown table 'tbl2' or table id '-1' in test",
                () -> recoverTable(recoverDbSql));
    }
}
