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
import org.apache.doris.common.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class CatalogRecycleBinRecoveryTest {
    @Test
    public void testMissingTablePreservesRecycledMetadata() throws Exception {
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        Database db = new Database(9200000L, "recover_missing_table");
        OlapTable first = createTable(9200001L, "t1");
        OlapTable second = createTable(9200002L, "t2");
        Assertions.assertTrue(recycleBin.recycleTable(db.getId(), first, false, false, 0));
        Assertions.assertTrue(recycleBin.recycleTable(db.getId(), second, false, false, 0));
        Assertions.assertTrue(recycleBin.recycleDatabase(db, Set.of(first.getName(), second.getName()),
                Set.of(first.getId(), second.getId()), false, false, 0));
        recycleBin.replayEraseTable(second.getId());
        Assertions.assertNull(recycleBin.getRecycledTable(second.getId()));
        Long databaseRecycleTime = recycleBin.getRecycleTimeById(db.getId());
        Long tableRecycleTime = recycleBin.getRecycleTimeById(first.getId());

        for (int attempt = 0; attempt < 2; attempt++) {
            DdlException error = Assertions.assertThrows(DdlException.class,
                    () -> recycleBin.recoverDatabase(db.getFullName(), db.getId()));
            Assertions.assertEquals("Tables[[t2]] is missing. Can not recover db", error.getDetailMessage());
            Assertions.assertTrue(db.getTables().isEmpty());
            Assertions.assertTrue(recycleBin.isRecycleDatabase(db.getId()));
            Assertions.assertSame(first, recycleBin.getRecycledTable(first.getId()));
            Assertions.assertEquals(databaseRecycleTime, recycleBin.getRecycleTimeById(db.getId()));
            Assertions.assertEquals(tableRecycleTime, recycleBin.getRecycleTimeById(first.getId()));
            Assertions.assertEquals(Set.of(first.getId()),
                    recycleBin.dbIdTableNameToIds.get(Pair.of(db.getId(), first.getName())));
        }
    }

    @Test
    public void testRecoverDatabaseKeepsOtherTableGenerations() throws Exception {
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        Database db = new Database(9300000L, "recover_table_generations");
        OlapTable first = createTable(9300001L, "t1");
        OlapTable second = createTable(9300002L, "t2");
        OlapTable older = createTable(9300003L, "t1");
        Assertions.assertTrue(recycleBin.recycleTable(db.getId(), first, false, false, 0));
        Assertions.assertTrue(recycleBin.recycleTable(db.getId(), second, false, false, 0));
        Assertions.assertTrue(recycleBin.recycleTable(db.getId(), older, false, false, 0));
        Assertions.assertTrue(recycleBin.recycleDatabase(db, Set.of(first.getName(), second.getName()),
                Set.of(first.getId(), second.getId()), false, false, 0));

        Assertions.assertSame(db, recycleBin.recoverDatabase(db.getFullName(), db.getId()));
        Assertions.assertSame(first, db.getTableNullable(first.getId()));
        Assertions.assertSame(second, db.getTableNullable(second.getId()));
        Assertions.assertFalse(recycleBin.isRecycleDatabase(db.getId()));
        Assertions.assertNull(recycleBin.getRecycledTable(first.getId()));
        Assertions.assertNull(recycleBin.getRecycledTable(second.getId()));
        Assertions.assertSame(older, recycleBin.getRecycledTable(older.getId()));
        Assertions.assertEquals(Set.of(older.getId()),
                recycleBin.dbIdTableNameToIds.get(Pair.of(db.getId(), first.getName())));
        Assertions.assertNull(recycleBin.dbIdTableNameToIds.get(Pair.of(db.getId(), second.getName())));
    }

    private OlapTable createTable(long id, String name) {
        Column column = new Column("k", Type.INT);
        return new OlapTable(id, name, List.of(column), KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new HashDistributionInfo(1, List.of(column)));
    }
}
