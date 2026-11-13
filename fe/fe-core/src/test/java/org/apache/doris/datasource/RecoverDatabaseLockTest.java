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

import org.apache.doris.catalog.CatalogRecycleBin;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class RecoverDatabaseLockTest {
    @Test
    public void testLockTimeoutKeepsDatabaseAndTableInRecycleBin() throws Exception {
        InternalCatalog catalog = Env.getCurrentInternalCatalog();
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();
        Database db = new Database(9100000L, "recover_catalog_lock_timeout");
        Column column = new Column("k", Type.INT);
        OlapTable table = new OlapTable(9100001L, "t", List.of(column), KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new HashDistributionInfo(1, List.of(column)));
        Assertions.assertTrue(recycleBin.recycleTable(db.getId(), table, false, false, 0));
        Assertions.assertTrue(recycleBin.recycleDatabase(db, Set.of(table.getName()), Set.of(table.getId()),
                false, false, 0));
        long recycleTime = recycleBin.getRecycleTimeById(db.getId());
        long oldTimeout = Config.catalog_try_lock_timeout_ms;
        ReentrantLock catalogLock = getCatalogLock(catalog);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        catalogLock.lock();
        try {
            Config.catalog_try_lock_timeout_ms = 50;
            DdlException error = executor.submit(() -> Assertions.assertThrows(DdlException.class,
                    () -> catalog.recoverDatabase(db.getFullName(), db.getId(), "")))
                    .get(5, TimeUnit.SECONDS);
            Assertions.assertTrue(error.getMessage().contains("Failed to acquire catalog lock"));
            Assertions.assertNull(catalog.getDbNullable(db.getId()));
            Assertions.assertTrue(recycleBin.isRecycleDatabase(db.getId()));
            Assertions.assertTrue(recycleBin.isRecycleTable(db.getId(), table.getId()));
            Assertions.assertEquals(recycleTime, recycleBin.getRecycleTimeById(db.getId()));
            Assertions.assertTrue(db.getTables().isEmpty());
        } finally {
            Config.catalog_try_lock_timeout_ms = oldTimeout;
            catalogLock.unlock();
            executor.shutdownNow();
        }

        Assertions.assertSame(db, recycleBin.recoverDatabase(db.getFullName(), db.getId()));
        Assertions.assertSame(table, db.getTableNullable(table.getId()));
        Assertions.assertFalse(recycleBin.isRecycleDatabase(db.getId()));
        Assertions.assertFalse(recycleBin.isRecycleTable(db.getId(), table.getId()));
    }

    @Test
    public void testUnknownDatabaseReleasesCatalogLock() throws Exception {
        InternalCatalog catalog = Env.getCurrentInternalCatalog();
        Assertions.assertThrows(DdlException.class,
                () -> catalog.recoverDatabase("unknown_recover_catalog_database", -1, ""));
        ReentrantLock catalogLock = getCatalogLock(catalog);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(() -> {
                Assertions.assertTrue(catalogLock.tryLock(5, TimeUnit.SECONDS));
                catalogLock.unlock();
                return null;
            }).get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    private ReentrantLock getCatalogLock(InternalCatalog catalog) throws Exception {
        Field field = InternalCatalog.class.getDeclaredField("lock");
        field.setAccessible(true);
        return (ReentrantLock) field.get(catalog);
    }
}
