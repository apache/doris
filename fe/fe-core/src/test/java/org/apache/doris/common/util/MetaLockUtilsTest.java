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

package org.apache.doris.common.util;

import com.google.common.collect.Lists;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class MetaLockUtilsTest {

    @Test
    public void testReadLockDatabases() {
        List<Database> databaseList = Lists.newArrayList(new Database(), new Database());
        MetaLockUtils.readLockDatabases(databaseList);
        Assert.assertFalse(databaseList.get(0).tryWriteLock(1, TimeUnit.MILLISECONDS));
        Assert.assertFalse(databaseList.get(1).tryWriteLock(1, TimeUnit.MILLISECONDS));
        MetaLockUtils.readUnlockDatabases(databaseList);
        Assert.assertTrue(databaseList.get(0).tryWriteLock(1, TimeUnit.MILLISECONDS));
        Assert.assertTrue(databaseList.get(1).tryWriteLock(1, TimeUnit.MILLISECONDS));
        databaseList.get(0).writeUnlock();
        databaseList.get(1).writeUnlock();
    }

    @Test
    public void testReadLockTables() {
        List<Table> tableList = Lists.newArrayList(new Table(Table.TableType.OLAP), new Table(Table.TableType.OLAP));
        MetaLockUtils.readLockTables(tableList);
        Assert.assertFalse(tableList.get(0).tryWriteLock(1, TimeUnit.MILLISECONDS));
        Assert.assertFalse(tableList.get(1).tryWriteLock(1, TimeUnit.MILLISECONDS));
        MetaLockUtils.readUnlockTables(tableList);
        Assert.assertTrue(tableList.get(0).tryWriteLock(1, TimeUnit.MILLISECONDS));
        Assert.assertTrue(tableList.get(1).tryWriteLock(1, TimeUnit.MILLISECONDS));
        tableList.get(0).writeUnlock();
        tableList.get(1).writeUnlock();
    }

    @Test
    public void testWriteLockTables() {
        List<Table> tableList = Lists.newArrayList(new Table(Table.TableType.OLAP), new Table(Table.TableType.OLAP));
        MetaLockUtils.writeLockTables(tableList);
        Assert.assertTrue(tableList.get(0).isWriteLockHeldByCurrentThread());
        Assert.assertTrue(tableList.get(1).isWriteLockHeldByCurrentThread());
        MetaLockUtils.writeUnlockTables(tableList);
        Assert.assertFalse(tableList.get(0).isWriteLockHeldByCurrentThread());
        Assert.assertFalse(tableList.get(1).isWriteLockHeldByCurrentThread());
        Assert.assertTrue(MetaLockUtils.tryWriteLockTables(tableList, 1, TimeUnit.MILLISECONDS));
        Assert.assertTrue(tableList.get(0).isWriteLockHeldByCurrentThread());
        Assert.assertTrue(tableList.get(1).isWriteLockHeldByCurrentThread());
        MetaLockUtils.writeUnlockTables(tableList);
        tableList.get(1).readLock();
        Assert.assertFalse(MetaLockUtils.tryWriteLockTables(tableList, 1, TimeUnit.MILLISECONDS));
        Assert.assertFalse(tableList.get(0).isWriteLockHeldByCurrentThread());
        Assert.assertFalse(tableList.get(1).isWriteLockHeldByCurrentThread());
        tableList.get(1).readUnlock();
    }
}
