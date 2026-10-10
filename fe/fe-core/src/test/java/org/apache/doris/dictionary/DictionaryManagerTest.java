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

package org.apache.doris.dictionary;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.plans.commands.info.CreateDictionaryInfo;
import org.apache.doris.persist.CreateDictionaryPersistInfo;
import org.apache.doris.persist.DictionaryDecreaseVersionInfo;
import org.apache.doris.persist.DictionaryIncreaseVersionInfo;
import org.apache.doris.persist.DropDictionaryPersistInfo;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Tests for dictionary version journal replay robustness.
 *
 * The crash in production: an async data load task writes the INC journal, then DROP removes the
 * dictionary, then the failed commit writes a DEC journal for the already-dropped dictionary.
 * Followers crash at replay because the dictionary cannot be found by name anymore.
 * Replay must be idempotent and lookup by dictionary id.
 */
public class DictionaryManagerTest {

    private DictionaryManager createManager() {
        return new DictionaryManager();
    }

    private Dictionary buildDictionary(long id, String dbName, String dictName, long version) {
        String json = String.format(
                "{\"clazz\":\"Dictionary\",\"id\":%d,\"name\":\"%s\",\"dbName\":\"%s\","
                        + "\"sourceTableName\":\"src_%s\",\"version\":%d}",
                id, dictName, dbName, dbName, version);
        return GsonUtils.GSON.fromJson(json, Dictionary.class);
    }

    @Test
    public void testReplayDecreaseVersionMissingDictionary() throws Exception {
        DictionaryManager manager = createManager();
        // dictionary never created on this FE
        Dictionary dict = buildDictionary(1001, "db1", "dic1", 2);
        manager.replayDecreaseVersion(new DictionaryDecreaseVersionInfo(dict));
    }

    @Test
    public void testReplayIncreaseVersionMissingDictionary() throws Exception {
        DictionaryManager manager = createManager();
        Dictionary dict = buildDictionary(1001, "db1", "dic1", 1);
        manager.replayIncreaseVersion(new DictionaryIncreaseVersionInfo(dict));
    }

    @Test
    public void testReplayDecreaseVersionAfterDrop() throws Exception {
        DictionaryManager manager = createManager();
        Dictionary dict = buildDictionary(1001, "db1", "dic1", 2);
        manager.replayCreateDictionary(new CreateDictionaryPersistInfo(dict));
        manager.replayDropDictionary(new DropDictionaryPersistInfo("db1", "dic1"));

        // journal order CREATE -> INC -> DROP -> DEC, DEC must be a no-op, not an exception
        manager.replayDecreaseVersion(new DictionaryDecreaseVersionInfo(dict));
        Assertions.assertNull(manager.getDictionary(1001));
    }

    @Test
    public void testReplayDecreaseVersionAbA() throws Exception {
        DictionaryManager manager = createManager();
        Dictionary oldDict = buildDictionary(1001, "db1", "dic1", 2);
        manager.replayCreateDictionary(new CreateDictionaryPersistInfo(oldDict));
        manager.replayDropDictionary(new DropDictionaryPersistInfo("db1", "dic1"));
        Dictionary newDict = buildDictionary(1002, "db1", "dic1", 1);
        manager.replayCreateDictionary(new CreateDictionaryPersistInfo(newDict));

        // DEC of the dropped dictionary must not affect the recreated same-name dictionary
        manager.replayDecreaseVersion(new DictionaryDecreaseVersionInfo(oldDict));
        Assertions.assertEquals(1, newDict.getVersion());
        Assertions.assertEquals(1, manager.getDictionary(1002).getVersion());
    }

    @Test
    public void testReplayDecreaseVersionNormal() throws Exception {
        DictionaryManager manager = createManager();
        Dictionary dict = buildDictionary(1001, "db1", "dic1", 2);
        manager.replayCreateDictionary(new CreateDictionaryPersistInfo(dict));

        manager.replayDecreaseVersion(new DictionaryDecreaseVersionInfo(dict));
        Assertions.assertEquals(1, manager.getDictionary(1001).getVersion());
    }

    @Test
    public void testReplayIncreaseVersionNormal() throws Exception {
        DictionaryManager manager = createManager();
        Dictionary dict = buildDictionary(1001, "db1", "dic1", 1);
        manager.replayCreateDictionary(new CreateDictionaryPersistInfo(dict));

        manager.replayIncreaseVersion(new DictionaryIncreaseVersionInfo(dict));
        Assertions.assertEquals(2, manager.getDictionary(1001).getVersion());
    }

    @Test
    public void testCreateDictionaryRejectsTableNameCollision() {
        // A dictionary is authorized with the table privilege key, so creating a dictionary
        // whose name is already taken by a table must be rejected, even with IF NOT EXISTS.
        Database db = new Database(1, "db1");
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.getId()).thenReturn(100L);
        Mockito.when(table.getName()).thenReturn("foo");
        db.registerTable(table);

        DictionaryManager manager = createManager();
        CreateDictionaryInfo info = new CreateDictionaryInfo(false, "db1", "foo", "internal", "db1", "src",
                ImmutableList.of(), Maps.newHashMap(), LayoutType.HASH_MAP);

        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbNullable("db1")).thenReturn(db);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            DdlException exception = Assertions.assertThrows(DdlException.class,
                    () -> manager.createDictionary(null, info));
            Assertions.assertTrue(exception.getMessage().contains(
                    "because a table with the same name already exists"));
        }
    }

    @Test
    public void testHasDictionary() throws Exception {
        DictionaryManager manager = createManager();
        Dictionary dict = buildDictionary(1001, "db1", "dic1", 1);
        manager.replayCreateDictionary(new CreateDictionaryPersistInfo(dict));

        Assertions.assertTrue(manager.hasDictionary("db1", "dic1"));
        Assertions.assertFalse(manager.hasDictionary("db1", "missing"));
        Assertions.assertFalse(manager.hasDictionary("missing_db", "dic1"));
    }
}
