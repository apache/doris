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

import org.apache.doris.datasource.ExternalScanTaskCacheKey;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.persist.CreateDictionaryPersistInfo;
import org.apache.doris.persist.DictionaryDecreaseVersionInfo;
import org.apache.doris.persist.DictionaryIncreaseVersionInfo;
import org.apache.doris.persist.DropDictionaryPersistInfo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

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
        Assert.assertNull(manager.getDictionary(1001));
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
        Assert.assertEquals(1, newDict.getVersion());
        Assert.assertEquals(1, manager.getDictionary(1002).getVersion());
    }

    @Test
    public void testReplayDecreaseVersionNormal() throws Exception {
        DictionaryManager manager = createManager();
        Dictionary dict = buildDictionary(1001, "db1", "dic1", 2);
        manager.replayCreateDictionary(new CreateDictionaryPersistInfo(dict));

        manager.replayDecreaseVersion(new DictionaryDecreaseVersionInfo(dict));
        Assert.assertEquals(1, manager.getDictionary(1001).getVersion());
    }

    @Test
    public void testReplayIncreaseVersionNormal() throws Exception {
        DictionaryManager manager = createManager();
        Dictionary dict = buildDictionary(1001, "db1", "dic1", 1);
        manager.replayCreateDictionary(new CreateDictionaryPersistInfo(dict));

        manager.replayIncreaseVersion(new DictionaryIncreaseVersionInfo(dict));
        Assert.assertEquals(2, manager.getDictionary(1001).getVersion());
    }

    @Test
    public void testScheduledContextCleanupReleasesExternalScanTasks() throws Exception {
        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext();
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        AtomicInteger loadCount = new AtomicInteger();
        ExternalScanTaskCacheKey<Integer> key = new ExternalScanTaskCacheKey<Integer>() { };

        statementContext.getExternalScanTaskCache().getOrLoad(
                key, () -> Collections.singletonList(loadCount.incrementAndGet()));
        DictionaryManager.cleanupScheduledContext(context);

        Assert.assertNull(ConnectContext.get());
        Assert.assertEquals(Collections.singletonList(2), statementContext.getExternalScanTaskCache().getOrLoad(
                key, () -> Collections.singletonList(loadCount.incrementAndGet())));
        Assert.assertEquals(2, loadCount.get());
    }
}
