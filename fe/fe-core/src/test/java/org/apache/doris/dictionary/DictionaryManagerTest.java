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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.analyzer.UnboundDictionarySink;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoDictionaryCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.persist.CreateDictionaryPersistInfo;
import org.apache.doris.persist.DictionaryDecreaseVersionInfo;
import org.apache.doris.persist.DictionaryIncreaseVersionInfo;
import org.apache.doris.persist.DropDictionaryPersistInfo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

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
    public void testBuildDataLoadSqlQuotesIdentifierParts() {
        List<String> targetName = ImmutableList.of("target.with` db", "dict.with` union all select 1");
        List<String> targetFullName = ImmutableList.of("internal", targetName.get(0), targetName.get(1));
        List<String> sourceName = ImmutableList.of(
                "internal` catalog", "source` db", "src` union all select k, v from secret.tbl");
        Dictionary dictionary = Mockito.mock(Dictionary.class);
        Mockito.when(dictionary.getDbName()).thenReturn(targetName.get(0));
        Mockito.when(dictionary.getName()).thenReturn(targetName.get(1));
        Mockito.when(dictionary.getFullQualifiers()).thenReturn(targetFullName);
        Mockito.when(dictionary.getSourceQualifiedName()).thenReturn(sourceName);
        Mockito.when(dictionary.getColumnNames()).thenReturn(ImmutableList.of("k", "v"));

        String insertSql = DictionaryManager.buildDataLoadSql(dictionary);

        Assert.assertEquals("insert into `internal`.`target.with`` db`.`dict.with`` union all select 1` "
                + "select * from `internal`` catalog`.`source`` db`."
                + "`src`` union all select k, v from secret.tbl`", insertSql);
        ConnectContext context = new ConnectContext();
        context.setEnv(Env.getCurrentEnv());
        context.changeDefaultCatalog("external_catalog");
        context.setThreadLocalInfo();
        try {
            InsertIntoTableCommand command = (InsertIntoTableCommand) new NereidsParser().parseSingle(insertSql);
            LogicalPlan query = command.getLogicalQuery();
            Assert.assertTrue(query instanceof UnboundTableSink);
            Assert.assertEquals(targetFullName, ((UnboundTableSink<?>) query).getNameParts());

            Database database = Mockito.mock(Database.class);
            InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
            Mockito.when(database.getCatalog()).thenReturn(catalog);
            Mockito.when(catalog.getName()).thenReturn(targetFullName.get(0));
            Mockito.when(database.getFullName()).thenReturn(targetFullName.get(1));
            InsertIntoDictionaryCommand dictionaryCommand = new InsertIntoDictionaryCommand(
                    command, database, dictionary, false);
            LogicalPlan dictionaryQuery = dictionaryCommand.getLogicalQuery();
            Assert.assertTrue(dictionaryQuery instanceof UnboundDictionarySink);
            Assert.assertEquals(targetFullName, ((UnboundDictionarySink<?>) dictionaryQuery).getNameParts());

            List<UnboundRelation> sourceRelations = dictionaryQuery.collectToList(UnboundRelation.class::isInstance);
            Assert.assertEquals(1, sourceRelations.size());
            Assert.assertEquals(sourceName, sourceRelations.get(0).getNameParts());
        } finally {
            ConnectContext.remove();
        }
    }
}
