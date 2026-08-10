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

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.analyzer.UnboundDictionarySink;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoDictionaryCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

public class DictionaryManagerTest {

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

        Assertions.assertEquals("insert into `internal`.`target.with`` db`.`dict.with`` union all select 1` "
                + "select * from `internal`` catalog`.`source`` db`."
                + "`src`` union all select k, v from secret.tbl`", insertSql);
        ConnectContext context = new ConnectContext();
        context.setEnv(Env.getCurrentEnv());
        context.changeDefaultCatalog("external_catalog");
        context.setThreadLocalInfo();
        try {
            InsertIntoTableCommand command = (InsertIntoTableCommand) new NereidsParser().parseSingle(insertSql);
            LogicalPlan query = command.getLogicalQuery();
            Assertions.assertInstanceOf(UnboundTableSink.class, query);
            Assertions.assertEquals(targetFullName, ((UnboundTableSink<?>) query).getNameParts());

            InsertIntoDictionaryCommand dictionaryCommand = new InsertIntoDictionaryCommand(command, dictionary, false);
            LogicalPlan dictionaryQuery = dictionaryCommand.getLogicalQuery();
            Assertions.assertInstanceOf(UnboundDictionarySink.class, dictionaryQuery);
            Assertions.assertEquals(targetFullName, ((UnboundDictionarySink<?>) dictionaryQuery).getNameParts());

            List<UnboundRelation> sourceRelations = dictionaryQuery.collectToList(UnboundRelation.class::isInstance);
            Assertions.assertEquals(1, sourceRelations.size());
            Assertions.assertEquals(sourceName, sourceRelations.get(0).getNameParts());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testDataLoadRestoresStatusWhenCommandPreparationFails() {
        Dictionary dictionary = Mockito.mock(Dictionary.class);
        Mockito.when(dictionary.getStatus()).thenReturn(Dictionary.DictionaryStatus.NORMAL);
        Mockito.when(dictionary.trySetStatus(Dictionary.DictionaryStatus.LOADING)).thenReturn(true);
        Mockito.when(dictionary.getFullQualifiers()).thenThrow(new IllegalStateException("invalid target"));

        DictionaryManager manager = new DictionaryManager();
        ConnectContext context = new ConnectContext();
        Assertions.assertThrows(IllegalStateException.class, () -> manager.dataLoad(context, dictionary, false));

        Mockito.verify(dictionary).trySetStatus(Dictionary.DictionaryStatus.LOADING);
        Mockito.verify(dictionary).trySetStatus(Dictionary.DictionaryStatus.NORMAL);
        Mockito.verify(dictionary).setLastUpdateResult("invalid target");
    }
}
