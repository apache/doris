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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSetMetaData;
import org.apache.doris.qe.help.HelpModule;
import org.apache.doris.qe.help.HelpTopic;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class SchemaOnlyCommandMetadataTest {
    private static List<String> names(ResultSetMetaData metadata) {
        return metadata.getColumns().stream().map(column -> column.getName()).collect(Collectors.toList());
    }

    @Test
    void helpTopicDoesNotReadDescriptionOrExample() throws Exception {
        HelpModule module = Mockito.mock(HelpModule.class);
        HelpTopic topic = Mockito.mock(HelpTopic.class);
        Mockito.when(module.getTopic("topic")).thenReturn(topic);
        try (MockedStatic<HelpModule> ignored = Mockito.mockStatic(HelpModule.class)) {
            ignored.when(HelpModule::getInstance).thenReturn(module);
            HelpCommand command = new HelpCommand("topic");
            Assertions.assertEquals(Arrays.asList("name", "description", "example"), names(command.getMetaData(null)));
            Mockito.verifyNoInteractions(topic);
            Assertions.assertEquals(names(command.doRun(null, null).getMetaData()), names(command.getMetaData(null)));
        }
    }

    @Test
    void helpKeywordWithOneTopicUsesTopicHeader() throws Exception {
        HelpModule module = Mockito.mock(HelpModule.class);
        Mockito.when(module.listTopicByKeyword("keyword")).thenReturn(Collections.singletonList("topic"));
        Mockito.when(module.getTopic("topic")).thenReturn(Mockito.mock(HelpTopic.class));
        try (MockedStatic<HelpModule> ignored = Mockito.mockStatic(HelpModule.class)) {
            ignored.when(HelpModule::getInstance).thenReturn(module);
            HelpCommand command = new HelpCommand("keyword");
            Assertions.assertEquals(Arrays.asList("name", "description", "example"), names(command.getMetaData(null)));
            Assertions.assertEquals(names(command.doRun(null, null).getMetaData()), names(command.getMetaData(null)));
        }
    }

    @Test
    void helpKeywordAndCategoryHeadersMatchExecution() throws Exception {
        HelpModule module = Mockito.mock(HelpModule.class);
        Mockito.when(module.listTopicByKeyword("keyword")).thenReturn(Arrays.asList("first", "second"));
        Mockito.when(module.listCategoryByName("categories")).thenReturn(Arrays.asList("first", "second"));
        Mockito.when(module.listCategoryByName("category")).thenReturn(Collections.singletonList("first"));
        try (MockedStatic<HelpModule> ignored = Mockito.mockStatic(HelpModule.class)) {
            ignored.when(HelpModule::getInstance).thenReturn(module);
            for (String mark : Arrays.asList("keyword", "categories", "category", "missing")) {
                HelpCommand command = new HelpCommand(mark);
                List<String> expected = mark.equals("categories")
                        ? Arrays.asList("source_category_name", "name", "is_it_category")
                        : Arrays.asList("name", "is_it_category");
                Assertions.assertEquals(expected, names(command.getMetaData(null)));
                Assertions.assertEquals(names(command.doRun(null, null).getMetaData()), names(command.getMetaData(null)));
            }
        }
    }

    @Test
    void invalidHelpAndSnapshotFiltersAreRejected() throws Exception {
        Assertions.assertThrows(AnalysisException.class, () -> new HelpCommand("").getMetaData(null));
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(env.getAccessManager().checkGlobalPriv(Mockito.nullable(ConnectContext.class),
                Mockito.eq(PrivPredicate.ADMIN))).thenReturn(true);
        try (MockedStatic<Env> ignored = Mockito.mockStatic(Env.class)) {
            ignored.when(Env::getCurrentEnv).thenReturn(env);
            ShowSnapshotCommand command = (ShowSnapshotCommand) new NereidsParser()
                    .parseSingle("SHOW SNAPSHOT ON example_repo WHERE TIMESTAMP = '2026-01-01-00-00-00'");
            Assertions.assertThrows(AnalysisException.class, () -> command.getMetaData(null));
            Mockito.verify(env, Mockito.never()).getBackupHandler();
        }
    }

    @Test
    void filteredSnapshotsResolveWithoutRepositoryAccess() throws Exception {
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(env.getAccessManager().checkGlobalPriv(Mockito.nullable(ConnectContext.class),
                Mockito.eq(PrivPredicate.ADMIN))).thenReturn(true);
        try (MockedStatic<Env> ignored = Mockito.mockStatic(Env.class)) {
            ignored.when(Env::getCurrentEnv).thenReturn(env);
            for (String filter : Arrays.asList("", " WHERE SNAPSHOT = 'sample'",
                    " WHERE SNAPSHOT = 'sample' AND TIMESTAMP = '2026-01-01-00-00-00'")) {
                ShowSnapshotCommand command = (ShowSnapshotCommand) new NereidsParser()
                        .parseSingle("SHOW SNAPSHOT ON example_repo" + filter);
                Assertions.assertEquals(filter.contains("TIMESTAMP")
                        ? ShowSnapshotCommand.SNAPSHOT_DETAIL : ShowSnapshotCommand.SNAPSHOT_ALL,
                        names(command.getMetaData(null)));
            }
            Mockito.verify(env, Mockito.never()).getBackupHandler();
        }
    }
}
