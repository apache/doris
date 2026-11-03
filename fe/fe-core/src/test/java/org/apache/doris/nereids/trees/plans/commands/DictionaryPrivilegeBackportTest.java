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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.dictionary.DictionaryManager;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.refresh.RefreshDictionaryCommand;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class DictionaryPrivilegeBackportTest {
    private ConnectContext context;
    private DictionaryManager manager;
    private AccessControllerManager access;
    private Dictionary dictionary;
    private MockedStatic<Env> environment;

    @BeforeEach
    void setUp() throws Exception {
        Env env = Mockito.mock(Env.class);
        context = Mockito.mock(ConnectContext.class);
        manager = Mockito.mock(DictionaryManager.class);
        access = Mockito.mock(AccessControllerManager.class);
        dictionary = Mockito.mock(Dictionary.class);
        Mockito.when(context.getEnv()).thenReturn(env);
        Mockito.when(context.getDatabase()).thenReturn("db");
        Mockito.when(context.getQualifiedUser()).thenReturn("viewer");
        Mockito.when(context.getRemoteIP()).thenReturn("127.0.0.1");
        Mockito.when(env.getDictionaryManager()).thenReturn(manager);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        Mockito.when(manager.getDictionaries("db")).thenReturn(ImmutableMap.of("dict", dictionary));
        environment = Mockito.mockStatic(Env.class);
        environment.when(Env::getCurrentEnv).thenReturn(env);
    }

    @AfterEach
    void tearDown() {
        environment.close();
    }

    @Test
    void hiddenDictionariesDoNotTriggerStatusRpc() throws Exception {
        Assertions.assertTrue(new ShowDictionariesCommand(null).doRun(context, null).getResultRows().isEmpty());
        Mockito.verify(manager, Mockito.never()).collectDictionaryStatus(Mockito.anyList());
        Mockito.verify(access).checkTblPriv(context, InternalCatalog.INTERNAL_CATALOG_NAME,
                "db", "dict", PrivPredicate.SHOW);
    }

    @Test
    void explainChecksPrivilegeBeforeLookingUpDictionary() throws Exception {
        Assertions.assertThrows(AnalysisException.class,
                () -> new ExplainDictionaryCommand(null, "dict").doRun(context, null));
        Mockito.verify(manager, Mockito.never()).getDictionary(Mockito.anyString(), Mockito.anyString());
    }

    @Test
    void refreshChecksPrivilegeBeforeLookingUpDictionary() throws Exception {
        Assertions.assertThrows(AnalysisException.class,
                () -> new RefreshDictionaryCommand(null, "dict").run(context, null));
        Mockito.verify(manager, Mockito.never()).getDictionary(Mockito.anyString(), Mockito.anyString());
        Mockito.verify(access).checkTblPriv(context, InternalCatalog.INTERNAL_CATALOG_NAME,
                "db", "dict", PrivPredicate.LOAD);
    }

    @Test
    void authorizedExplainUsesDictionarySchema() throws Exception {
        Mockito.when(access.checkTblPriv(context, InternalCatalog.INTERNAL_CATALOG_NAME,
                "db", "dict", PrivPredicate.SHOW)).thenReturn(true);
        Mockito.when(manager.getDictionary("db", "dict")).thenReturn(dictionary);
        Mockito.when(dictionary.getDicColumns()).thenReturn(ImmutableList.of());
        Assertions.assertTrue(new ExplainDictionaryCommand(null, "dict")
                .doRun(context, null).getResultRows().isEmpty());
        Mockito.verify(dictionary).getDicColumns();
    }

    @Test
    void authorizedRefreshStartsLoad() throws Exception {
        Mockito.when(access.checkTblPriv(context, InternalCatalog.INTERNAL_CATALOG_NAME,
                "db", "dict", PrivPredicate.LOAD)).thenReturn(true);
        Mockito.when(manager.getDictionary("db", "dict")).thenReturn(dictionary);
        new RefreshDictionaryCommand(null, "dict").run(context, null);
        Mockito.verify(manager).dataLoad(context, dictionary, false);
    }
}
