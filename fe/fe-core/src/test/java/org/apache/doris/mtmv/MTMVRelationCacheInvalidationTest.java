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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class MTMVRelationCacheInvalidationTest {
    @Test
    void tableInvalidationUsesTheExistingGenerationBarrier() {
        MTMVRelationManager manager = new MTMVRelationManager();
        BaseTableInfo source = name("lake", "source", "t");
        addDependency(manager, source, "dependent");
        addDependency(manager, name("lake", "source", "other"), "unrelated");
        MTMV dependent = Mockito.mock(MTMV.class);
        MTMV unrelated = Mockito.mock(MTMV.class);
        try (MockedStatic<Env> env = localViews(dependent, unrelated)) {
            manager.invalidateRewriteCaches("lake", "source", "t");
            Mockito.verify(dependent).invalidateRewriteCache();
            Mockito.verify(unrelated, Mockito.never()).invalidateRewriteCache();
        }
    }

    @Test
    void namespaceInvalidationDoesNotRequireWarmBaseObjects() {
        MTMVRelationManager manager = new MTMVRelationManager();
        addDependency(manager, name("lake", "first", "t"), "dependent");
        addDependency(manager, name("lake", "second", "t"), "unrelated");
        MTMV first = Mockito.mock(MTMV.class);
        MTMV second = Mockito.mock(MTMV.class);
        try (MockedStatic<Env> env = localViews(first, second)) {
            manager.invalidateRewriteCaches("lake", "first", null);
            Mockito.verify(first).invalidateRewriteCache();
            Mockito.verify(second, Mockito.never()).invalidateRewriteCache();
            manager.invalidateRewriteCaches("lake", null, null);
            Mockito.verify(first, Mockito.times(2)).invalidateRewriteCache();
            Mockito.verify(second).invalidateRewriteCache();
        }
    }

    @Test
    void oneFailingViewDoesNotBlockTheOtherViews() {
        MTMVRelationManager manager = new MTMVRelationManager();
        BaseTableInfo source = name("lake", "source", "t");
        addDependency(manager, source, "dependent");
        addDependency(manager, source, "unrelated");
        MTMV failing = Mockito.mock(MTMV.class);
        MTMV healthy = Mockito.mock(MTMV.class);
        Mockito.doThrow(new IllegalStateException("cache failure")).when(failing).invalidateRewriteCache();
        try (MockedStatic<Env> env = localViews(failing, healthy)) {
            manager.invalidateRewriteCaches("lake", "source", "t");
            Mockito.verify(failing).invalidateRewriteCache();
            Mockito.verify(healthy).invalidateRewriteCache();
        }
    }

    private BaseTableInfo name(String catalog, String db, String table) {
        return new BaseTableInfo(new TableNameInfo(catalog, db, table));
    }

    private void addDependency(MTMVRelationManager manager, BaseTableInfo source, String view) {
        manager.refreshMTMVCache(new MTMVRelation(ImmutableSet.of(source), ImmutableSet.of(source),
                        ImmutableSet.of(source), ImmutableSet.of(), ImmutableSet.of()),
                name("internal", "views", view));
    }

    private MockedStatic<Env> localViews(MTMV dependent, MTMV unrelated) {
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        Mockito.when(catalog.getDbNullable("views")).thenReturn(database);
        Mockito.when(database.getTableNullable("dependent")).thenReturn(dependent);
        Mockito.when(database.getTableNullable("unrelated")).thenReturn(unrelated);
        MockedStatic<Env> env = Mockito.mockStatic(Env.class);
        env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        return env;
    }
}
