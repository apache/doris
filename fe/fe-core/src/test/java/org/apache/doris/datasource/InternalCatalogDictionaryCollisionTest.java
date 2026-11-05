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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.dictionary.DictionaryManager;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * A table and a dictionary share the table privilege namespace of the internal catalog,
 * so InternalCatalog.createTable() must reject a table whose name collides with a dictionary.
 */
public class InternalCatalogDictionaryCollisionTest {

    @Test
    public void testCreateTableRejectsDictionaryNameCollision() {
        InternalCatalog catalog = Mockito.spy(new InternalCatalog());
        Database db = Mockito.mock(Database.class);
        Mockito.when(catalog.getDbNullable("db1")).thenReturn(db);
        Mockito.when(db.isTableExist("foo")).thenReturn(false);

        CreateTableInfo createTableInfo = Mockito.mock(CreateTableInfo.class);
        Mockito.when(createTableInfo.getEngineName()).thenReturn(CreateTableInfo.ENGINE_OLAP);
        Mockito.when(createTableInfo.getDbName()).thenReturn("db1");
        Mockito.when(createTableInfo.getTableName()).thenReturn("foo");
        Mockito.when(createTableInfo.isTemp()).thenReturn(false);
        Mockito.when(createTableInfo.isExternal()).thenReturn(true);
        Mockito.when(createTableInfo.isIfNotExists()).thenReturn(false);

        Env env = Mockito.mock(Env.class);
        DictionaryManager dictionaryManager = Mockito.mock(DictionaryManager.class);
        Mockito.when(env.getDictionaryManager()).thenReturn(dictionaryManager);
        Mockito.when(dictionaryManager.hasDictionary("db1", "foo")).thenReturn(true);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            DdlException exception = Assertions.assertThrows(DdlException.class,
                    () -> catalog.createTable(createTableInfo));
            Assertions.assertTrue(exception.getMessage().contains(
                    "a dictionary with the same name exists"));
        }
    }
}
