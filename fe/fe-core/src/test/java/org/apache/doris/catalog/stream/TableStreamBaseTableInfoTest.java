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

package org.apache.doris.catalog.stream;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TableStreamBaseTableInfoTest {

    @Test
    public void testConstructionAndSerialization() {
        TableIf table = Mockito.mock(TableIf.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getId()).thenReturn(3L);
        Mockito.when(database.getId()).thenReturn(2L);
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(table.getName()).thenReturn("table_name");
        Mockito.when(database.getFullName()).thenReturn("db_name");
        Mockito.when(catalog.getName()).thenReturn("catalog_name");

        TableStreamBaseTableInfo baseTableInfo = new TableStreamBaseTableInfo(table);
        Assertions.assertEquals(3L, baseTableInfo.getTableId());
        Assertions.assertEquals(2L, baseTableInfo.getDbId());
        Assertions.assertEquals(1L, baseTableInfo.getCtlId());
        Assertions.assertEquals("table_name", baseTableInfo.getTableName());
        Assertions.assertEquals("db_name", baseTableInfo.getDbName());
        Assertions.assertEquals("catalog_name", baseTableInfo.getCtlName());
        Assertions.assertEquals(ImmutableList.of("catalog_name", "db_name", "table_name"),
                baseTableInfo.getFullQualifiers());

        OlapTableStream tableStream = new OlapTableStream();
        tableStream.baseTableInfo = baseTableInfo;
        JsonObject json = JsonParser.parseString(GsonUtils.GSON.toJson(tableStream)).getAsJsonObject();
        Assertions.assertTrue(json.has("bti"));
        Assertions.assertFalse(json.has("tsi"));

        OlapTableStream deserialized = GsonUtils.GSON.fromJson(json, OlapTableStream.class);
        Assertions.assertEquals(baseTableInfo, deserialized.baseTableInfo);
    }
}
