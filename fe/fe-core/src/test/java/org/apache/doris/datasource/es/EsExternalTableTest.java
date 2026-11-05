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

package org.apache.doris.datasource.es;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.Type;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

class EsExternalTableTest {
    @Test
    void fieldLimitPreservesLegacyDefaults() {
        EsExternalCatalog catalog = new EsExternalCatalog(1, "es_catalog", null,
                ImmutableMap.of("hosts", "http://127.0.0.1:9200"), "");
        Assertions.assertEquals(EsTable.getDEFAULT_MAX_DOCVALUE_FIELDS(), catalog.getMaxDocValueFields());
        Map<String, Integer> limits = ImmutableMap.of("0", 0, "-1", 0, " 3 ", 3,
                "invalid", EsTable.getDEFAULT_MAX_DOCVALUE_FIELDS(),
                "2147483648", EsTable.getDEFAULT_MAX_DOCVALUE_FIELDS());
        limits.forEach((value, expected) -> {
            EsExternalCatalog configured = new EsExternalCatalog(1, "es_catalog", null,
                    ImmutableMap.of("hosts", "http://127.0.0.1:9200", "max_docvalue_fields", value), "");
            Assertions.assertEquals(expected.intValue(), configured.getMaxDocValueFields(), value);
        });
    }

    @Test
    void catalogFieldLimitReachesScanAdapter() throws Exception {
        EsExternalCatalog catalog = Mockito.spy(new EsExternalCatalog(1, "es_catalog", null,
                ImmutableMap.of("hosts", "http://127.0.0.1:9200", "max_docvalue_fields", "1"), ""));
        Mockito.doReturn(Mockito.mock(EsRestClient.class)).when(catalog).getEsRestClient();
        EsExternalTable table = new EsExternalTable(2, "es_index", "es_index", catalog,
                Mockito.mock(EsExternalDatabase.class)) {
            @Override
            public List<Column> getFullSchema() {
                return ImmutableList.of(new Column("a", Type.INT), new Column("b", Type.INT));
            }

            @Override
            public Map<String, String> getColumn2type() {
                return ImmutableMap.of("a", "integer", "b", "integer");
            }
        };
        try (MockedConstruction<EsMetaStateTracker> ignored = Mockito.mockConstruction(EsMetaStateTracker.class,
                (tracker, context) -> Mockito.when(tracker.searchContext())
                        .thenReturn(Mockito.mock(SearchContext.class)))) {
            Method adapter = EsExternalTable.class.getDeclaredMethod("toEsTable");
            adapter.setAccessible(true);
            EsTable adapted = (EsTable) adapter.invoke(table);
            Assertions.assertEquals(1, adapted.getMaxDocValueFields());
        }
    }
}
