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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class CatalogMgrTest {

    @Test
    void testAlterCatalogRollsBackUncheckedValidationFailure() throws Exception {
        CatalogMgr catalogMgr = new CatalogMgr();
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        long catalogId = 42L;
        Mockito.when(catalog.getId()).thenReturn(catalogId);

        Field idToCatalogField = CatalogMgr.class.getDeclaredField("idToCatalog");
        idToCatalogField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<Long, CatalogIf<? extends DatabaseIf<? extends TableIf>>> idToCatalog =
                (ConcurrentMap<Long, CatalogIf<? extends DatabaseIf<? extends TableIf>>>)
                        idToCatalogField.get(catalogMgr);
        idToCatalog.put(catalogId, catalog);

        Map<String, String> oldProperties = ImmutableMap.of("read.batch-size", "1024");
        Map<String, String> newProperties = ImmutableMap.of("read.batch-size", "0");
        CatalogLog log = new CatalogLog();
        log.setCatalogId(catalogId);
        log.setNewProps(newProperties);
        Mockito.doThrow(new IllegalArgumentException("invalid reader option"))
                .when(catalog).checkProperties();

        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> catalogMgr.replayAlterCatalogProps(log, oldProperties, false));

        Assertions.assertTrue(exception.getMessage().contains("invalid reader option"));
        Mockito.verify(catalog).tryModifyCatalogProps(newProperties);
        Mockito.verify(catalog).rollBackCatalogProps(oldProperties);
        Mockito.verify(catalog, Mockito.never()).modifyCatalogProps(newProperties);
    }
}
