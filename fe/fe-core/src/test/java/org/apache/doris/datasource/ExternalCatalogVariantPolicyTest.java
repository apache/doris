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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ExternalCatalogVariantPolicyTest {
    @Test
    void createRejectsExplicitEncodingBeforeInitializingExternalMetadata() throws Exception {
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class, Mockito.CALLS_REAL_METHODS);
        CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
        for (String key : new String[] {"variant_escape_dots_in_keys", "VARIANT_ESCAPE_DOTS_IN_KEYS"}) {
            for (String value : new String[] {"true", "false"}) {
                Mockito.when(info.getProperties()).thenReturn(ImmutableMap.of(key, value));
                AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                        () -> catalog.createTable(info));
                Assertions.assertTrue(error.getMessage().contains("requires an OLAP table"), error.getMessage());
            }
        }
        Mockito.verify(catalog, Mockito.never()).makeSureInitialized();
    }
}
