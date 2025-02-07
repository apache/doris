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

package org.apache.doris.datasource.paimon;

import mockit.Mock;
import mockit.MockUp;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;


public class PaimonExternalCatalogTest {

    @Test
    public void testGetPaimonTable() {

        HashMap<String, String> props = new HashMap<>();
        props.put("warehouse", "not_exist");
        PaimonExternalCatalog catalog = new PaimonFileExternalCatalog(1, "name", "resource", props, "comment");
        catalog.setInitialized(true);

        new MockUp<Catalog>() {
            @Mock
            Table getTable(Identifier id) throws Catalog.TableNotExistException {
                throw new Catalog.TableNotExistException(id);
            }
        };

        try {
            catalog.getPaimonTable("dbName", "tblName");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Failed to get Paimon table"));
        }
    }
}
