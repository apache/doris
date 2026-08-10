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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.datasource.test.TestExternalTable;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExternalDatabaseImageRestoreTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDefaultCtx().setThreadLocalInfo();
    }

    @Test
    public void testImageRestoreLazyLoadRebuildsTableOwnerReferences() {
        Map<String, String> props = Maps.newHashMap();
        props.put("catalog_provider.class", DatabaseCatalogProvider.class.getName());
        TestExternalCatalog catalog = new TestExternalCatalog(303L, "image_owner_test", "", props, "");
        catalog.setInitializedForTest(true);
        TestExternalDatabase db = new TestExternalDatabase(catalog, 304L, "db1", "db1");
        db.makeSureInitialized();
        catalog.addDatabaseForTest(db);
        db.addTableForTest(new TestExternalTable(305L, "tbl_base", "tbl_base", catalog, db));

        String json = GsonUtils.GSON.toJson(catalog, CatalogIf.class);
        Assertions.assertFalse(json.contains("tbl_base"));
        TestExternalCatalog restored =
                (TestExternalCatalog) GsonUtils.GSON.fromJson(json, CatalogIf.class);
        restored.setInitializedForTest(true);

        ExternalDatabase<? extends ExternalTable> restoredDb = restored.getDbNullable("db1");
        ExternalTable restoredTable = restoredDb.getTableNullable("tbl_base");

        Assertions.assertSame(restored, restoredTable.getCatalog());
        Assertions.assertSame(restoredDb, restoredTable.getDatabase());
        Assertions.assertDoesNotThrow(() -> new BaseTableInfo(restoredTable));
    }

    public static class DatabaseCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        private static final Map<String, Map<String, List<Column>>> MOCKED_META = new HashMap<>();

        static {
            Map<String, List<Column>> db1Tables = Maps.newHashMap();
            db1Tables.put("tbl_base", Lists.newArrayList(new Column("k1", PrimitiveType.INT)));
            MOCKED_META.put("db1", db1Tables);
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return MOCKED_META;
        }
    }
}
