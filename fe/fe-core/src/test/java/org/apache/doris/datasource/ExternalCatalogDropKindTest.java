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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.iceberg.IcebergMetadataOps;
import org.apache.doris.persist.EditLog;

import mockit.Mock;
import mockit.MockUp;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ExternalCatalogDropKindTest {
    @Test
    public void dropViewCannotDeleteV0Table() {
        for (boolean ifExists : new boolean[] {false, true}) {
            DropCatalog catalog = new DropCatalog(false);
            Assertions.assertThrows(DdlException.class,
                    () -> catalog.dropTable("db", "t", true, false, ifExists, false, false));
            catalog.assertTableUnchanged();
        }
    }

    @Test
    public void dropMaterializedViewCannotDeleteV0Table() {
        for (boolean ifExists : new boolean[] {false, true}) {
            DropCatalog catalog = new DropCatalog(false);
            Assertions.assertThrows(DdlException.class,
                    () -> catalog.dropTable("db", "t", false, true, ifExists, false, false));
            catalog.assertTableUnchanged();
        }
    }

    @Test
    public void dropTemporaryTableCannotDeleteV0Table() {
        DropCatalog catalog = new DropCatalog(false);
        Assertions.assertThrows(DdlException.class,
                () -> catalog.dropTable("db", "t", false, false, false, true, false));
        catalog.assertTableUnchanged();
    }

    @Test
    public void dropTemporaryTableIfExistsDoesNotDeleteV0Table() throws Exception {
        DropCatalog catalog = new DropCatalog(false);
        catalog.dropTable("db", "t", false, false, true, true, false);
        catalog.assertTableUnchanged();
    }

    @Test
    public void dropTableCannotDeleteView() {
        for (boolean ifExists : new boolean[] {false, true}) {
            DropCatalog catalog = new DropCatalog(true);
            Assertions.assertThrows(DdlException.class,
                    () -> catalog.dropTable("db", "t", false, false, ifExists, false, false));
            Assertions.assertEquals(0, catalog.sdk.viewDrops);
            Assertions.assertTrue(catalog.sdk.present);
            Mockito.verifyNoInteractions(catalog.editLog);
        }
    }

    @Test
    public void matchingV0TableDropStillWorks() throws Exception {
        DropCatalog catalog = new DropCatalog(false);
        catalog.dropTable("db", "t", false, false, false, false, false);
        Assertions.assertFalse(catalog.sdk.present);
        Assertions.assertEquals(1, catalog.sdk.tableDrops);
        Assertions.assertTrue(catalog.sdk.purge);
        Mockito.verify(catalog.editLog).logDropTable(Mockito.any());
    }

    @Test
    public void matchingViewDropStillWorks() throws Exception {
        DropCatalog catalog = new DropCatalog(true);
        catalog.dropTable("db", "t", true, false, false, false, false);
        Assertions.assertFalse(catalog.sdk.present);
        Assertions.assertEquals(1, catalog.sdk.viewDrops);
        Assertions.assertEquals(0, catalog.sdk.tableDrops);
        Mockito.verify(catalog.editLog).logDropTable(Mockito.any());
    }

    @Test
    public void missingObjectIfExistsStillDoesNotMutate() throws Exception {
        for (boolean view : new boolean[] {false, true}) {
            DropCatalog catalog = new DropCatalog(false);
            Mockito.when(catalog.database.getTableNullable("t")).thenReturn(null);
            catalog.dropTable("db", "t", view, false, true, false, false);
            catalog.assertTableUnchanged();
        }
    }

    private static class DropCatalog extends ExternalCatalog {
        private final RecordingJdbcCatalog sdk;
        private final ExternalDatabase<ExternalTable> database;
        private final EditLog editLog = Mockito.mock(EditLog.class);

        private DropCatalog(boolean view) {
            super(1L, "drop_kind_test", InitCatalogLog.Type.TEST, "");
            initialized = true;
            catalogProperty = new CatalogProperty(null, Collections.emptyMap());
            executionAuthenticator = new ExecutionAuthenticator() {
            };
            sdk = new RecordingJdbcCatalog(view);
            metadataOps = new IcebergMetadataOps(this, sdk) {
                @Override
                public void afterDropTable(String dbName, String tableName) {
                    // Cache retirement is outside this test's drop-kind and mutation boundary.
                }
            };
            database = Mockito.mock(ExternalDatabase.class);
            ExternalTable table = Mockito.mock(ExternalTable.class);
            Mockito.when(database.getTableNullable("t")).thenReturn(table);
            Mockito.when(database.getFullName()).thenReturn("db");
            Mockito.when(table.getName()).thenReturn("t");
            Mockito.when(table.getRemoteName()).thenReturn("t");
            Mockito.when(table.getDbName()).thenReturn("db");
            Mockito.when(table.getRemoteDbName()).thenReturn("db");
            Mockito.when(table.isView()).thenReturn(view);
            Env env = Mockito.mock(Env.class);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            new MockUp<Env>() {
                @Mock
                Env getCurrentEnv() {
                    return env;
                }
            };
        }

        private void assertTableUnchanged() {
            Assertions.assertTrue(sdk.present);
            Assertions.assertEquals(Arrays.asList(1, 2), sdk.rows);
            Assertions.assertEquals(0, sdk.tableDrops);
            Assertions.assertEquals(0, sdk.viewDrops);
            Mockito.verifyNoInteractions(editLog);
        }

        @Override
        protected void initLocalObjectsImpl() {
        }

        @Override
        public ExternalDatabase<ExternalTable> getDbNullable(String dbName) {
            return database;
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            return Collections.singletonList("t");
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            return sdk.present;
        }
    }

    // Retain the real SDK's V0 view guard while recording the destructive table operation it used to block.
    private static class RecordingJdbcCatalog extends JdbcCatalog {
        private final boolean view;
        private final List<Integer> rows = new ArrayList<>(Arrays.asList(1, 2));
        private boolean present = true;
        private boolean purge;
        private int tableDrops;
        private int viewDrops;

        private RecordingJdbcCatalog(boolean view) {
            this.view = view;
        }

        @Override
        public boolean viewExists(TableIdentifier identifier) {
            return view ? present : super.viewExists(identifier);
        }

        @Override
        public boolean tableExists(TableIdentifier identifier) {
            return present;
        }

        @Override
        public boolean dropTable(TableIdentifier identifier, boolean purge) {
            this.purge = purge;
            tableDrops++;
            present = false;
            rows.clear();
            return true;
        }

        @Override
        public boolean dropView(TableIdentifier identifier) {
            viewDrops++;
            present = false;
            return true;
        }
    }
}
