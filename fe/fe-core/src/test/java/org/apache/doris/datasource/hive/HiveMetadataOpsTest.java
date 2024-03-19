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

package org.apache.doris.datasource.hive;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.DatabaseMetadata;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.TableMetadata;

import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveMetadataOpsTest {

    private HiveMetadataOps metadataOps;
    private String mockedDbName = "mockedDb";

    @Mocked
    private HMSCachedClient mockedClient;
    @Mocked
    private HMSExternalCatalog mockedCatalog;

    @BeforeEach
    public void init() {
        metadataOps = new HiveMetadataOps(mockedCatalog, mockedClient);
        new MockUp<HMSExternalCatalog>(HMSExternalCatalog.class) {
            @Mock
            public ExternalDatabase<? extends ExternalTable> getDbNullable(String dbName) {
                return new HMSExternalDatabase(mockedCatalog, 0L, mockedDbName);
            }

            @Mock
            public void onRefresh(boolean invalidCache) {
                // mocked
            }
        };
        new MockUp<HMSCachedClient>(HMSCachedClient.class) {
            @Mock
            public void createDatabase(DatabaseMetadata catalogDatabase) {
                // mocked
            }

            @Mock
            public void dropDatabase(String dbName) {
                // mocked
            }

            @Mock
            public void dropTable(String dbName, String tableName) {
                // mocked
            }

            @Mock
            public void createTable(TableMetadata catalogTable, boolean ignoreIfExists) {
                // mocked
            }
        };
    }

    @Test
    public void createDb() throws DdlException {
        Map<String, String> props = new HashMap<>();
        CreateDbStmt createDbStmt = new CreateDbStmt(true, mockedDbName, props);
        metadataOps.createDb(createDbStmt);
    }

    @Test
    public void createTable() throws UserException {
        TableName tableName = new TableName();
        tableName.setCtl("mockedCtl");
        tableName.setDb("mockedDb");
        tableName.setCtl("mockedTbl");
        List<ColumnDef> cols = new ArrayList<>();
        List<String> colsName = new ArrayList<>();
        Map<String, String> props = new HashMap<>();
        CreateTableStmt stmt = new CreateTableStmt(false, false, tableName, cols, "hive",
                new KeysDesc(KeysType.AGG_KEYS, colsName), null, null, props, null, "");
        metadataOps.createTable(stmt);
    }

    @Test
    public void dropTable() throws DdlException {
        TableName tableName = new TableName();
        tableName.setCtl("mockedCtl");
        tableName.setDb("mockedDb");
        tableName.setCtl("mockedTbl");
        DropTableStmt dropTblStmt = new DropTableStmt(true, tableName, true);
        metadataOps.dropTable(dropTblStmt);
    }

    @Test
    public void dropDb() throws DdlException {
        DropDbStmt dropDbStmt = new DropDbStmt(true, mockedDbName, true);
        metadataOps.dropDb(dropDbStmt);
    }
}
