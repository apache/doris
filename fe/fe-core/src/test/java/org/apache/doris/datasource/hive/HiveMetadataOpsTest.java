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

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DbName;
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.Type;
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
import java.util.stream.Collectors;

/**
 * just overlay all metadata operations here.
 * @see HiveDDLAndDMLPlanTest Use it if you need to verify correctness.
 */
public class HiveMetadataOpsTest {

    private HiveMetadataOps metadataOps;

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
                return new HMSExternalDatabase(mockedCatalog, 0L, "mockedDb", "mockedDb");
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

    private void createDb(String dbName, Map<String, String> props) throws DdlException {
        CreateDbStmt createDbStmt = new CreateDbStmt(true, new DbName("hive", dbName), props);
        metadataOps.createDb(createDbStmt);
    }

    private void dropDb(String dbName, boolean forceDrop) throws DdlException {
        DropDbStmt dropDbStmt = new DropDbStmt(true, new DbName("hive", dbName), forceDrop);
        metadataOps.dropDb(dropDbStmt);
    }

    private void createTable(TableName tableName,
                             List<Column> cols,
                             List<String> parts,
                             List<String> buckets,
                             Map<String, String> props)
            throws UserException {
        PartitionDesc partitionDesc = new PartitionDesc(parts, null);
        DistributionDesc distributionDesc = null;
        if (!buckets.isEmpty()) {
            distributionDesc = new HashDistributionDesc(10, buckets);
        }
        List<String> colsName = cols.stream().map(Column::getName).collect(Collectors.toList());
        CreateTableStmt stmt = new CreateTableStmt(true, false,
                tableName,
                cols, null,
                "hive",
                new KeysDesc(KeysType.AGG_KEYS, colsName),
                partitionDesc,
                distributionDesc,
                props,
                props,
                "comment",
                null, null);
        metadataOps.createTable(stmt);
    }

    private void dropTable(TableName tableName, boolean forceDrop) throws DdlException {
        DropTableStmt dropTblStmt = new DropTableStmt(true, tableName, forceDrop);
        metadataOps.dropTable(dropTblStmt);
    }

    @Test
    public void testCreateAndDropAll() throws UserException {
        new MockUp<HMSExternalDatabase>(HMSExternalDatabase.class) {
            // create table if getTableNullable return null
            @Mock
            HMSExternalTable getTableNullable(String tableName) {
                return null;
            }
        };
        Map<String, String> dbProps = new HashMap<>();
        dbProps.put(HiveMetadataOps.LOCATION_URI_KEY, "file://loc/db");
        createDb("mockedDb", dbProps);
        Map<String, String> tblProps = new HashMap<>();
        tblProps.put(HiveMetadataOps.FILE_FORMAT_KEY, "orc");
        tblProps.put(HiveMetadataOps.LOCATION_URI_KEY, "file://loc/tbl");
        tblProps.put("fs.defaultFS", "hdfs://ha");
        TableName tableName = new TableName("mockedCtl", "mockedDb", "mockedTbl");
        List<Column> cols = new ArrayList<>();
        cols.add(new Column("id", Type.BIGINT));
        cols.add(new Column("pt", Type.STRING));
        cols.add(new Column("rate", Type.DOUBLE));
        cols.add(new Column("time", Type.DATETIME));
        List<String> parts = new ArrayList<>();
        parts.add("pt");
        List<String> bucks = new ArrayList<>();
        // bucks.add("id");
        createTable(tableName, cols, parts, bucks, tblProps);
        dropTable(tableName, true);
        dropDb("mockedDb", true);
    }
}
