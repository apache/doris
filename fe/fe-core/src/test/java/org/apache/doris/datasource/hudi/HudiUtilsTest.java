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

package org.apache.doris.datasource.hudi;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class HudiUtilsTest {

    private MockedStatic<Env> envMockedStatic;

    @org.junit.After
    public void tearDown() {
        if (envMockedStatic != null) {
            envMockedStatic.close();
            envMockedStatic = null;
        }
    }

    @Test
    public void testGetHudiSchemaWithCleanCommit() throws IOException {

        /*
        example table:
            CREATE TABLE tbx (
                c1 INT)
            USING hudi
            TBLPROPERTIES (
            'hoodie.cleaner.policy'='KEEP_LATEST_COMMITS',
            'hoodie.clean.automatic' = 'true',
            'hoodie.cleaner.commits.retained' = '2'
            );
         */

        String commitContent1 = "{\n"
                + "  \"partitionToWriteStats\" : {\n"
                + "    \"\" : [ {\n"
                + "      \"fileId\" : \"91b75cdf-e851-4524-b579-a9b08edd61d8-0\",\n"
                + "      \"path\" : \"91b75cdf-e851-4524-b579-a9b08edd61d8-0_0-2164-2318_20241219214517936.parquet\",\n"
                + "      \"cdcStats\" : null,\n"
                + "      \"prevCommit\" : \"20241219214431757\",\n"
                + "      \"numWrites\" : 2,\n"
                + "      \"numDeletes\" : 0,\n"
                + "      \"numUpdateWrites\" : 0,\n"
                + "      \"numInserts\" : 1,\n"
                + "      \"totalWriteBytes\" : 434370,\n"
                + "      \"totalWriteErrors\" : 0,\n"
                + "      \"tempPath\" : null,\n"
                + "      \"partitionPath\" : \"\",\n"
                + "      \"totalLogRecords\" : 0,\n"
                + "      \"totalLogFilesCompacted\" : 0,\n"
                + "      \"totalLogSizeCompacted\" : 0,\n"
                + "      \"totalUpdatedRecordsCompacted\" : 0,\n"
                + "      \"totalLogBlocks\" : 0,\n"
                + "      \"totalCorruptLogBlock\" : 0,\n"
                + "      \"totalRollbackBlocks\" : 0,\n"
                + "      \"fileSizeInBytes\" : 434370,\n"
                + "      \"minEventTime\" : null,\n"
                + "      \"maxEventTime\" : null,\n"
                + "      \"runtimeStats\" : {\n"
                + "        \"totalScanTime\" : 0,\n"
                + "        \"totalUpsertTime\" : 87,\n"
                + "        \"totalCreateTime\" : 0\n"
                + "      }\n"
                + "    } ]\n"
                + "  },\n"
                + "  \"compacted\" : false,\n"
                + "  \"extraMetadata\" : {\n"
                + "    \"schema\" : \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"tbx_record\\\",\\\"namespace\\\":\\\"hoodie.tbx\\\",\\\"fields\\\":[{\\\"name\\\":\\\"c1\\\",\\\"type\\\":[\\\"null\\\",\\\"int\\\"],\\\"default\\\":null}]}\"\n"
                + "  },\n"
                + "  \"operationType\" : \"INSERT\"\n"
                + "}";

        String commitContent2 = "{\n"
                + "  \"partitionToWriteStats\" : {\n"
                + "    \"\" : [ {\n"
                + "      \"fileId\" : \"91b75cdf-e851-4524-b579-a9b08edd61d8-0\",\n"
                + "      \"path\" : \"91b75cdf-e851-4524-b579-a9b08edd61d8-0_0-2180-2334_20241219214518880.parquet\",\n"
                + "      \"cdcStats\" : null,\n"
                + "      \"prevCommit\" : \"20241219214517936\",\n"
                + "      \"numWrites\" : 3,\n"
                + "      \"numDeletes\" : 0,\n"
                + "      \"numUpdateWrites\" : 0,\n"
                + "      \"numInserts\" : 1,\n"
                + "      \"totalWriteBytes\" : 434397,\n"
                + "      \"totalWriteErrors\" : 0,\n"
                + "      \"tempPath\" : null,\n"
                + "      \"partitionPath\" : \"\",\n"
                + "      \"totalLogRecords\" : 0,\n"
                + "      \"totalLogFilesCompacted\" : 0,\n"
                + "      \"totalLogSizeCompacted\" : 0,\n"
                + "      \"totalUpdatedRecordsCompacted\" : 0,\n"
                + "      \"totalLogBlocks\" : 0,\n"
                + "      \"totalCorruptLogBlock\" : 0,\n"
                + "      \"totalRollbackBlocks\" : 0,\n"
                + "      \"fileSizeInBytes\" : 434397,\n"
                + "      \"minEventTime\" : null,\n"
                + "      \"maxEventTime\" : null,\n"
                + "      \"runtimeStats\" : {\n"
                + "        \"totalScanTime\" : 0,\n"
                + "        \"totalUpsertTime\" : 86,\n"
                + "        \"totalCreateTime\" : 0\n"
                + "      }\n"
                + "    } ]\n"
                + "  },\n"
                + "  \"compacted\" : false,\n"
                + "  \"extraMetadata\" : {\n"
                + "    \"schema\" : \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"tbx_record\\\",\\\"namespace\\\":\\\"hoodie.tbx\\\",\\\"fields\\\":[{\\\"name\\\":\\\"c1\\\",\\\"type\\\":[\\\"null\\\",\\\"int\\\"],\\\"default\\\":null}]}\"\n"
                + "  },\n"
                + "  \"operationType\" : \"INSERT\"\n"
                + "}";

        String propContent = "#Updated at 2024-12-19T13:44:32.166Z\n"
                + "#Thu Dec 19 21:44:32 CST 2024\n"
                + "hoodie.datasource.write.drop.partition.columns=false\n"
                + "hoodie.table.type=COPY_ON_WRITE\n"
                + "hoodie.archivelog.folder=archived\n"
                + "hoodie.timeline.layout.version=1\n"
                + "hoodie.table.version=6\n"
                + "hoodie.table.metadata.partitions=files\n"
                + "hoodie.database.name=mmc_hudi\n"
                + "hoodie.datasource.write.partitionpath.urlencode=false\n"
                + "hoodie.table.keygenerator.class=org.apache.hudi.keygen.NonpartitionedKeyGenerator\n"
                + "hoodie.table.name=tbx\n"
                + "hoodie.table.metadata.partitions.inflight=\n"
                + "hoodie.datasource.write.hive_style_partitioning=true\n"
                + "hoodie.table.checksum=1632286010\n"
                + "hoodie.table.create.schema={\"type\"\\:\"record\",\"name\"\\:\"tbx_record\",\"namespace\"\\:\"hoodie.tbx\",\"fields\"\\:[{\"name\"\\:\"c1\",\"type\"\\:[\"int\",\"null\"]}]}";


        // 1. prepare table path
        Path hudiTable = Files.createTempDirectory("hudiTable");
        File meta = new File(hudiTable + "/.hoodie");
        Assert.assertTrue(meta.mkdirs());

        // 2. generate properties and commit
        File prop = new File(meta + "/hoodie.properties");
        Files.write(prop.toPath(), propContent.getBytes());
        File commit1 = new File(meta + "/1.commit");
        Files.write(commit1.toPath(), commitContent1.getBytes());

        // 3. now, we can get the schema from this table.
        HMSExternalCatalog catalog = Mockito.spy(new HMSExternalCatalog(10001, "hudi_ut", null, Maps.newHashMap(), ""));
        Env env = mockCurrentEnvWithCatalog(catalog);
        Assert.assertNotNull(env);
        env.getExtMetaCacheMgr().prepareCatalogByEngine(catalog.getId(), HudiExternalMetaCache.ENGINE,
                catalog.getProperties());
        HMSExternalDatabase db = Mockito.spy(new HMSExternalDatabase(catalog, 1, "db", "db"));
        HMSExternalTable hmsExternalTable = Mockito.spy(new HMSExternalTable(2, "tb", "tb", catalog, db));
        Table remoteTable = new Table();
        StorageDescriptor storageDescriptor = new StorageDescriptor();
        storageDescriptor.setLocation("file://" + hudiTable.toAbsolutePath());
        remoteTable.setSd(storageDescriptor);
        Mockito.doReturn(remoteTable).when(hmsExternalTable).getRemoteTable();
        mockCatalogLookup(catalog, db, hmsExternalTable);
        HiveMetaStoreClientHelper.getHudiTableSchema(hmsExternalTable, new boolean[] {false}, "20241219214518880");

        // 4. delete the commit file,
        //    this operation is used to imitate the clean operation in hudi
        Assert.assertTrue(commit1.delete());

        // 5. generate a new commit
        File commit2 = new File(meta + "/2.commit");
        Files.write(commit2.toPath(), commitContent2.getBytes());

        // 6. we should get schema correctly
        //    because we will refresh timeline in this `getHudiTableSchema` method,
        //    and we can get the latest commit.
        //    so that this error: `Could not read commit details from file <table_path>/.hoodie/1.commit` will be not reported.
        HiveMetaStoreClientHelper.getHudiTableSchema(hmsExternalTable, new boolean[] {false}, "20241219214518880");

        // 7. clean up
        Assert.assertTrue(commit2.delete());
        Assert.assertTrue(prop.delete());
        Assert.assertTrue(meta.delete());
        Files.delete(hudiTable);
        env.getExtMetaCacheMgr().invalidateCatalogByEngine(catalog.getId(), HudiExternalMetaCache.ENGINE);
    }

    private Env mockCurrentEnvWithCatalog(HMSExternalCatalog catalog) {
        CatalogMgr catalogMgr = new TestingCatalogMgr(catalog);
        Env env = new TestingEnv(catalogMgr);
        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        return env;
    }

    private void mockCatalogLookup(HMSExternalCatalog catalog, HMSExternalDatabase db, HMSExternalTable table) {
        Mockito.doAnswer(invocation -> {
            String dbName = invocation.getArgument(0);
            return "db".equals(dbName) ? db : null;
        }).when(catalog).getDbNullable(Mockito.anyString());
        Mockito.doReturn(new Configuration()).when(catalog).getConfiguration();

        Mockito.doAnswer(invocation -> {
            String tableName = invocation.getArgument(0);
            return "tb".equals(tableName) ? table : null;
        }).when(db).getTableNullable(Mockito.anyString());
    }

    private static final class TestingCatalogMgr extends CatalogMgr {
        private final CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog;

        private TestingCatalogMgr(CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog) {
            this.catalog = catalog;
        }

        @Override
        public CatalogIf<? extends DatabaseIf<? extends TableIf>> getCatalog(long id) {
            return catalog.getId() == id ? catalog : null;
        }
    }

    private static final class TestingEnv extends Env {
        private final CatalogMgr catalogMgr;

        private TestingEnv(CatalogMgr catalogMgr) {
            super(true);
            this.catalogMgr = catalogMgr;
        }

        @Override
        public CatalogMgr getCatalogMgr() {
            return catalogMgr;
        }
    }
}
