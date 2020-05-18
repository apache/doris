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

package org.apache.doris.es;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Iterator;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.FakeCatalog;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.external.EsIndexState;
import org.apache.doris.external.EsRestClient;
import org.apache.doris.external.EsStateStore;
import org.apache.doris.external.EsTableState;
import org.apache.doris.external.ExternalDataSourceException;
import org.apache.doris.meta.MetaContext;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class EsStateStoreTest {

    private static FakeEditLog fakeEditLog;
    private static FakeCatalog fakeCatalog;
    private static Catalog masterCatalog;
    private static String mappingsStr = "";
    private static String searchShardsStr = "";
    private EsStateStore esStateStore;
    private EsRestClient fakeClient;
    
    @BeforeClass
    public static void init() throws IOException, InstantiationException, IllegalAccessException, 
        IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException,
        URISyntaxException {
        fakeEditLog = new FakeEditLog();
        fakeCatalog = new FakeCatalog();
        masterCatalog = CatalogTestUtil.createTestCatalog();
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_40);
        metaContext.setThreadLocalInfo();
        // masterCatalog.setJournalVersion(FeMetaVersion.VERSION_40);
        FakeCatalog.setCatalog(masterCatalog);
        mappingsStr = loadJsonFromFile("data/es/mappings.json");
        searchShardsStr = loadJsonFromFile("data/es/search_shards.json");
    }
    
    @Before
    public void setUp() {
        esStateStore = new EsStateStore();
        fakeClient = new EsRestClient(new String[]{"localhost:9200"}, null, null);
    }

    @Test
    public void testSetEsTableContext() {
        EsTable esTable = (EsTable) Catalog.getCurrentCatalog()
            .getDb(CatalogTestUtil.testDb1)
            .getTable(CatalogTestUtil.testEsTableId1);
        JSONObject properties = fakeClient.parseProperties(mappingsStr, esTable.getMappingType());
        esStateStore.setEsTableContext(properties, esTable);
        assertEquals("userId.keyword", esTable.fieldsContext().get("userId"));
        assertEquals("userId.keyword", esTable.docValueContext().get("userId"));
    }

    @Test
    public void testSetTableState() throws ExternalDataSourceException, DdlException {
        EsTable esTable = (EsTable) Catalog.getCurrentCatalog()
            .getDb(CatalogTestUtil.testDb1)
            .getTable(CatalogTestUtil.testEsTableId1);
        EsIndexState esIndexState = fakeClient
            .parseIndexState(esTable.getIndexName(), searchShardsStr);
        EsTableState esTableState = esStateStore.setTableStatePartitionInfo(esTable, esIndexState);
        assertNotNull(esTableState);
        assertEquals(1, esTableState.getUnPartitionedIndexStates().size());
        assertEquals(5, esTableState.getIndexState("indexa").getShardRoutings().size());
    }
    
    private static String loadJsonFromFile(String fileName) throws IOException, URISyntaxException {
        File file = new File(EsStateStoreTest.class.getClassLoader().getResource(fileName).toURI());
        InputStream is = new FileInputStream(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));  
        StringBuilder jsonStr = new StringBuilder();
        String line = "";
        while ((line = br.readLine()) != null)  {
            jsonStr.append(line);
        }
        br.close();
        is.close();
        return jsonStr.toString();
    }
}
