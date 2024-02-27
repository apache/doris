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

package org.apache.doris.external.elasticsearch;

import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EsResource;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.datasource.es.EsUtil;
import org.apache.doris.meta.MetaContext;

import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Test case for es.
 **/
public class EsTestCase {

    protected static FakeEditLog fakeEditLog;
    protected static FakeEnv fakeEnv;
    protected static Env masterEnv;

    /**
     * Init
     **/
    @BeforeClass
    public static void init() throws Exception {
        fakeEditLog = new FakeEditLog();
        fakeEnv = new FakeEnv();
        masterEnv = CatalogTestUtil.createTestCatalog();
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();
        FakeEnv.setEnv(masterEnv);
    }

    protected String loadJsonFromFile(String fileName) throws IOException, URISyntaxException {
        File file = new File(EsUtil.class.getClassLoader().getResource(fileName).toURI());
        InputStream is = new FileInputStream(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuilder jsonStr = new StringBuilder();
        String line = "";
        while ((line = br.readLine()) != null) {
            jsonStr.append(line);
        }
        br.close();
        is.close();
        return jsonStr.toString();
    }

    protected EsTable fakeEsTable(String table, String index, String type, List<Column> columns) throws DdlException {
        Map<String, String> props = new HashMap<>();
        props.put(EsResource.HOSTS, "127.0.0.1:8200");
        props.put(EsResource.INDEX, index);
        props.put(EsResource.TYPE, type);
        props.put(EsResource.VERSION, "6.5.3");
        return new EsTable(new Random().nextLong(), table, columns, props, null);

    }
}
