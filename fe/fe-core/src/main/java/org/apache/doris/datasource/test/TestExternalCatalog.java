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

package org.apache.doris.datasource.test;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * This catalog is for unit test.
 * You can provide an implementation of TestCatalogProvider, which can mock metadata such as database/table/schema
 * You can refer to ColumnPrivTest for example.
 */
public class TestExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(TestExternalCatalog.class);

    private TestCatalogProvider catalogProvider;

    public TestExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, InitCatalogLog.Type.TEST, comment);
        this.catalogProperty = new CatalogProperty(resource, props);
        initCatalogProvider();
    }

    private void initCatalogProvider() {
        String providerClass = this.catalogProperty.getProperties().get("catalog_provider.class");
        Class<?> providerClazz = null;
        try {
            providerClazz = Class.forName(providerClass);
            this.catalogProvider = (TestCatalogProvider) providerClazz.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void initLocalObjectsImpl() {
    }

    protected List<String> listDatabaseNames() {
        return Lists.newArrayList(catalogProvider.getMetadata().keySet());
    }

    private List<String> mockedTableNames(String dbName) {
        if (!catalogProvider.getMetadata().containsKey(dbName)) {
            throw new RuntimeException("unknown database: " + dbName);
        }
        return Lists.newArrayList(catalogProvider.getMetadata().get(dbName).keySet());
    }

    public List<Column> mockedSchema(String dbName, String tblName) {
        if (!catalogProvider.getMetadata().containsKey(dbName)) {
            throw new RuntimeException("unknown db: " + dbName);
        }
        if (!catalogProvider.getMetadata().get(dbName).containsKey(tblName)) {
            throw new RuntimeException("unknown tbl: " + tblName);
        }
        return catalogProvider.getMetadata().get(dbName).get(tblName);
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        return mockedTableNames(dbName);
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        if (!catalogProvider.getMetadata().containsKey(dbName)) {
            return false;
        }
        if (!catalogProvider.getMetadata().get(dbName).containsKey(tblName)) {
            return false;
        }
        return true;
    }

    public interface TestCatalogProvider {
        // db name -> (tbl name -> schema)
        Map<String, Map<String, List<Column>>> getMetadata();
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        initCatalogProvider();
    }
}

