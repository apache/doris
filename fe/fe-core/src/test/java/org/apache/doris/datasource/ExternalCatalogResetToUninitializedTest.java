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

import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.jdbc.JdbcExternalCatalog;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExternalCatalogResetToUninitializedTest {

    @Test
    public void testExternalCatalogRefreshRunsOutsideCatalogMonitor() {
        LockAwareExternalCatalog catalog = new LockAwareExternalCatalog();

        catalog.resetToUninitialized(true);

        Assert.assertTrue(catalog.refreshCalled);
        Assert.assertFalse(catalog.holdsCatalogMonitorInRefresh);
    }

    @Test
    public void testJdbcCatalogRefreshRunsOutsideCatalogMonitor() throws DdlException {
        LockAwareJdbcExternalCatalog catalog = new LockAwareJdbcExternalCatalog();

        catalog.resetToUninitialized(true);

        Assert.assertTrue(catalog.refreshCalled);
        Assert.assertFalse(catalog.holdsCatalogMonitorInRefresh);
    }

    private static class LockAwareExternalCatalog extends ExternalCatalog {
        private boolean refreshCalled;
        private boolean holdsCatalogMonitorInRefresh;

        LockAwareExternalCatalog() {
            super(1L, "lock_test_catalog", InitCatalogLog.Type.TEST, "");
            this.catalogProperty = new CatalogProperty("", new HashMap<>());
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            return false;
        }

        @Override
        protected void initLocalObjectsImpl() {
        }

        @Override
        protected List<String> listDatabaseNames() {
            return Collections.emptyList();
        }

        @Override
        public void onRefreshCache(boolean invalidCache) {
            refreshCalled = true;
            holdsCatalogMonitorInRefresh = Thread.holdsLock(this);
        }
    }

    private static class LockAwareJdbcExternalCatalog extends JdbcExternalCatalog {
        private boolean refreshCalled;
        private boolean holdsCatalogMonitorInRefresh;

        LockAwareJdbcExternalCatalog() throws DdlException {
            super(2L, "lock_test_jdbc_catalog", null, buildJdbcProperties(), "");
        }

        @Override
        public void onRefreshCache(boolean invalidCache) {
            refreshCalled = true;
            holdsCatalogMonitorInRefresh = Thread.holdsLock(this);
        }

        private static Map<String, String> buildJdbcProperties() {
            Map<String, String> properties = new HashMap<>();
            properties.put("type", "jdbc");
            properties.put(JdbcResource.DRIVER_URL, "ojdbc8.jar");
            properties.put(JdbcResource.JDBC_URL, "jdbc:oracle:thin:@127.0.0.1:1521:XE");
            properties.put(JdbcResource.DRIVER_CLASS, "oracle.jdbc.driver.OracleDriver");
            return properties;
        }
    }
}
