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

package org.apache.doris.nereids.trees.expressions.functions.table;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.jdbc.JdbcExternalCatalog;
import org.apache.doris.datasource.jdbc.client.JdbcClientException;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class QueryTest {
    @Test
    public void testQueryErrorMesage(@Injectable Auth auth, @Injectable CatalogMgr catalogMgr,
                                     @Injectable JdbcExternalCatalog catalog) {
        new MockUp<JdbcExternalCatalog>() {
            @Mock
            public List<Column> getColumnsFromQuery(String query) {
                throw new JdbcClientException("test error: %s", new SQLException("test jdbc error"), "test");
            }
        };
        new MockUp<Env>() {
            @Mock
            public Auth getAuth() {
                return auth;
            }

            @Mock
            CatalogMgr getCatalogMgr() {
                return catalogMgr;
            }
        };
        new Expectations() {
            {
                catalogMgr.getCatalog("jdbc");
                minTimes = 1;
                result = catalog;
            }
        };
        Map<String, String> map = Maps.newHashMap();
        map.put("catalog", "jdbc");
        map.put("query", "select 1");
        ConnectContext connectContext = new ConnectContext();
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.setThreadLocalInfo();
        try {
            Query query = new Query(new Properties(map));
            query.getTable().getColumns();
            Assertions.fail("jdbc query should throw exception");
        } catch (Exception e) {
            Assertions.assertEquals("Can not build FunctionGenTable 'query'. error: JdbcClientException: test error: test\n"
                    + "  Caused by: SQLException: test jdbc error", e.getMessage());
        }
        new MockUp<JdbcExternalCatalog>() {
            @Mock
            public List<Column> getColumnsFromQuery(String query) {
                throw new JdbcClientException("test error without cause");
            }
        };
        try {
            Query query = new Query(new Properties(map));
            query.getTable().getColumns();
            Assertions.fail("jdbc query should throw exception");
        } catch (Exception e) {
            Assertions.assertEquals("Can not build FunctionGenTable 'query'. error: JdbcClientException: test error without cause", e.getMessage());
        }
    }
}
