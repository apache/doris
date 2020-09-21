
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

package org.apache.doris.catalog;

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateResourceStmt;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class OdbcCatalogResourceTest {
    private String name;
    private String type;

    private String host;
    private String port;
    private String user;
    private String passwd;
    private Map<String, String> properties;
    private Analyzer analyzer;

    @Before
    public void setUp() {
        name = "odbc";
        type = "odbc_catalog";
        host = "127.0.0.1";
        port = "7777";
        user = "doris";
        passwd = "doris";
        properties = Maps.newHashMap();
        properties.put("type", type);
        properties.put("host", host);
        properties.put("port", port);
        properties.put("user", user);
        properties.put("password", passwd);
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
    }

    @Test
    public void testFromStmt(@Mocked Catalog catalog, @Injectable PaloAuth auth)
            throws UserException {
        new Expectations() {
            {
                catalog.getAuth();
                result = auth;
                auth.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        // host: 127.0.0.1, port: 7777, without driver and odbc_type
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        stmt.analyze(analyzer);
        OdbcCatalogResource resource = (OdbcCatalogResource) Resource.fromStmt(stmt);
        Assert.assertEquals(name, resource.getName());
        Assert.assertEquals(type, resource.getType().name().toLowerCase());
        Assert.assertEquals(host, resource.getProperties("host"));
        Assert.assertEquals(port, resource.getProperties("port"));
        Assert.assertEquals(user, resource.getProperties("user"));
        Assert.assertEquals(passwd, resource.getProperties("password"));

        // with driver and odbc_type
        properties.put("driver", "mysql");
        properties.put("odbc_type", "mysql");
        stmt = new CreateResourceStmt(true, name, properties);
        stmt.analyze(analyzer);
        resource = (OdbcCatalogResource) Resource.fromStmt(stmt);
        Assert.assertEquals("mysql", resource.getProperties("driver"));
        Assert.assertEquals("mysql", resource.getProperties("odbc_type"));

        // test getProcNodeData
        BaseProcResult result = new BaseProcResult();
        resource.getProcNodeData(result);
        Assert.assertEquals(7, result.getRows().size());
    }
}