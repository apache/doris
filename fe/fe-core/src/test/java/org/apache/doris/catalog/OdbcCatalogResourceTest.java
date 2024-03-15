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
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
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
    public void testFromStmt(@Mocked Env env, @Injectable AccessControllerManager accessManager)
            throws UserException {
        new Expectations() {
            {
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        // host: 127.0.0.1, port: 7777, without driver and odbc_type
        CreateResourceStmt stmt = new CreateResourceStmt(true, false, name, properties);
        Config.enable_odbc_mysql_broker_table = true;
        stmt.analyze(analyzer);
        OdbcCatalogResource resource = (OdbcCatalogResource) Resource.fromStmt(stmt);
        Assert.assertEquals(name, resource.getName());
        Assert.assertEquals(type, resource.getType().name().toLowerCase());
        Assert.assertEquals(host, resource.getProperty("host"));
        Assert.assertEquals(port, resource.getProperty("port"));
        Assert.assertEquals(user, resource.getProperty("user"));
        Assert.assertEquals(passwd, resource.getProperty("password"));

        // with driver and odbc_type
        properties.put("driver", "mysql");
        properties.put("odbc_type", "mysql");
        stmt = new CreateResourceStmt(true, false, name, properties);
        stmt.analyze(analyzer);
        resource = (OdbcCatalogResource) Resource.fromStmt(stmt);
        Assert.assertEquals("mysql", resource.getProperty("driver"));
        Assert.assertEquals("mysql", resource.getProperty("odbc_type"));

        // test getProcNodeData
        BaseProcResult result = new BaseProcResult();
        resource.getProcNodeData(result);
        Assert.assertEquals(7, result.getRows().size());
    }

    @Test
    public void testSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        Path path = Files.createFile(Paths.get("./odbcCatalogResource"));
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        OdbcCatalogResource odbcCatalogResource1 = new OdbcCatalogResource("odbc1");
        odbcCatalogResource1.write(dos);

        Map<String, String> configs = new HashMap<>();
        configs.put("host", "host");
        configs.put("port", "port");
        configs.put("user", "user");
        configs.put("password", "password");
        OdbcCatalogResource odbcCatalogResource2 = new OdbcCatalogResource("odbc2");
        odbcCatalogResource2.setProperties(configs);
        odbcCatalogResource2.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));

        OdbcCatalogResource rOdbcCatalogResource1 = (OdbcCatalogResource) OdbcCatalogResource.read(dis);
        OdbcCatalogResource rOdbcCatalogResource2 = (OdbcCatalogResource) OdbcCatalogResource.read(dis);

        Assert.assertEquals("odbc1", rOdbcCatalogResource1.getName());
        Assert.assertEquals("odbc2", rOdbcCatalogResource2.getName());

        Assert.assertEquals(rOdbcCatalogResource2.getProperty("host"), "host");
        Assert.assertEquals(rOdbcCatalogResource2.getProperty("port"), "port");
        Assert.assertEquals(rOdbcCatalogResource2.getProperty("user"), "user");
        Assert.assertEquals(rOdbcCatalogResource2.getProperty("password"), "password");

        // 3. delete files
        dis.close();
        Files.deleteIfExists(path);
    }
}
