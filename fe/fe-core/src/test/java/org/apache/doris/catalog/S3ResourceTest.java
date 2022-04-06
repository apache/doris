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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

public class S3ResourceTest {
    private String name;
    private String type;

    private String s3Endpoint;
    private String s3Region;
    private String s3RootPath;
    private String s3AccessKey;
    private String s3SecretKey;
    private String s3MaxConnections;
    private String s3ReqTimeoutMs;
    private String s3ConnTimeoutMs;
    private Map<String, String> s3Properties;

    private Analyzer analyzer;

    @Before
    public void setUp() {
        name = "s3";
        type = "s3";
        s3Endpoint = "aaa";
        s3Region = "bj";
        s3RootPath = "/path/to/root";
        s3AccessKey = "xxx";
        s3SecretKey = "yyy";
        s3MaxConnections = "50";
        s3ReqTimeoutMs = "3000";
        s3ConnTimeoutMs = "1000";
        s3Properties = new HashMap<>();
        s3Properties.put("type", type);
        s3Properties.put("s3_endpoint", s3Endpoint);
        s3Properties.put("s3_region", s3Region);
        s3Properties.put("s3_root_path", s3RootPath);
        s3Properties.put("s3_access_key", s3AccessKey);
        s3Properties.put("s3_secret_key", s3SecretKey);

        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
    }

    @Test
    public void testFromStmt(@Mocked Catalog catalog, @Injectable PaloAuth auth) throws UserException {
        new Expectations() {
            {
                catalog.getAuth();
                result = auth;
                auth.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        // resource with default settings
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, s3Properties);
        stmt.analyze(analyzer);
        S3Resource s3Resource = (S3Resource) Resource.fromStmt(stmt);
        Assert.assertEquals(name, s3Resource.getName());
        Assert.assertEquals(type, s3Resource.getType().name().toLowerCase());
        Assert.assertEquals(s3Endpoint, s3Resource.getProperty("s3_endpoint"));
        Assert.assertEquals(s3Region, s3Resource.getProperty("s3_region"));
        Assert.assertEquals(s3RootPath, s3Resource.getProperty("s3_root_path"));
        Assert.assertEquals(s3AccessKey, s3Resource.getProperty("s3_access_key"));
        Assert.assertEquals(s3SecretKey, s3Resource.getProperty("s3_secret_key"));
        Assert.assertEquals(s3MaxConnections, s3Resource.getProperty("s3_max_connections"));
        Assert.assertEquals(s3ReqTimeoutMs, s3Resource.getProperty("s3_request_timeout_ms"));
        Assert.assertEquals(s3ConnTimeoutMs, s3Resource.getProperty("s3_connection_timeout_ms"));

        // with no default settings
        s3Properties.put("s3_max_connections", "100");
        s3Properties.put("s3_request_timeout_ms", "2000");
        s3Properties.put("s3_connection_timeout_ms", "2000");
        stmt = new CreateResourceStmt(true, name, s3Properties);
        stmt.analyze(analyzer);

        s3Resource = (S3Resource) Resource.fromStmt(stmt);
        Assert.assertEquals(name, s3Resource.getName());
        Assert.assertEquals(type, s3Resource.getType().name().toLowerCase());
        Assert.assertEquals(s3Endpoint, s3Resource.getProperty("s3_endpoint"));
        Assert.assertEquals(s3Region, s3Resource.getProperty("s3_region"));
        Assert.assertEquals(s3RootPath, s3Resource.getProperty("s3_root_path"));
        Assert.assertEquals(s3AccessKey, s3Resource.getProperty("s3_access_key"));
        Assert.assertEquals(s3SecretKey, s3Resource.getProperty("s3_secret_key"));
        Assert.assertEquals("100", s3Resource.getProperty("s3_max_connections"));
        Assert.assertEquals("2000", s3Resource.getProperty("s3_request_timeout_ms"));
        Assert.assertEquals("2000", s3Resource.getProperty("s3_connection_timeout_ms"));
    }

    @Test (expected = DdlException.class)
    public void testAbnormalResource(@Mocked Catalog catalog, @Injectable PaloAuth auth) throws UserException {
        new Expectations() {
            {
                catalog.getAuth();
                result = auth;
                auth.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };
        s3Properties.remove("s3_root_path");
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, s3Properties);
        stmt.analyze(analyzer);
        Resource.fromStmt(stmt);
    }

    @Test
    public void testSerialization() throws Exception{
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. write
        File s3File = new File("./s3Resource");
        s3File.createNewFile();
        DataOutputStream s3Dos = new DataOutputStream(new FileOutputStream(s3File));

        S3Resource s3Resource1 = new S3Resource("s3_1");
        s3Resource1.write(s3Dos);

        Map<String, String> properties = new HashMap<>();
        properties.put("s3_endpoint", "aaa");
        properties.put("s3_region", "bbb");
        properties.put("s3_root_path", "/path/to/root");
        properties.put("s3_access_key", "xxx");
        properties.put("s3_secret_key", "yyy");
        S3Resource s3Resource2 = new S3Resource("s3_2");
        s3Resource2.setProperties(properties);
        s3Resource2.write(s3Dos);

        s3Dos.flush();
        s3Dos.close();

        // 2. read
        DataInputStream s3Dis = new DataInputStream(new FileInputStream(s3File));
        S3Resource rS3Resource1 = (S3Resource) S3Resource.read(s3Dis);
        S3Resource rS3Resource2 = (S3Resource) S3Resource.read(s3Dis);

        Assert.assertEquals("s3_1", rS3Resource1.getName());
        Assert.assertEquals("s3_2", rS3Resource2.getName());

        Assert.assertEquals(rS3Resource2.getProperty("s3_endpoint"), "aaa");
        Assert.assertEquals(rS3Resource2.getProperty("s3_region"), "bbb");
        Assert.assertEquals(rS3Resource2.getProperty("s3_root_path"), "/path/to/root");
        Assert.assertEquals(rS3Resource2.getProperty("s3_access_key"), "xxx");
        Assert.assertEquals(rS3Resource2.getProperty("s3_secret_key"), "yyy");
        Assert.assertEquals(rS3Resource2.getProperty("s3_max_connections"), "50");
        Assert.assertEquals(rS3Resource2.getProperty("s3_request_timeout_ms"), "3000");
        Assert.assertEquals(rS3Resource2.getProperty("s3_connection_timeout_ms"), "1000");

        // 3. delete
        s3Dis.close();
        s3File.delete();
    }
}
