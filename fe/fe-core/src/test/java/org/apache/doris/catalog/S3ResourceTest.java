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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.mysql.privilege.AccessControllerManager;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    private String s3Bucket;
    private Map<String, String> s3Properties;

    private Analyzer analyzer;

    @Before
    public void setUp() {
        name = "s3";
        type = "s3";
        s3Endpoint = "http://aaa";
        s3Region = "bj";
        s3RootPath = "/path/to/root";
        s3AccessKey = "xxx";
        s3SecretKey = "yyy";
        s3MaxConnections = "50";
        s3ReqTimeoutMs = "3000";
        s3ConnTimeoutMs = "1000";
        s3Bucket = "test-bucket";
        s3Properties = new HashMap<>();
        s3Properties.put("type", type);
        s3Properties.put("AWS_ENDPOINT", s3Endpoint);
        s3Properties.put("AWS_REGION", s3Region);
        s3Properties.put("AWS_ROOT_PATH", s3RootPath);
        s3Properties.put("AWS_ACCESS_KEY", s3AccessKey);
        s3Properties.put("AWS_SECRET_KEY", s3SecretKey);
        s3Properties.put("AWS_BUCKET", s3Bucket);
        s3Properties.put("s3_validity_check", "false");

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

        // resource with default settings
        CreateResourceStmt stmt = new CreateResourceStmt(true, false, name, s3Properties);
        stmt.analyze(analyzer);
        S3Resource s3Resource = (S3Resource) Resource.fromStmt(stmt);
        Assert.assertEquals(name, s3Resource.getName());
        Assert.assertEquals(type, s3Resource.getType().name().toLowerCase());
        Assert.assertEquals(s3Endpoint, s3Resource.getProperty(S3Properties.ENDPOINT));
        Assert.assertEquals(s3Region, s3Resource.getProperty(S3Properties.REGION));
        Assert.assertEquals(s3RootPath, s3Resource.getProperty(S3Properties.ROOT_PATH));
        Assert.assertEquals(s3AccessKey, s3Resource.getProperty(S3Properties.ACCESS_KEY));
        Assert.assertEquals(s3SecretKey, s3Resource.getProperty(S3Properties.SECRET_KEY));
        Assert.assertEquals(s3MaxConnections, s3Resource.getProperty(S3Properties.MAX_CONNECTIONS));
        Assert.assertEquals(s3ReqTimeoutMs, s3Resource.getProperty(S3Properties.REQUEST_TIMEOUT_MS));
        Assert.assertEquals(s3ConnTimeoutMs, s3Resource.getProperty(S3Properties.CONNECTION_TIMEOUT_MS));

        // with no default settings
        s3Properties.put(S3Properties.MAX_CONNECTIONS, "100");
        s3Properties.put(S3Properties.REQUEST_TIMEOUT_MS, "2000");
        s3Properties.put(S3Properties.CONNECTION_TIMEOUT_MS, "2000");
        s3Properties.put(S3Properties.VALIDITY_CHECK, "false");
        stmt = new CreateResourceStmt(true, false, name, s3Properties);
        stmt.analyze(analyzer);

        s3Resource = (S3Resource) Resource.fromStmt(stmt);
        Assert.assertEquals(name, s3Resource.getName());
        Assert.assertEquals(type, s3Resource.getType().name().toLowerCase());
        Assert.assertEquals(s3Endpoint, s3Resource.getProperty(S3Properties.ENDPOINT));
        Assert.assertEquals(s3Region, s3Resource.getProperty(S3Properties.REGION));
        Assert.assertEquals(s3RootPath, s3Resource.getProperty(S3Properties.ROOT_PATH));
        Assert.assertEquals(s3AccessKey, s3Resource.getProperty(S3Properties.ACCESS_KEY));
        Assert.assertEquals(s3SecretKey, s3Resource.getProperty(S3Properties.SECRET_KEY));
        Assert.assertEquals("100", s3Resource.getProperty(S3Properties.MAX_CONNECTIONS));
        Assert.assertEquals("2000", s3Resource.getProperty(S3Properties.REQUEST_TIMEOUT_MS));
        Assert.assertEquals("2000", s3Resource.getProperty(S3Properties.CONNECTION_TIMEOUT_MS));
    }

    @Test(expected = DdlException.class)
    public void testAbnormalResource(@Mocked Env env, @Injectable AccessControllerManager accessManager)
            throws UserException {
        new Expectations() {
            {
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };
        s3Properties.remove("AWS_ENDPOINT");
        CreateResourceStmt stmt = new CreateResourceStmt(true, false, name, s3Properties);
        stmt.analyze(analyzer);
        Resource.fromStmt(stmt);
    }

    @Test
    public void testSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. write
        // Path path = Files.createFile(Paths.get("./s3Resource"));
        Path path = Paths.get("./s3Resource");
        DataOutputStream s3Dos = new DataOutputStream(Files.newOutputStream(path));

        S3Resource s3Resource1 = new S3Resource("s3_1");
        s3Resource1.write(s3Dos);

        Map<String, String> properties = new HashMap<>();
        properties.put("AWS_ENDPOINT", "aaa");
        properties.put("AWS_REGION", "bbb");
        properties.put("AWS_ROOT_PATH", "/path/to/root");
        properties.put("AWS_ACCESS_KEY", "xxx");
        properties.put("AWS_SECRET_KEY", "yyy");
        properties.put("AWS_BUCKET", "test-bucket");
        properties.put("s3_validity_check", "false");
        S3Resource s3Resource2 = new S3Resource("s3_2");
        s3Resource2.setProperties(properties);
        s3Resource2.write(s3Dos);

        s3Dos.flush();
        s3Dos.close();

        // 2. read
        DataInputStream s3Dis = new DataInputStream(Files.newInputStream(path));
        S3Resource rS3Resource1 = (S3Resource) S3Resource.read(s3Dis);
        S3Resource rS3Resource2 = (S3Resource) S3Resource.read(s3Dis);

        Assert.assertEquals("s3_1", rS3Resource1.getName());
        Assert.assertEquals("s3_2", rS3Resource2.getName());

        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.ENDPOINT), "http://aaa");
        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.REGION), "bbb");
        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.ROOT_PATH), "/path/to/root");
        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.ACCESS_KEY), "xxx");
        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.SECRET_KEY), "yyy");
        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.MAX_CONNECTIONS), "50");
        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.REQUEST_TIMEOUT_MS), "3000");
        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.CONNECTION_TIMEOUT_MS), "1000");

        // 3. delete
        s3Dis.close();
        Files.deleteIfExists(path);
    }

    @Test
    public void testModifyProperties() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("AWS_ENDPOINT", "aaa");
        properties.put("AWS_REGION", "bbb");
        properties.put("AWS_ROOT_PATH", "/path/to/root");
        properties.put("AWS_ACCESS_KEY", "xxx");
        properties.put("AWS_SECRET_KEY", "yyy");
        properties.put("AWS_BUCKET", "test-bucket");
        properties.put("s3_validity_check", "false");
        S3Resource s3Resource = new S3Resource("t_source");
        s3Resource.setProperties(properties);
        FeConstants.runningUnitTest = true;

        Map<String, String> modify = new HashMap<>();
        modify.put("s3.access_key", "aaa");
        s3Resource.modifyProperties(modify);
    }

    @Test
    public void testHttpScheme() throws DdlException {
        // if https:// is set, it should be replaced with http://
        Map<String, String> properties = new HashMap<>();
        properties.put("AWS_ENDPOINT", "https://aaa");
        properties.put("AWS_REGION", "bbb");
        properties.put("AWS_ROOT_PATH", "/path/to/root");
        properties.put("AWS_ACCESS_KEY", "xxx");
        properties.put("AWS_SECRET_KEY", "yyy");
        properties.put("AWS_BUCKET", "test-bucket");
        properties.put("s3_validity_check", "false");
        S3Resource s3Resource = new S3Resource("s3_2");
        s3Resource.setProperties(properties);
        Assert.assertEquals(s3Resource.getProperty(S3Properties.ENDPOINT), "https://aaa");
    }
}
