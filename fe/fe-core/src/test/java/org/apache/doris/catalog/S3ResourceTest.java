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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Assume;
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
    private static final Logger LOG = LogManager.getLogger(S3ResourceTest.class);
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
        CreateResourceCommand createResourceCommand = new CreateResourceCommand(new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(s3Properties)));
        createResourceCommand.getInfo().validate();

        S3Resource s3Resource = (S3Resource) Resource.fromCommand(createResourceCommand);
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

        createResourceCommand = new CreateResourceCommand(new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(s3Properties)));
        createResourceCommand.getInfo().validate();

        s3Resource = (S3Resource) Resource.fromCommand(createResourceCommand);
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

        CreateResourceCommand createResourceCommand = new CreateResourceCommand(new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(s3Properties)));
        createResourceCommand.getInfo().validate();

        Resource.fromCommand(createResourceCommand);
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

        ImmutableMap<String, String> properties = ImmutableMap.of(
                "AWS_ENDPOINT", "aaa",
                "AWS_REGION", "bbb",
                "AWS_ROOT_PATH", "/path/to/root",
                "AWS_ACCESS_KEY", "xxx",
                "AWS_SECRET_KEY", "yyy",
                "AWS_BUCKET", "test-bucket",
                "s3_validity_check", "false"
        );
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
        ImmutableMap<String, String> properties = ImmutableMap.of(
                "AWS_ENDPOINT", "aaa",
                "AWS_REGION", "bbb",
                "AWS_ROOT_PATH", "/path/to/root",
                "AWS_ACCESS_KEY", "xxx",
                "AWS_SECRET_KEY", "yyy",
                "AWS_BUCKET", "test-bucket",
                "s3_validity_check", "false"
        );
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
        ImmutableMap<String, String> properties = ImmutableMap.of(
                "AWS_ENDPOINT", "https://aaa",
                "AWS_REGION", "bbb",
                "AWS_ROOT_PATH", "/path/to/root",
                "AWS_ACCESS_KEY", "xxx",
                "AWS_SECRET_KEY", "yyy",
                "AWS_BUCKET", "test-bucket",
                "s3_validity_check", "false"
        );
        S3Resource s3Resource = new S3Resource("s3_2");
        s3Resource.setProperties(properties);
        Assert.assertEquals(s3Resource.getProperty(S3Properties.ENDPOINT), "https://aaa");
    }

    @Test
    public void testPingS3() {
        try {
            String accessKey = System.getenv("ACCESS_KEY");
            String secretKey = System.getenv("SECRET_KEY");
            String bucket = System.getenv("BUCKET");
            String endpoint = System.getenv("ENDPOINT");
            String region = System.getenv("REGION");
            String provider = System.getenv("PROVIDER");

            Assume.assumeTrue("ACCESS_KEY isNullOrEmpty.", !Strings.isNullOrEmpty(accessKey));
            Assume.assumeTrue("SECRET_KEY isNullOrEmpty.", !Strings.isNullOrEmpty(secretKey));
            Assume.assumeTrue("BUCKET isNullOrEmpty.", !Strings.isNullOrEmpty(bucket));
            Assume.assumeTrue("ENDPOINT isNullOrEmpty.", !Strings.isNullOrEmpty(endpoint));
            Assume.assumeTrue("REGION isNullOrEmpty.", !Strings.isNullOrEmpty(region));
            Assume.assumeTrue("PROVIDER isNullOrEmpty.", !Strings.isNullOrEmpty(provider));

            Map<String, String> properties = new HashMap<>();
            properties.put("s3.endpoint", endpoint);
            properties.put("s3.region", region);
            properties.put("s3.access_key", accessKey);
            properties.put("s3.secret_key", secretKey);
            properties.put("provider", provider);
            S3Resource.pingS3(bucket, "fe_ut_prefix", properties);
        } catch (DdlException e) {
            LOG.info("testPingS3 exception:", e);
            Assert.assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testPingS3WithRoleArn() {
        try {
            String endpoint = System.getenv("ENDPOINT");
            String region = System.getenv("REGION");
            String provider = System.getenv("PROVIDER");

            String roleArn = System.getenv("ROLE_ARN");
            String externalId = System.getenv("EXTERNAL_ID");
            String bucket = System.getenv("BUCKET");

            Assume.assumeTrue("ENDPOINT isNullOrEmpty.", !Strings.isNullOrEmpty(endpoint));
            Assume.assumeTrue("REGION isNullOrEmpty.", !Strings.isNullOrEmpty(region));
            Assume.assumeTrue("PROVIDER isNullOrEmpty.", !Strings.isNullOrEmpty(provider));
            Assume.assumeTrue("ROLE_ARN isNullOrEmpty.", !Strings.isNullOrEmpty(roleArn));
            Assume.assumeTrue("EXTERNAL_ID isNullOrEmpty.", !Strings.isNullOrEmpty(externalId));
            Assume.assumeTrue("BUCKET isNullOrEmpty.", !Strings.isNullOrEmpty(bucket));

            Map<String, String> properties = new HashMap<>();
            properties.put("s3.endpoint", endpoint);
            properties.put("s3.region", region);
            properties.put("s3.role_arn", roleArn);
            properties.put("s3.external_id", externalId);
            properties.put("provider", provider);
            S3Resource.pingS3(bucket, "fe_ut_role_prefix", properties);
        } catch (DdlException e) {
            LOG.info("testPingS3WithRoleArn exception:", e);
            Assert.assertTrue(e.getMessage(), false);
        }
    }
}
