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
import org.apache.doris.analysis.AlterResourceStmt;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateResourceStmt;
import org.apache.doris.analysis.DropResourceStmt;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ResourceMgrTest {
    // spark resource
    private String master;
    private String sparkResName;
    private String sparkRestype;
    private String workingDir;
    private String broker;
    private Map<String, String> sparkProperties;
    // s3 resource
    private String s3ResName;
    private String s3ResType;
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
        sparkResName = "spark0";
        sparkRestype = "spark";
        master = "spark://127.0.0.1:7077";
        workingDir = "hdfs://127.0.0.1/tmp/doris";
        broker = "broker0";
        sparkProperties = Maps.newHashMap();
        sparkProperties.put("type", sparkRestype);
        sparkProperties.put("spark.master", master);
        sparkProperties.put("spark.submit.deployMode", "cluster");
        sparkProperties.put("working_dir", workingDir);
        sparkProperties.put("broker", broker);

        s3ResName = "s30";
        s3ResType = "s3";
        s3Endpoint = "aaa";
        s3Region = "bj";
        s3RootPath = "/path/to/root";
        s3AccessKey = "xxx";
        s3SecretKey = "yyy";
        s3MaxConnections = "50";
        s3ReqTimeoutMs = "3000";
        s3ConnTimeoutMs = "1000";
        s3Properties = new HashMap<>();
        s3Properties.put("type", s3ResType);
        s3Properties.put("AWS_ENDPOINT", s3Endpoint);
        s3Properties.put("AWS_REGION", s3Region);
        s3Properties.put("AWS_ROOT_PATH", s3RootPath);
        s3Properties.put("AWS_ACCESS_KEY", s3AccessKey);
        s3Properties.put("AWS_SECRET_KEY", s3SecretKey);
        s3Properties.put("AWS_BUCKET", "test-bucket");
        s3Properties.put("s3_validity_check", "false");
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
    }

    @Test
    public void testAddAlterDropResource(@Injectable BrokerMgr brokerMgr, @Injectable EditLog editLog,
            @Mocked Env env, @Injectable AccessControllerManager accessManager) throws UserException {
        new Expectations() {
            {
                env.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.containsBroker(broker);
                result = true;
                env.getEditLog();
                result = editLog;
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        // spark resource
        // add
        ResourceMgr mgr = new ResourceMgr();
        CreateResourceStmt stmt = new CreateResourceStmt(true, false, sparkResName, sparkProperties);
        stmt.analyze(analyzer);
        Assert.assertEquals(0, mgr.getResourceNum());
        mgr.createResource(stmt);
        Assert.assertEquals(1, mgr.getResourceNum());
        Assert.assertTrue(mgr.containsResource(sparkResName));
        SparkResource resource = (SparkResource) mgr.getResource(sparkResName);
        Assert.assertNotNull(resource);
        Assert.assertEquals(broker, resource.getBroker());

        // alter
        workingDir = "hdfs://127.0.0.1/tmp/doris_new";
        Map<String, String> copiedSparkProperties = Maps.newHashMap(sparkProperties);
        copiedSparkProperties.put("working_dir", workingDir);
        copiedSparkProperties.remove("spark.master");
        AlterResourceStmt alterResourceStmt = new AlterResourceStmt(sparkResName, copiedSparkProperties);
        mgr.alterResource(alterResourceStmt);
        Assert.assertEquals(workingDir, ((SparkResource) mgr.getResource(sparkResName)).getWorkingDir());

        // drop
        DropResourceStmt dropStmt = new DropResourceStmt(false, sparkResName);
        mgr.dropResource(dropStmt);
        Assert.assertEquals(0, mgr.getResourceNum());

        // s3 resource
        stmt = new CreateResourceStmt(true, false, s3ResName, s3Properties);
        stmt.analyze(analyzer);
        Assert.assertEquals(0, mgr.getResourceNum());
        mgr.createResource(stmt);
        Assert.assertEquals(1, mgr.getResourceNum());

        // alter
        s3Region = "sh";
        Map<String, String> copiedS3Properties = Maps.newHashMap(s3Properties);
        copiedS3Properties.put("AWS_REGION", s3Region);
        copiedS3Properties.remove("type");
        // current not support modify s3 property
        // mgr.alterResource(alterResourceStmt);
        // Assert.assertEquals(s3Region, ((S3Resource) mgr.getResource(s3ResName)).getProperty("AWS_REGION"));

        // drop
        dropStmt = new DropResourceStmt(false, s3ResName);
        mgr.dropResource(dropStmt);
        Assert.assertEquals(0, mgr.getResourceNum());

    }

    @Test(expected = DdlException.class)
    public void testAddResourceExist(@Injectable BrokerMgr brokerMgr, @Mocked Env env,
            @Injectable AccessControllerManager accessManager)
            throws UserException {
        new Expectations() {
            {
                env.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.containsBroker(broker);
                result = true;
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        // add
        ResourceMgr mgr = new ResourceMgr();
        CreateResourceStmt stmt = new CreateResourceStmt(true, false, sparkResName, sparkProperties);
        stmt.analyze(analyzer);
        Assert.assertEquals(0, mgr.getResourceNum());
        mgr.createResource(stmt);
        Assert.assertEquals(1, mgr.getResourceNum());

        // add again
        mgr.createResource(stmt);
    }

    @Test(expected = DdlException.class)
    public void testDropResourceNotExist() throws UserException {
        // drop
        ResourceMgr mgr = new ResourceMgr();
        Assert.assertEquals(0, mgr.getResourceNum());
        DropResourceStmt stmt = new DropResourceStmt(false, sparkResName);
        mgr.dropResource(stmt);
    }
}
