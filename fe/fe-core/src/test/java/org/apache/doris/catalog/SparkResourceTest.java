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
import org.apache.doris.analysis.ResourceDesc;
import org.apache.doris.common.DdlException;
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

public class SparkResourceTest {
    private String name;
    private String type;
    private String master;
    private String workingDir;
    private String broker;
    private Map<String, String> properties;
    private Analyzer analyzer;

    @Before
    public void setUp() {
        name = "spark0";
        type = "spark";
        master = "spark://127.0.0.1:7077";
        workingDir = "hdfs://127.0.0.1/tmp/doris";
        broker = "broker0";
        properties = Maps.newHashMap();
        properties.put("type", type);
        properties.put("spark.master", master);
        properties.put("spark.submit.deployMode", "cluster");
        properties.put("working_dir", workingDir);
        properties.put("broker", broker);
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
    }

    @Test
    public void testFromStmt(@Injectable BrokerMgr brokerMgr, @Mocked Catalog catalog, @Injectable PaloAuth auth)
            throws UserException {
        new Expectations() {
            {
                catalog.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.containsBroker(broker);
                result = true;
                catalog.getAuth();
                result = auth;
                auth.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        // master: spark, deploy_mode: cluster
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        stmt.analyze(analyzer);
        SparkResource resource = (SparkResource) Resource.fromStmt(stmt);
        Assert.assertEquals(name, resource.getName());
        Assert.assertEquals(type, resource.getType().name().toLowerCase());
        Assert.assertEquals(master, resource.getMaster());
        Assert.assertEquals("cluster", resource.getDeployMode().name().toLowerCase());
        Assert.assertEquals(workingDir, resource.getWorkingDir());
        Assert.assertEquals(broker, resource.getBroker());
        Assert.assertEquals(2, resource.getSparkConfigs().size());
        Assert.assertFalse(resource.isYarnMaster());

        // master: spark, deploy_mode: client
        properties.put("spark.submit.deployMode", "client");
        stmt = new CreateResourceStmt(true, name, properties);
        stmt.analyze(analyzer);
        resource = (SparkResource) Resource.fromStmt(stmt);
        Assert.assertEquals("client", resource.getDeployMode().name().toLowerCase());

        // master: yarn, deploy_mode cluster
        properties.put("spark.master", "yarn");
        properties.put("spark.submit.deployMode", "cluster");
        properties.put("spark.jars", "xxx.jar,yyy.jar");
        properties.put("spark.files", "/tmp/aaa,/tmp/bbb");
        properties.put("spark.driver.memory", "1g");
        properties.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        properties.put("spark.hadoop.fs.defaultFS", "hdfs://127.0.0.1:10000");
        stmt = new CreateResourceStmt(true, name, properties);
        stmt.analyze(analyzer);
        resource = (SparkResource) Resource.fromStmt(stmt);
        Assert.assertTrue(resource.isYarnMaster());
        Map<String, String> map = resource.getSparkConfigs();
        Assert.assertEquals(7, map.size());
        // test getProcNodeData
        BaseProcResult result = new BaseProcResult();
        resource.getProcNodeData(result);
        Assert.assertEquals(9, result.getRows().size());
    }

    @Test
    public void testUpdate(@Injectable BrokerMgr brokerMgr, @Mocked Catalog catalog, @Injectable PaloAuth auth)
            throws UserException {
        new Expectations() {
            {
                catalog.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.containsBroker(broker);
                result = true;
                catalog.getAuth();
                result = auth;
                auth.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        properties.put("spark.master", "yarn");
        properties.put("spark.submit.deployMode", "cluster");
        properties.put("spark.driver.memory", "1g");
        properties.put("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:9999");
        properties.put("spark.hadoop.fs.defaultFS", "hdfs://127.0.0.1:10000");
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        stmt.analyze(analyzer);
        SparkResource resource = (SparkResource) Resource.fromStmt(stmt);
        SparkResource copiedResource = resource.getCopiedResource();
        Map<String, String> newProperties = Maps.newHashMap();
        newProperties.put("spark.executor.memory", "1g");
        newProperties.put("spark.driver.memory", "2g");
        ResourceDesc resourceDesc = new ResourceDesc(name, newProperties);
        copiedResource.update(resourceDesc);
        Map<String, String> map = copiedResource.getSparkConfigs();
        Assert.assertEquals(5, resource.getSparkConfigs().size());
        Assert.assertEquals("1g", resource.getSparkConfigs().get("spark.driver.memory"));
        Assert.assertEquals(6, map.size());
        Assert.assertEquals("2g", copiedResource.getSparkConfigs().get("spark.driver.memory"));
    }

    @Test(expected = DdlException.class)
    public void testNoBroker(@Injectable BrokerMgr brokerMgr, @Mocked Catalog catalog, @Injectable PaloAuth auth)
            throws UserException {
        new Expectations() {
            {
                catalog.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.containsBroker(broker);
                result = false;
                catalog.getAuth();
                result = auth;
                auth.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        stmt.analyze(analyzer);
        Resource.fromStmt(stmt);
    }
}
