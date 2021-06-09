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
import org.apache.doris.analysis.DropResourceStmt;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PaloAuth;
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

import java.util.Map;

public class ResourceMgrTest {
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
    public void testAddDropResource(@Injectable BrokerMgr brokerMgr, @Injectable EditLog editLog,
                                    @Mocked Catalog catalog, @Injectable PaloAuth auth) throws UserException {
        new Expectations() {
            {
                catalog.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.containsBroker(broker);
                result = true;
                catalog.getEditLog();
                result = editLog;
                catalog.getAuth();
                result = auth;
                auth.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        // add
        ResourceMgr mgr = new ResourceMgr();
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        stmt.analyze(analyzer);
        Assert.assertEquals(0, mgr.getResourceNum());
        mgr.createResource(stmt);
        Assert.assertEquals(1, mgr.getResourceNum());
        Assert.assertTrue(mgr.containsResource(name));
        SparkResource resource = (SparkResource) mgr.getResource(name);
        Assert.assertNotNull(resource);
        Assert.assertEquals(broker, resource.getBroker());

        // drop
        DropResourceStmt dropStmt = new DropResourceStmt(name);
        mgr.dropResource(dropStmt);
        Assert.assertEquals(0, mgr.getResourceNum());
    }

    @Test(expected = DdlException.class)
    public void testAddResourceExist(@Injectable BrokerMgr brokerMgr, @Mocked Catalog catalog, @Injectable PaloAuth auth)
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

        // add
        ResourceMgr mgr = new ResourceMgr();
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
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
        DropResourceStmt stmt = new DropResourceStmt(name);
        mgr.dropResource(stmt);
    }
}
