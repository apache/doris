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

import org.apache.doris.analysis.ModifyEtlClusterClause;
import org.apache.doris.analysis.ModifyEtlClusterClause.ModifyOp;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.doris.persist.EditLog;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class EtlClusterMgrTest {
    private String name;
    private String type;
    private String master;
    private String hdfsEtlPath;
    private String broker;
    private Map<String, String> properties;

    @Before
    public void setUp() {
        name = "cluster0";
        type = "spark";
        master = "spark://127.0.0.1:7077";
        hdfsEtlPath = "hdfs://127.0.0.1/tmp/doris";
        broker = "broker0";
        properties = Maps.newHashMap();
        properties.put("type", type);
        properties.put("master", master);
        properties.put("hdfs_etl_path", hdfsEtlPath);
        properties.put("broker", broker);
    }

    @Test
    public void testAddDropEtlCluster(@Injectable BrokerMgr brokerMgr, @Injectable EditLog editLog, @Mocked Catalog catalog)
            throws DdlException, AnalysisException {
        new Expectations() {
            {
                catalog.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.contaisnBroker(broker);
                result = true;
                catalog.getEditLog();
                result = editLog;
            }
        };

        // add
        EtlClusterMgr mgr = new EtlClusterMgr();
        ModifyEtlClusterClause clause = new ModifyEtlClusterClause(ModifyOp.OP_ADD, name, properties);
        clause.analyze(null);
        Assert.assertEquals(0, mgr.getEtlClusters().size());
        mgr.execute(clause);
        Assert.assertEquals(1, mgr.getEtlClusters().size());
        Assert.assertTrue(mgr.containsEtlCluster(name));
        SparkEtlCluster cluster = (SparkEtlCluster) mgr.getEtlCluster(name);
        Assert.assertNotNull(cluster);
        Assert.assertEquals(broker, cluster.getBroker());

        // drop
        clause = new ModifyEtlClusterClause(ModifyOp.OP_DROP, name);
        clause.analyze(null);
        mgr.execute(clause);
        Assert.assertEquals(0, mgr.getEtlClusters().size());
    }

    @Test(expected = DdlException.class)
    public void testAddEtlClusterExist(@Injectable BrokerMgr brokerMgr, @Mocked Catalog catalog)
            throws DdlException, AnalysisException {
        new Expectations() {
            {
                catalog.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.contaisnBroker(broker);
                result = true;
            }
        };

        // add
        EtlClusterMgr mgr = new EtlClusterMgr();
        ModifyEtlClusterClause clause = new ModifyEtlClusterClause(ModifyOp.OP_ADD, name, properties);
        clause.analyze(null);
        Assert.assertEquals(0, mgr.getEtlClusters().size());
        mgr.execute(clause);
        Assert.assertEquals(1, mgr.getEtlClusters().size());

        // add again
        mgr.execute(clause);
    }

    @Test(expected = DdlException.class)
    public void testDropEtlClusterNotExist() throws DdlException, AnalysisException {
        // drop
        EtlClusterMgr mgr = new EtlClusterMgr();
        Assert.assertEquals(0, mgr.getEtlClusters().size());
        ModifyEtlClusterClause clause = new ModifyEtlClusterClause(ModifyOp.OP_DROP, name);
        clause.analyze(null);
        mgr.execute(clause);
    }
}