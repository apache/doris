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

//import org.apache.doris.analysis.EtlClusterDesc;
import org.apache.doris.analysis.ModifyEtlClusterClause;
import org.apache.doris.analysis.ModifyEtlClusterClause.ModifyOp;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.doris.common.proc.BaseProcResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class SparkEtlClusterTest {
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
    public void testFromClause(@Injectable BrokerMgr brokerMgr, @Mocked Catalog catalog)
            throws DdlException, AnalysisException {
        new Expectations() {
            {
                catalog.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.contaisnBroker(broker);
                result = true;
            }
        };

        // master: spark, deploy_mode: cluster
        ModifyEtlClusterClause clause = new ModifyEtlClusterClause(ModifyOp.OP_ADD, name, properties);
        clause.analyze(null);
        SparkEtlCluster cluster = (SparkEtlCluster) EtlCluster.fromClause(clause);
        Assert.assertEquals(name, cluster.getName());
        Assert.assertEquals(type, cluster.getType().name().toLowerCase());
        Assert.assertEquals(master, cluster.getMaster());
        Assert.assertEquals("cluster", cluster.getDeployMode().name().toLowerCase());
        Assert.assertEquals(hdfsEtlPath, cluster.getHdfsEtlPath());
        Assert.assertEquals(broker, cluster.getBroker());
        Assert.assertTrue(cluster.getSparkArgsMap().isEmpty());
        Assert.assertTrue(cluster.getSparkConfigsMap().isEmpty());
        Assert.assertTrue(cluster.getYarnConfigsMap().isEmpty());
        Assert.assertFalse(cluster.isYarnMaster());

        // master: spark, deploy_mode: client
        properties.put("deploy_mode", "client");
        clause = new ModifyEtlClusterClause(ModifyOp.OP_ADD, name, properties);
        clause.analyze(null);
        cluster = (SparkEtlCluster) EtlCluster.fromClause(clause);
        Assert.assertEquals("client", cluster.getDeployMode().name().toLowerCase());

        // master: yarn, deploy_mode cluster
        properties.put("master", "yarn");
        properties.put("deploy_mode", "cluster");
        properties.put("spark_args", "--jars=xxx.jar,yyy.jar;--files=/tmp/aaa,/tmp/bbb");
        properties.put("spark_configs", "spark.driver.memory=1g");
        properties.put("yarn_configs",
                       "yarn.resourcemanager.address=127.0.0.1:9999;fs.defaultFS=hdfs://127.0.0.1:10000");
        clause = new ModifyEtlClusterClause(ModifyOp.OP_ADD, name, properties);
        clause.analyze(null);
        cluster = (SparkEtlCluster) EtlCluster.fromClause(clause);
        Assert.assertTrue(cluster.isYarnMaster());
        Map<String, String> map = cluster.getSparkArgsMap();
        Assert.assertEquals(2, map.size());
        Assert.assertEquals("xxx.jar,yyy.jar", map.get("--jars"));
        Assert.assertEquals("/tmp/aaa,/tmp/bbb", map.get("--files"));
        map = cluster.getSparkConfigsMap();
        Assert.assertEquals(1, map.size());
        Assert.assertEquals("1g", map.get("spark.driver.memory"));
        map = cluster.getYarnConfigsMap();
        Assert.assertEquals(2, map.size());
        Assert.assertEquals("127.0.0.1:9999", map.get("yarn.resourcemanager.address"));
        Assert.assertEquals("hdfs://127.0.0.1:10000", map.get("fs.defaultFS"));
        // test getProcNodeData
        BaseProcResult result = new BaseProcResult();
        cluster.getProcNodeData(result);
        Assert.assertEquals(7, result.getRows().size());
    }

    /*
    @Test
    public void testUpdate(@Injectable BrokerMgr brokerMgr, @Mocked Catalog catalog)
            throws DdlException, AnalysisException {
        new Expectations() {
            {
                catalog.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.contaisnBroker(broker);
                result = true;
            }
        };

        properties.put("master", "yarn");
        properties.put("deploy_mode", "cluster");
        properties.put("spark_args", "--jars=xxx.jar,yyy.jar;--files=/tmp/aaa,/tmp/bbb");
        properties.put("spark_configs", "spark.driver.memory=1g");
        properties.put("yarn_configs",
                       "yarn.resourcemanager.address=127.0.0.1:9999;fs.defaultFS=hdfs://127.0.0.1:10000");
        ModifyEtlClusterClause clause = new ModifyEtlClusterClause(ModifyOp.OP_ADD, name, properties);
        clause.analyze(null);
        SparkEtlCluster cluster = (SparkEtlCluster) EtlCluster.fromClause(clause);
        SparkEtlCluster copiedCluster = cluster.getCopiedEtlCluster();
        Map<String, String> newProperties = Maps.newHashMap();
        newProperties.put("spark_args", "--queue=queue1");
        newProperties.put("spark_configs", "spark.driver.memory=2g");
        EtlClusterDesc etlClusterDesc = new EtlClusterDesc(name, newProperties);
        copiedCluster.update(etlClusterDesc);
        Map<String, String> map = copiedCluster.getSparkArgsMap();
        Assert.assertEquals(2, cluster.getSparkArgsMap().size());
        Assert.assertEquals(3, map.size());
        Assert.assertEquals("queue1", map.get("--queue"));
        Assert.assertEquals("2g", copiedCluster.getSparkConfigsMap().get("spark.driver.memory"));
    }
    */

    @Test(expected = DdlException.class)
    public void testNoBroker(@Injectable BrokerMgr brokerMgr, @Mocked Catalog catalog)
            throws DdlException, AnalysisException {
        new Expectations() {
            {
                catalog.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.contaisnBroker(broker);
                result = false;
            }
        };

        ModifyEtlClusterClause clause = new ModifyEtlClusterClause(ModifyOp.OP_ADD, name, properties);
        clause.analyze(null);
        EtlCluster.fromClause(clause);
    }

    @Test(expected = DdlException.class)
    public void testArgsFormatError(@Injectable BrokerMgr brokerMgr, @Mocked Catalog catalog)
            throws DdlException, AnalysisException {
        new Expectations() {
            {
                catalog.getBrokerMgr();
                result = brokerMgr;
                brokerMgr.contaisnBroker(broker);
                result = true;
            }
        };

        properties.put("spark_args", "jars=xxx.jar,yyy.jar");
        ModifyEtlClusterClause clause = new ModifyEtlClusterClause(ModifyOp.OP_ADD, name, properties);
        clause.analyze(null);
        EtlCluster.fromClause(clause);
    }
}
