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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.load.EtlJobType;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class EtlClusterWithBrokerDescTest {

    @Test
    public void testNormal() throws AnalysisException {
        String clusterName = "spark.cluster0";
        String brokerName = "broker0";

        Map<String, String> properties = Maps.newHashMap();
        String key = "outputPath";
        String value = "/tmp/output";
        properties.put(key, value);

        EtlClusterWithBrokerDesc etlClusterWithBrokerDesc = new EtlClusterWithBrokerDesc(clusterName, brokerName,
                                                                                         properties);

        Assert.assertEquals(clusterName, etlClusterWithBrokerDesc.getClusterName());
        Assert.assertEquals(brokerName, etlClusterWithBrokerDesc.getBrokerName());
        Assert.assertEquals(value, etlClusterWithBrokerDesc.getProperties().get(key));
        Assert.assertEquals(EtlJobType.SPARK, etlClusterWithBrokerDesc.getEtlJobType());
    }

    @Test(expected = AnalysisException.class)
    public void testNoTable() throws AnalysisException {
        EtlClusterWithBrokerDesc etlClusterWithBrokerDesc = new EtlClusterWithBrokerDesc("test_cluster0",
                                                                                         "broker1", null);
        etlClusterWithBrokerDesc.analyze();
    }
}
