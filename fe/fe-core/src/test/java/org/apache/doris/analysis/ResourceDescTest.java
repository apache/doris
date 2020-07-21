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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.catalog.SparkResource;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.load.EtlJobType;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;

import java.util.Map;

public class ResourceDescTest {

    @Test
    public void testNormal(@Mocked Catalog catalog, @Injectable ResourceMgr resourceMgr)
            throws AnalysisException, DdlException {
        String resourceName = "spark0";
        Map<String, String> properties = Maps.newHashMap();
        String key = "spark.executor.memory";
        String value = "2g";
        properties.put(key, value);
        ResourceDesc resourceDesc = new ResourceDesc(resourceName, properties);
        SparkResource resource = new SparkResource(resourceName);

        new Expectations() {
            {
                catalog.getResourceMgr();
                result = resourceMgr;
                resourceMgr.getResource(resourceName);
                result = resource;
            }
        };

        resourceDesc.analyze();
        Assert.assertEquals(resourceName, resourceDesc.getName());
        Assert.assertEquals(value, resourceDesc.getProperties().get(key));
        Assert.assertEquals(EtlJobType.SPARK, resourceDesc.getEtlJobType());
    }

    @Test(expected = AnalysisException.class)
    public void testNoResource(@Mocked Catalog catalog, @Injectable ResourceMgr resourceMgr) throws AnalysisException {
        String resourceName = "spark1";
        ResourceDesc resourceDesc = new ResourceDesc(resourceName, null);

        new Expectations() {
            {
                catalog.getResourceMgr();
                result = resourceMgr;
                resourceMgr.getResource(resourceName);
                result = null;
            }
        };

        resourceDesc.analyze();
    }
}
