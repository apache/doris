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

package org.apache.doris.resource.resourcegroup;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ResourceGroupTest {

    @Test
    public void testCreateNormal() throws DdlException {
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(ResourceGroup.CPU_SHARE, "10");
        String name1 = "g1";
        ResourceGroup group1 = ResourceGroup.create(name1, properties1);
        Assert.assertEquals(name1, group1.getName());
        Assert.assertEquals(1, group1.getProperties().size());
        Assert.assertTrue(group1.getProperties().containsKey(ResourceGroup.CPU_SHARE));
    }

    @Test(expected = DdlException.class)
    public void testNotSupportProperty() throws DdlException {
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(ResourceGroup.CPU_SHARE, "10");
        properties1.put("share", "10");
        String name1 = "g1";
        ResourceGroup.create(name1, properties1);
    }

    @Test(expected = DdlException.class)
    public void testRequiredProperty() throws DdlException {
        Map<String, String> properties1 = Maps.newHashMap();
        String name1 = "g1";
        ResourceGroup.create(name1, properties1);
    }

    @Test
    public void testCpuShareValue() {
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(ResourceGroup.CPU_SHARE, "0");
        String name1 = "g1";
        try {
            ResourceGroup.create(name1, properties1);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains(ResourceGroup.CPU_SHARE + " requires a positive integer."));
        }

        properties1.put(ResourceGroup.CPU_SHARE, "cpu");
        try {
            ResourceGroup.create(name1, properties1);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains(ResourceGroup.CPU_SHARE + " requires a positive integer."));
        }
    }

    @Test
    public void testGetProcNodeData() throws DdlException {
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(ResourceGroup.CPU_SHARE, "10");
        String name1 = "g1";
        ResourceGroup group1 = ResourceGroup.create(name1, properties1);

        BaseProcResult result = new BaseProcResult();
        group1.getProcNodeData(result);
        List<List<String>> rows = result.getRows();
        Assert.assertEquals(1, rows.size());
        List<List<String>> expectedRows = Lists.newArrayList();
        expectedRows.add(Lists.newArrayList(String.valueOf(group1.getId()), name1, ResourceGroup.CPU_SHARE, "10"));
        for (int i = 0; i < expectedRows.size(); ++i) {
            for (int j = 0; j < expectedRows.get(i).size(); ++j) {
                Assert.assertEquals(expectedRows.get(i).get(j), rows.get(i).get(j));
            }
        }
    }
}
