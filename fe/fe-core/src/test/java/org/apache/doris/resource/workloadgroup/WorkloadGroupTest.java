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

package org.apache.doris.resource.workloadgroup;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class WorkloadGroupTest {

    @Test
    public void testCreateNormal() throws DdlException {
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(WorkloadGroup.CPU_SHARE, "10");
        properties1.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        String name1 = "g1";
        WorkloadGroup group1 = WorkloadGroup.create(name1, properties1);
        Assert.assertEquals(name1, group1.getName());
        Assert.assertEquals(5, group1.getProperties().size());
        Assert.assertTrue(group1.getProperties().containsKey(WorkloadGroup.CPU_SHARE));
        Assert.assertTrue(Math.abs(group1.getMemoryLimitPercent() - 30) < 1e-6);
    }

    @Test(expected = DdlException.class)
    public void testNotSupportProperty() throws DdlException {
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(WorkloadGroup.CPU_SHARE, "10");
        properties1.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        properties1.put("share", "10");
        String name1 = "g1";
        WorkloadGroup.create(name1, properties1);
    }

    @Test(expected = DdlException.class)
    public void testRequiredProperty() throws DdlException {
        Map<String, String> properties1 = Maps.newHashMap();
        String name1 = "g1";
        WorkloadGroup.create(name1, properties1);
    }

    @Test
    public void testCpuShareValue() {
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(WorkloadGroup.CPU_SHARE, "0");
        properties1.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        String name1 = "g1";
        try {
            WorkloadGroup.create(name1, properties1);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("requires a positive integer."));
        }

        properties1.put(WorkloadGroup.CPU_SHARE, "cpu");
        try {
            WorkloadGroup.create(name1, properties1);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("requires a positive integer."));
        }
    }

    @Test
    public void testGetProcNodeData() throws DdlException {
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(WorkloadGroup.CPU_SHARE, "10");
        properties1.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        String name1 = "g1";
        WorkloadGroup group1 = WorkloadGroup.create(name1, properties1);

        BaseProcResult result = new BaseProcResult();
        group1.getProcNodeData(result);
        List<List<String>> rows = result.getRows();
        Assert.assertEquals(1, rows.size());
    }
}
