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
import org.apache.doris.thrift.TWgSlotMemoryPolicy;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class WorkloadGroupTest {

    @Test
    public void testCreateNormal() throws DdlException {
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(WorkloadGroup.MIN_CPU_PERCENT, "10");
        properties1.put(WorkloadGroup.MAX_CPU_PERCENT, "20");
        properties1.put(WorkloadGroup.MAX_MEMORY_PERCENT, "30%");
        properties1.put(WorkloadGroup.COMPUTE_GROUP, "default");
        String name1 = "g1";
        WorkloadGroup group1 = WorkloadGroup.create(name1, properties1);
        Assert.assertEquals(name1, group1.getName());
        Assert.assertTrue(group1.getProperties().containsKey(WorkloadGroup.MIN_CPU_PERCENT));
        Assert.assertTrue(group1.getMaxMemoryPercent() == 30);
    }

    @Test(expected = DdlException.class)
    public void testNotSupportProperty() throws DdlException {
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(WorkloadGroup.MIN_CPU_PERCENT, "10");
        properties1.put(WorkloadGroup.MAX_MEMORY_PERCENT, "30%");
        properties1.put("share", "10");
        String name1 = "g1";
        WorkloadGroup.create(name1, properties1);
    }

    @Test
    public void testGetProcNodeData() throws DdlException {
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(WorkloadGroup.MIN_CPU_PERCENT, "10");
        properties1.put(WorkloadGroup.MAX_CPU_PERCENT, "20");
        properties1.put(WorkloadGroup.MAX_MEMORY_PERCENT, "30%");
        properties1.put(WorkloadGroup.COMPUTE_GROUP, "default");
        String name1 = "g1";
        WorkloadGroup group1 = WorkloadGroup.create(name1, properties1);

        BaseProcResult result = new BaseProcResult();
        group1.getProcNodeData(result);
        List<List<String>> rows = result.getRows();
        Assert.assertEquals(1, rows.size());
        // TODO check proc data with system table
    }

    @Test
    public void testPolicyToString() {
        TWgSlotMemoryPolicy p1 = WorkloadGroup.findSlotPolicyValueByString("fixed");
        Assert.assertEquals(p1, TWgSlotMemoryPolicy.FIXED);
        TWgSlotMemoryPolicy p2 = WorkloadGroup.findSlotPolicyValueByString("dynamic");
        Assert.assertEquals(p2, TWgSlotMemoryPolicy.DYNAMIC);
        TWgSlotMemoryPolicy p3 = WorkloadGroup.findSlotPolicyValueByString("none");
        Assert.assertEquals(p3, TWgSlotMemoryPolicy.NONE);
        TWgSlotMemoryPolicy p4 = WorkloadGroup.findSlotPolicyValueByString("none");
        Assert.assertEquals(p4, TWgSlotMemoryPolicy.NONE);
        boolean hasException = false;
        try {
            WorkloadGroup.findSlotPolicyValueByString("disableDa");
        } catch (RuntimeException e) {
            hasException = true;
        }
        Assert.assertEquals(hasException, true);
    }

    @Test
    public void testWorkloadGroupKey() {
        // equal
        WorkloadGroupKey eqKey1 = WorkloadGroupKey.get("cg1", "wg1");
        WorkloadGroupKey eqKey2 = WorkloadGroupKey.get("cg1", "wg1");
        WorkloadGroupKey eqKey3 = WorkloadGroupKey.get("cg1", "wg2");
        Assert.assertTrue(eqKey1.equals(eqKey1));
        Assert.assertTrue(eqKey1.equals(eqKey2));
        Assert.assertTrue(eqKey2.equals(eqKey1));
        Assert.assertTrue(eqKey1.hashCode() == eqKey2.hashCode());

        Assert.assertFalse(eqKey3.equals(eqKey1));
        Assert.assertFalse(eqKey1.equals(eqKey3));
        Assert.assertTrue(eqKey1.hashCode() != eqKey3.hashCode());

        WorkloadGroupKey eqKey4 = WorkloadGroupKey.get("cg2", "wg2");
        Assert.assertFalse(eqKey4.equals(eqKey3));
        Assert.assertFalse(eqKey3.equals(eqKey4));
        Assert.assertFalse(eqKey4.hashCode() == eqKey3.hashCode());


        // test wg name exception
        try {
            WorkloadGroupKey.get("cg1", "");
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(true);
        }


        // test null equal
        Assert.assertTrue(!eqKey1.equals(null));

        WorkloadGroupKey nullkey2 = WorkloadGroupKey.get(null, "wg1");
        WorkloadGroupKey nullkey3 = WorkloadGroupKey.get("", "wg1");
        Assert.assertTrue(nullkey2.equals(nullkey3));
        Assert.assertTrue(nullkey3.equals(nullkey2));

        Assert.assertFalse(nullkey2.equals(eqKey1));
        Assert.assertFalse(eqKey1.equals(nullkey2));

    }
}
