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

import org.apache.doris.common.DdlException;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PartitionKeyDescTest {
    private List<PartitionValue> values;

    // value of key is ["1", "abc"]
    @Before
    public void setUp() {
        values = Lists.newArrayList(new PartitionValue("1"), new PartitionValue("abc"));
    }

    @Test
    public void testNormal() throws DdlException {
        PartitionKeyDesc desc = PartitionKeyDesc.createLessThan(values);

        Assert.assertEquals(values, desc.getUpperValues());
        Assert.assertEquals("('1', 'abc')", desc.toSql());
    }

    @Test
    public void testMax() {
        PartitionKeyDesc desc = PartitionKeyDesc.createMaxKeyDesc();

        Assert.assertNull(desc.getUpperValues());
        Assert.assertEquals("MAXVALUE", desc.toSql());
    }

    @Test
    public void testListMulti() {
        List<List<PartitionValue>> list = new ArrayList<>();
        list.add(values);
        list.add(Lists.newArrayList(new PartitionValue("2"), new PartitionValue("cde")));
        PartitionKeyDesc desc = PartitionKeyDesc.createIn(list);
        Assert.assertEquals(list, desc.getInValues());
        Assert.assertEquals("(('1', 'abc'),('2', 'cde'))", desc.toSql());
    }

    @Test
    public void testListSingle() {
        List<List<PartitionValue>> list = new ArrayList<>();
        list.add(Lists.newArrayList(new PartitionValue("1")));
        list.add(Lists.newArrayList(new PartitionValue("2")));
        PartitionKeyDesc desc = PartitionKeyDesc.createIn(list);
        Assert.assertEquals(list, desc.getInValues());
        Assert.assertEquals("('1','2')", desc.toSql());
    }
}
