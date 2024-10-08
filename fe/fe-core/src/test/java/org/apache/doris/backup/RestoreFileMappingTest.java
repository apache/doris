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

package org.apache.doris.backup;

import org.apache.doris.backup.RestoreFileMapping.IdChain;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

public class RestoreFileMappingTest {

    private RestoreFileMapping fileMapping = new RestoreFileMapping();
    private IdChain src;
    private IdChain dest;

    @Before
    public void setUp() {
        src = new IdChain(10005L, 10006L, 10005L, 10007L, 10008L, -1L);
        dest = new IdChain(10004L, 10003L, 10004L, 10007L, -1L, -1L);
        fileMapping.putMapping(src, dest, true);
    }

    @Test
    public void test() {
        IdChain key = new IdChain(10005L, 10006L, 10005L, 10007L, 10008L, -1L);
        Assert.assertEquals(key, src);
        Assert.assertEquals(src, key);
        IdChain val = fileMapping.get(key);
        Assert.assertNotNull(val);
        Assert.assertEquals(dest, val);

        Long l1 = new Long(10005L);
        Long l2 = new Long(10005L);
        Assert.assertFalse(l1 == l2);
        Assert.assertEquals(l1, l2);

        Long l3 = new Long(1L);
        Long l4 = new Long(1L);
        Assert.assertFalse(l3 == l4);
        Assert.assertEquals(l3, l4);
    }

}
