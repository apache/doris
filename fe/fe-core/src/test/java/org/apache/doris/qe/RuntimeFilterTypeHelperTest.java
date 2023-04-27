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

package org.apache.doris.qe;

import org.apache.doris.common.DdlException;

import org.junit.Assert;
import org.junit.Test;

public class RuntimeFilterTypeHelperTest {

    @Test
    public void testNormal() throws DdlException {
        String runtimeFilterType = "";
        Assert.assertEquals(new Long(0L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "IN";
        Assert.assertEquals(new Long(1L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "BLOOM_FILTER";
        Assert.assertEquals(new Long(2L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "IN,BLOOM_FILTER";
        Assert.assertEquals(new Long(3L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "MIN_MAX";
        Assert.assertEquals(new Long(4L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "IN,MIN_MAX";
        Assert.assertEquals(new Long(5L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "MIN_MAX, BLOOM_FILTER";
        Assert.assertEquals(new Long(6L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "IN,BLOOM_FILTER,MIN_MAX";
        Assert.assertEquals(new Long(7L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "IN_OR_BLOOM_FILTER";
        Assert.assertEquals(new Long(8L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "IN,IN_OR_BLOOM_FILTER";
        Assert.assertEquals(new Long(9L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "BLOOM_FILTER,IN_OR_BLOOM_FILTER";
        Assert.assertEquals(new Long(10L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "IN,BLOOM_FILTER,IN_OR_BLOOM_FILTER";
        Assert.assertEquals(new Long(11L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "MIN_MAX,IN_OR_BLOOM_FILTER";
        Assert.assertEquals(new Long(12L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "IN,MIN_MAX,IN_OR_BLOOM_FILTER";
        Assert.assertEquals(new Long(13L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "BLOOM_FILTER,MIN_MAX,IN_OR_BLOOM_FILTER";
        Assert.assertEquals(new Long(14L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "IN,BLOOM_FILTER,MIN_MAX,IN_OR_BLOOM_FILTER";
        Assert.assertEquals(new Long(15L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        long runtimeFilterTypeValue = 0L;
        Assert.assertEquals("", RuntimeFilterTypeHelper.decode(runtimeFilterTypeValue));

        runtimeFilterTypeValue = 1L;
        Assert.assertEquals("IN", RuntimeFilterTypeHelper.decode(runtimeFilterTypeValue));

        runtimeFilterTypeValue = 3L;
        Assert.assertEquals("BLOOM_FILTER,IN", RuntimeFilterTypeHelper.decode(runtimeFilterTypeValue)); // Orderly

        runtimeFilterTypeValue = 7L;
        Assert.assertEquals("BLOOM_FILTER,IN,MIN_MAX", RuntimeFilterTypeHelper.decode(runtimeFilterTypeValue)); // Orderly

        runtimeFilterTypeValue = 15L;
        Assert.assertEquals("BLOOM_FILTER,IN,IN_OR_BLOOM_FILTER,MIN_MAX", RuntimeFilterTypeHelper.decode(runtimeFilterTypeValue)); // Orderly
    }

    @Test(expected = DdlException.class)
    public void testInvalidSqlMode() throws DdlException {
        RuntimeFilterTypeHelper.encode("BLOOM,IN");
        Assert.fail("No exception throws");
    }

    @Test(expected = DdlException.class)
    public void testInvalidDecode() throws DdlException {
        RuntimeFilterTypeHelper.decode(32L);
        Assert.fail("No exception throws");
    }
}
