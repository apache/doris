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
import org.apache.doris.thrift.TRuntimeFilterType;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class RuntimeFilterTypeHelperTest {

    @Test
    public void testNormal() throws DdlException {
        String runtimeFilterType = "";
        Assert.assertEquals(new Long(0L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "IN";
        Assert.assertEquals(new Long(1L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "BLOOM_FILTER";
        Assert.assertEquals(new Long(2L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "MIN_MAX";
        Assert.assertEquals(new Long(4L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "IN,MIN_MAX";
        Assert.assertEquals(new Long(5L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "MIN_MAX, BLOOM_FILTER";
        Assert.assertEquals(new Long(6L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "IN_OR_BLOOM_FILTER";
        Assert.assertEquals(new Long(8L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "MIN_MAX,IN_OR_BLOOM_FILTER";
        Assert.assertEquals(new Long(12L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        long runtimeFilterTypeValue = 0L;
        Assert.assertEquals("", RuntimeFilterTypeHelper.decode(runtimeFilterTypeValue));

        runtimeFilterTypeValue = 1L;
        Assert.assertEquals("IN", RuntimeFilterTypeHelper.decode(runtimeFilterTypeValue));
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

    @Test
    public void testDeprecatedBitmapNumericCompatibility() throws DdlException {
        Assert.assertEquals(Long.valueOf(0L), RuntimeFilterTypeHelper.encode("16"));
        Assert.assertEquals(Long.valueOf(8L), RuntimeFilterTypeHelper.encode("24"));
        Assert.assertEquals(Long.valueOf(12L), RuntimeFilterTypeHelper.encode("28"));

        Assert.assertEquals("", RuntimeFilterTypeHelper.decode(16L));
        Assert.assertEquals("IN_OR_BLOOM_FILTER", RuntimeFilterTypeHelper.decode(24L));
        Assert.assertEquals("IN_OR_BLOOM_FILTER,MIN_MAX", RuntimeFilterTypeHelper.decode(28L));
    }

    @Test
    public void testDeprecatedBitmapIsNotAllowedForPlanning() {
        Assert.assertFalse(RuntimeFilterTypeHelper.getSupportedRuntimeFilterTypes()
                .contains(TRuntimeFilterType.BITMAP));
        Assert.assertFalse(RuntimeFilterTypeHelper.allowedRuntimeFilterType(24L, TRuntimeFilterType.BITMAP));
        Assert.assertTrue(RuntimeFilterTypeHelper.allowedRuntimeFilterType(24L, TRuntimeFilterType.IN_OR_BLOOM));
    }

    @Test
    public void testDeprecatedBitmapSessionRestoreCompatibility() throws Exception {
        SessionVariable restored = new SessionVariable();
        restored.readFromJson("{\"runtime_filter_type\":24}");
        Assert.assertEquals(TRuntimeFilterType.IN_OR_BLOOM.getValue(), restored.getRuntimeFilterType());
        Assert.assertFalse(restored.allowedRuntimeFilterType(TRuntimeFilterType.BITMAP));

        Map<String, String> sessionVarMap = new HashMap<>();
        sessionVarMap.put(SessionVariable.RUNTIME_FILTER_TYPE, "28");
        restored.readFromMap(sessionVarMap);
        Assert.assertEquals(TRuntimeFilterType.IN_OR_BLOOM.getValue() | TRuntimeFilterType.MIN_MAX.getValue(),
                restored.getRuntimeFilterType());
        Assert.assertFalse(restored.allowedRuntimeFilterType(TRuntimeFilterType.BITMAP));

        SessionVariable forwarded = new SessionVariable();
        Map<String, String> forwardVariables = new HashMap<>();
        forwardVariables.put(SessionVariable.RUNTIME_FILTER_TYPE, "24");
        forwarded.setForwardedSessionVariables(forwardVariables);
        Assert.assertEquals(TRuntimeFilterType.IN_OR_BLOOM.getValue(), forwarded.getRuntimeFilterType());
        Assert.assertFalse(forwarded.allowedRuntimeFilterType(TRuntimeFilterType.BITMAP));

        restored.setRuntimeFilterType(TRuntimeFilterType.BITMAP.getValue());
        Assert.assertEquals(0, restored.getRuntimeFilterType());
    }

    @Test(expected = DdlException.class)
    public void testInvalidSqlMode2() throws DdlException {
        RuntimeFilterTypeHelper.encode("BLOOM_FILTER,IN");
        Assert.fail("No exception throws");
    }

    @Test(expected = DdlException.class)
    public void testInvalidSqlMode3() throws DdlException {
        RuntimeFilterTypeHelper.encode("BLOOM_FILTER,IN_OR_BLOOM_FILTER");
        Assert.fail("No exception throws");
    }

    @Test(expected = DdlException.class)
    public void testInvalidSqlMode4() throws DdlException {
        RuntimeFilterTypeHelper.encode("IN,IN_OR_BLOOM_FILTER");
        Assert.fail("No exception throws");
    }

    @Test(expected = DdlException.class)
    public void testInvalidBitmapSqlMode() throws DdlException {
        RuntimeFilterTypeHelper.encode("BITMAP_FILTER");
        Assert.fail("No exception throws");
    }
}
