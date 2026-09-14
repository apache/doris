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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class RuntimeFilterTypeHelperTest {

    @Test
    public void testNormal() throws DdlException {
        String runtimeFilterType = "";
        Assertions.assertEquals(new Long(0L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "IN";
        Assertions.assertEquals(new Long(1L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "BLOOM_FILTER";
        Assertions.assertEquals(new Long(2L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "MIN_MAX";
        Assertions.assertEquals(new Long(4L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "IN,MIN_MAX";
        Assertions.assertEquals(new Long(5L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "MIN_MAX, BLOOM_FILTER";
        Assertions.assertEquals(new Long(6L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "IN_OR_BLOOM_FILTER";
        Assertions.assertEquals(new Long(8L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        runtimeFilterType = "MIN_MAX,IN_OR_BLOOM_FILTER";
        Assertions.assertEquals(new Long(12L), RuntimeFilterTypeHelper.encode(runtimeFilterType));

        long runtimeFilterTypeValue = 0L;
        Assertions.assertEquals("", RuntimeFilterTypeHelper.decode(runtimeFilterTypeValue));

        runtimeFilterTypeValue = 1L;
        Assertions.assertEquals("IN", RuntimeFilterTypeHelper.decode(runtimeFilterTypeValue));
    }

    @Test
    public void testInvalidSqlMode() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            RuntimeFilterTypeHelper.encode("BLOOM,IN");
            Assertions.fail("No exception throws");
        });
    }

    @Test
    public void testInvalidDecode() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            RuntimeFilterTypeHelper.decode(32L);
            Assertions.fail("No exception throws");
        });
    }

    @Test
    public void testDeprecatedBitmapNumericCompatibility() throws DdlException {
        Assertions.assertEquals(Long.valueOf(0L), RuntimeFilterTypeHelper.encode("16"));
        Assertions.assertEquals(Long.valueOf(8L), RuntimeFilterTypeHelper.encode("24"));
        Assertions.assertEquals(Long.valueOf(12L), RuntimeFilterTypeHelper.encode("28"));

        Assertions.assertEquals("", RuntimeFilterTypeHelper.decode(16L));
        Assertions.assertEquals("IN_OR_BLOOM_FILTER", RuntimeFilterTypeHelper.decode(24L));
        Assertions.assertEquals("IN_OR_BLOOM_FILTER,MIN_MAX", RuntimeFilterTypeHelper.decode(28L));
    }

    @Test
    public void testDeprecatedBitmapIsNotAllowedForPlanning() {
        Assertions.assertFalse(RuntimeFilterTypeHelper.getSupportedRuntimeFilterTypes()
                .contains(TRuntimeFilterType.BITMAP));
        Assertions.assertFalse(RuntimeFilterTypeHelper.allowedRuntimeFilterType(24L, TRuntimeFilterType.BITMAP));
        Assertions.assertTrue(RuntimeFilterTypeHelper.allowedRuntimeFilterType(24L, TRuntimeFilterType.IN_OR_BLOOM));
    }

    @Test
    public void testDeprecatedBitmapSessionRestoreCompatibility() throws Exception {
        SessionVariable restored = new SessionVariable();
        restored.readFromJson("{\"runtime_filter_type\":24}");
        Assertions.assertEquals(TRuntimeFilterType.IN_OR_BLOOM.getValue(), restored.getRuntimeFilterType());
        Assertions.assertFalse(restored.allowedRuntimeFilterType(TRuntimeFilterType.BITMAP));

        Map<String, String> sessionVarMap = new HashMap<>();
        sessionVarMap.put(SessionVariable.RUNTIME_FILTER_TYPE, "28");
        restored.readFromMap(sessionVarMap);
        Assertions.assertEquals(TRuntimeFilterType.IN_OR_BLOOM.getValue() | TRuntimeFilterType.MIN_MAX.getValue(),
                restored.getRuntimeFilterType());
        Assertions.assertFalse(restored.allowedRuntimeFilterType(TRuntimeFilterType.BITMAP));

        SessionVariable forwarded = new SessionVariable();
        Map<String, String> forwardVariables = new HashMap<>();
        forwardVariables.put(SessionVariable.RUNTIME_FILTER_TYPE, "24");
        forwarded.setForwardedSessionVariables(forwardVariables);
        Assertions.assertEquals(TRuntimeFilterType.IN_OR_BLOOM.getValue(), forwarded.getRuntimeFilterType());
        Assertions.assertFalse(forwarded.allowedRuntimeFilterType(TRuntimeFilterType.BITMAP));

        restored.setRuntimeFilterType(TRuntimeFilterType.BITMAP.getValue());
        Assertions.assertEquals(0, restored.getRuntimeFilterType());
    }

    @Test
    public void testInvalidSqlMode2() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            RuntimeFilterTypeHelper.encode("BLOOM_FILTER,IN");
            Assertions.fail("No exception throws");
        });
    }

    @Test
    public void testInvalidSqlMode3() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            RuntimeFilterTypeHelper.encode("BLOOM_FILTER,IN_OR_BLOOM_FILTER");
            Assertions.fail("No exception throws");
        });
    }

    @Test
    public void testInvalidSqlMode4() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            RuntimeFilterTypeHelper.encode("IN,IN_OR_BLOOM_FILTER");
            Assertions.fail("No exception throws");
        });
    }

    @Test
    public void testInvalidBitmapSqlMode() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            RuntimeFilterTypeHelper.encode("BITMAP_FILTER");
            Assertions.fail("No exception throws");
        });
    }
}
