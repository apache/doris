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

import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.DdlException;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TRuntimeFilterType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.security.InvalidParameterException;
import java.util.Collections;

class SessionVariableRuntimeBloomFilterTest {
    @ParameterizedTest
    @ValueSource(strings = {SessionVariable.RUNTIME_BLOOM_FILTER_SIZE,
            SessionVariable.RUNTIME_BLOOM_FILTER_MIN_SIZE, SessionVariable.RUNTIME_BLOOM_FILTER_MAX_SIZE})
    void testInvalidSetValues(String variable) {
        for (String value : new String[] {"0", "-1", "32", "63", "65", "1000", "2147483647",
                "2147483648", "invalid"}) {
            SessionVariable session = new SessionVariable();
            Assertions.assertThrows(DdlException.class, () -> set(session, variable, value), variable + "=" + value);
            Assertions.assertFalse(session.setVarOnce(variable, value), variable + "=" + value);
            assertDefaults(session);
        }
    }

    @Test
    void testAllSupportedPowersOfTwo() throws Exception {
        for (int shift = 6; shift <= 30; shift++) {
            int size = 1 << shift;
            SessionVariable session = new SessionVariable();
            set(session, SessionVariable.RUNTIME_BLOOM_FILTER_SIZE, Integer.toString(size));
            set(session, SessionVariable.RUNTIME_BLOOM_FILTER_MIN_SIZE, Integer.toString(size));
            set(session, SessionVariable.RUNTIME_BLOOM_FILTER_MAX_SIZE, Integer.toString(size));
            Assertions.assertEquals(size, session.getRuntimeBloomFilterSize());
            RuntimeFilter.FilterSizeLimits limits = new RuntimeFilter.FilterSizeLimits(session);
            Assertions.assertEquals(size, limits.minVal);
            Assertions.assertEquals(size, limits.maxVal);
            Assertions.assertEquals(size, limits.defaultVal);
            TQueryOptions options = session.toThrift();
            Assertions.assertEquals(size, options.getRuntimeBloomFilterMinSize());
            Assertions.assertEquals(size, options.getRuntimeBloomFilterMaxSize());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {SessionVariable.RUNTIME_BLOOM_FILTER_SIZE,
            SessionVariable.RUNTIME_BLOOM_FILTER_MIN_SIZE, SessionVariable.RUNTIME_BLOOM_FILTER_MAX_SIZE})
    void testHistoricalValuesRejectedAtUse(String variable) throws Exception {
        for (int value : new int[] {0, -1, 32, 1000, Integer.MAX_VALUE}) {
            SessionVariable fromJson = new SessionVariable();
            fromJson.readFromJson("{\"" + variable + "\":" + value + "}");
            assertInvalidAtUse(fromJson);
            SessionVariable fromMap = new SessionVariable();
            fromMap.readFromMap(Collections.singletonMap(variable, Integer.toString(value)));
            assertInvalidAtUse(fromMap);
            set(fromJson, variable, "2097152");
            set(fromMap, variable, "2097152");
            Assertions.assertDoesNotThrow(fromJson::toThrift);
            Assertions.assertDoesNotThrow(fromMap::toThrift);
        }
    }

    @Test
    void testBoundsCanBeUpdatedInEitherOrder() throws Exception {
        for (boolean minFirst : new boolean[] {true, false}) {
            SessionVariable session = new SessionVariable();
            int size = minFirst ? 1 << 30 : 64;
            String first = minFirst ? SessionVariable.RUNTIME_BLOOM_FILTER_MIN_SIZE
                    : SessionVariable.RUNTIME_BLOOM_FILTER_MAX_SIZE;
            String second = minFirst ? SessionVariable.RUNTIME_BLOOM_FILTER_MAX_SIZE
                    : SessionVariable.RUNTIME_BLOOM_FILTER_MIN_SIZE;
            set(session, first, Integer.toString(size));
            assertInvalidAtUse(session);
            set(session, second, Integer.toString(size));
            RuntimeFilter.FilterSizeLimits limits = new RuntimeFilter.FilterSizeLimits(session);
            Assertions.assertEquals(size, limits.defaultVal);
            Assertions.assertDoesNotThrow(session::toThrift);
        }
    }

    @Test
    void testValidValuesSurviveRestoreAndForwarding() throws Exception {
        SessionVariable source = new SessionVariable();
        set(source, SessionVariable.RUNTIME_BLOOM_FILTER_MIN_SIZE, "64");
        set(source, SessionVariable.RUNTIME_BLOOM_FILTER_MAX_SIZE, "1073741824");
        SessionVariable fromJson = new SessionVariable();
        fromJson.readFromJson(source.toJson().toString());
        SessionVariable fromMap = new SessionVariable();
        fromMap.readFromMap(source.getForwardVariables());
        SessionVariable forwarded = new SessionVariable();
        forwarded.setForwardedSessionVariables(source.getForwardVariables());
        for (SessionVariable restored : new SessionVariable[] {fromJson, fromMap, forwarded}) {
            Assertions.assertEquals(64, restored.getRuntimeBloomFilterMinSize());
            Assertions.assertEquals(1 << 30, restored.getRuntimeBloomFilterMaxSize());
            Assertions.assertEquals(2097152, restored.getRuntimeBloomFilterSize());
            Assertions.assertDoesNotThrow(restored::toThrift);
        }
    }

    private static void assertInvalidAtUse(SessionVariable session) {
        Assertions.assertThrows(InvalidParameterException.class, session::getRuntimeBloomFilterSize);
        Assertions.assertThrows(InvalidParameterException.class, session::getRuntimeBloomFilterMinSize);
        Assertions.assertThrows(InvalidParameterException.class, session::getRuntimeBloomFilterMaxSize);
        Assertions.assertThrows(InvalidParameterException.class, () -> new RuntimeFilter.FilterSizeLimits(session));
        Assertions.assertThrows(InvalidParameterException.class, session::toThrift);
        Assertions.assertThrows(InvalidParameterException.class, session::getForwardVariables);
        ConnectContext context = new ConnectContext();
        context.setSessionVariable(session);
        context.setThreadLocalInfo();
        try {
            for (boolean useDefault : new boolean[] {false, true}) {
                session.useRuntimeFilterDefaultSize = useDefault;
                Assertions.assertThrows(InvalidParameterException.class, () -> new RuntimeFilter(
                        RuntimeFilterId.createGenerator().getNextId(), null, null, 0, Collections.emptyList(),
                        TRuntimeFilterType.BLOOM,
                        1000, TMinMaxRuntimeFilterType.MIN_MAX));
            }
        } finally {
            ConnectContext.remove();
        }
    }

    private static void assertDefaults(SessionVariable session) {
        Assertions.assertEquals(2097152, session.getRuntimeBloomFilterSize());
        Assertions.assertEquals(1048576, session.getRuntimeBloomFilterMinSize());
        Assertions.assertEquals(67108864, session.getRuntimeBloomFilterMaxSize());
    }

    private static void set(SessionVariable session, String variable, String value) throws DdlException {
        VariableMgr.setVar(session, new SetVar(SetType.SESSION, variable, new StringLiteral(value)));
    }
}
