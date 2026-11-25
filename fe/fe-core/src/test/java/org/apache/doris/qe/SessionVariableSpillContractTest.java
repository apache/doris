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
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.GlobalVarPersistInfo;
import org.apache.doris.thrift.TQueryOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

class SessionVariableSpillContractTest {
    private static final long MIB = 1024L * 1024;
    private static final long GIB = 1024L * MIB;

    private static Stream<Arguments> ranges() {
        // Variable, minimum, interior value, maximum, default.
        return Stream.of(
                Arguments.of(SessionVariable.SPILL_HASH_JOIN_PARTITION_COUNT, 2L, 5L, 32L, 4L),
                Arguments.of(SessionVariable.SPILL_AGGREGATION_PARTITION_COUNT, 2L, 5L, 32L, 4L),
                Arguments.of(SessionVariable.SPILL_REPARTITION_MAX_DEPTH, 1L, 5L, 128L, 8L),
                Arguments.of(SessionVariable.SPILL_BUFFER_SIZE_BYTES, MIB, 2 * MIB, 256 * MIB, 8 * MIB),
                Arguments.of(SessionVariable.SPILL_AGGREGATION_SINK_MEM_LIMIT_BYTES, MIB, 2 * MIB, 4 * GIB, 64 * MIB),
                Arguments.of(SessionVariable.SPILL_SORT_MERGE_MEM_LIMIT_BYTES, MIB, 2 * MIB, 4 * GIB, 64 * MIB));
    }

    @Test
    void aggregationDefaultAgreesWithProtocolDefault() {
        SessionVariable session = new SessionVariable();
        Assertions.assertEquals(64 * MIB, session.spillAggregationSinkMemLimitBytes);
        Assertions.assertEquals(new TQueryOptions().getSpillAggregationSinkMemLimitBytes(),
                session.toThrift().getSpillAggregationSinkMemLimitBytes());
        Assertions.assertTrue(session.toThrift().isSetSpillAggregationSinkMemLimitBytes());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ranges")
    void defaultsAndReset(String name, long min, long interior, long max, long defaultValue) throws Exception {
        SessionVariable session = new SessionVariable();
        assertStoredValue(session, name, defaultValue);
        set(session, name, interior);
        VariableMgr.setVar(session, new SetVar(SetType.SESSION, name, null));
        assertStoredValue(session, name, defaultValue);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ranges")
    void setStoresAndForwardsEffectiveValue(String name, long min, long interior, long max, long defaultValue)
            throws Exception {
        long[][] samples = {{min - 1, min}, {min, min}, {interior, interior}, {max, max},
                {max + 1, max}, {0, min}, {-1, min}};
        SessionVariable session = new SessionVariable();
        for (long[] sample : samples) {
            set(session, name, sample[0]);
            assertStoredValue(session, name, sample[1]);
            SessionVariable forwarded = new SessionVariable();
            forwarded.setForwardedSessionVariables(session.getForwardVariables());
            assertStoredValue(forwarded, name, sample[1]);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ranges")
    void historicalReadPathsAndHintRestoration(String name, long min, long interior, long max, long defaultValue)
            throws Exception {
        long[][] samples = {{max + 1, max}, {0, min}, {-1, min}};
        for (long[] sample : samples) {
            Map<String, String> raw = Collections.singletonMap(name, Long.toString(sample[0]));
            SessionVariable fromJson = new SessionVariable();
            fromJson.readFromJson("{\"" + name + "\":" + sample[0] + "}");
            assertRestoredValue(fromJson, name, interior, sample[1]);

            SessionVariable fromMap = new SessionVariable();
            fromMap.readFromMap(raw);
            assertRestoredValue(fromMap, name, interior, sample[1]);

            SessionVariable forwarded = new SessionVariable();
            forwarded.setForwardedSessionVariables(raw);
            assertRestoredValue(forwarded, name, interior, sample[1]);
        }
    }

    @Test
    void historicalAggregationDefaultKeepsItsEffectiveValue() throws Exception {
        String name = SessionVariable.SPILL_AGGREGATION_SINK_MEM_LIMIT_BYTES;
        Map<String, String> raw = Collections.singletonMap(name, Long.toString(64 * GIB));
        SessionVariable fromJson = new SessionVariable();
        fromJson.readFromJson("{\"" + name + "\":" + 64 * GIB + "}");
        assertRestoredValue(fromJson, name, 64 * MIB, 4 * GIB);
        SessionVariable fromMap = new SessionVariable();
        fromMap.readFromMap(raw);
        assertRestoredValue(fromMap, name, 64 * MIB, 4 * GIB);
        SessionVariable forwarded = new SessionVariable();
        forwarded.setForwardedSessionVariables(raw);
        assertRestoredValue(forwarded, name, 64 * MIB, 4 * GIB);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ranges")
    void absentKeysPreserveExistingValue(String name, long min, long interior, long max, long defaultValue)
            throws Exception {
        SessionVariable session = new SessionVariable();
        set(session, name, interior);
        session.readFromJson("{}");
        session.readFromMap(Collections.emptyMap());
        session.setForwardedSessionVariables(Collections.emptyMap());
        assertStoredValue(session, name, interior);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ranges")
    void invalidNumbersStillFailWithoutChangingValue(String name, long min, long interior, long max, long defaultValue)
            throws Exception {
        SessionVariable session = new SessionVariable();
        set(session, name, interior);
        boolean isInt = VariableMgr.getVarContext(name).getField().getType() == int.class;
        String overflow = isInt ? "2147483648" : "9223372036854775808";
        for (String invalid : new String[] {"not_an_integer", "1.5", overflow}) {
            Assertions.assertThrows(DdlException.class, () -> VariableMgr.setVar(session,
                    new SetVar(SetType.SESSION, name, new StringLiteral(invalid))), name);
            assertStoredValue(session, name, interior);
        }
        set(session, name, isInt ? Integer.MIN_VALUE : Long.MIN_VALUE);
        assertStoredValue(session, name, min);
        set(session, name, isInt ? Integer.MAX_VALUE : Long.MAX_VALUE);
        assertStoredValue(session, name, max);
    }

    @Test
    void historicalGlobalEditLogNormalizesBeforeNewSessions() throws Exception {
        SessionVariable defaults = VariableMgr.getDefaultSessionVariable();
        long aggregationBefore = defaults.spillAggregationSinkMemLimitBytes;
        int partitionBefore = defaults.spillAggregationPartitionCount;
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            Text.writeString(new DataOutputStream(bytes), "{\"spill_aggregation_sink_mem_limit_bytes\":68719476736,"
                    + "\"spill_aggregation_partition_count\":64}");
            GlobalVarPersistInfo info = GlobalVarPersistInfo.read(
                    new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
            VariableMgr.replayGlobalVariableV2(info, false);
            SessionVariable session = VariableMgr.newSessionVariable();
            assertStoredValue(session, SessionVariable.SPILL_AGGREGATION_SINK_MEM_LIMIT_BYTES, 4 * GIB);
            assertStoredValue(session, SessionVariable.SPILL_AGGREGATION_PARTITION_COUNT, 32);
        } finally {
            defaults.spillAggregationSinkMemLimitBytes = aggregationBefore;
            defaults.spillAggregationPartitionCount = partitionBefore;
        }
    }

    @Test
    void unrelatedLongVariablesAreNotNormalized() throws Exception {
        SessionVariable session = new SessionVariable();
        String name = SessionVariable.SPILL_JOIN_BUILD_SINK_MEM_LIMIT_BYTES;
        set(session, name, 64 * GIB);
        assertStoredValue(session, name, 64 * GIB);
        session.readFromJson("{\"" + name + "\":0}");
        assertStoredValue(session, name, 0);
        session.readFromMap(Collections.singletonMap(name, "-1"));
        assertStoredValue(session, name, -1);
    }

    private static void set(SessionVariable session, String name, long value) throws DdlException {
        VariableMgr.setVar(session, new SetVar(SetType.SESSION, name, new StringLiteral(Long.toString(value))));
    }

    private static void assertStoredValue(SessionVariable session, String name, long expected) throws Exception {
        Assertions.assertEquals(expected,
                ((Number) VariableMgr.getVarContext(name).getField().get(session)).longValue(), name);
        Assertions.assertEquals(expected, ((Number) session.toJson().get(name)).longValue(), name);
        TQueryOptions options = session.toThrift();
        TQueryOptions._Fields field = TQueryOptions._Fields.findByName(name);
        Assertions.assertTrue(options.isSet(field), name);
        Assertions.assertEquals(expected, ((Number) options.getFieldValue(field)).longValue(), name);
        Assertions.assertEquals(Long.toString(expected), session.getForwardVariables().get(name), name);
    }

    private static void assertRestoredValue(SessionVariable session, String name, long temporary, long expected)
            throws Exception {
        assertStoredValue(session, name, expected);
        Assertions.assertTrue(session.setVarOnce(name, Long.toString(temporary)), name);
        assertStoredValue(session, name, temporary);
        VariableMgr.revertSessionValue(session);
        assertStoredValue(session, name, expected);
    }
}
