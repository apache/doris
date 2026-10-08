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

package org.apache.doris.service.arrowflight;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BackendHbResponse;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TBackendInfo;
import org.apache.doris.thrift.TColumnDesc;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TResultSinkType;

import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FlightSqlNativeVariantTest {
    @Test
    public void schemaKeepsExtensionAcrossIpc() throws Exception {
        TColumnDesc variant = new TColumnDesc("item", TPrimitiveType.VARIANT);
        variant.setIsAllowNull(true);
        TColumnDesc array = new TColumnDesc("a", TPrimitiveType.ARRAY);
        array.setChildren(Collections.singletonList(variant));
        Field field = Deencapsulation.invoke(FlightSqlSchemaHelper.class, "buildField",
                "test_db", "test_table", array, true);
        Field child = field.getChildren().get(0);
        Assert.assertEquals(new ArrowType.Struct(), child.getType());
        Assert.assertEquals("arrow.parquet.variant", child.getMetadata().get("ARROW:extension:name"));
        Assert.assertEquals("metadata", child.getChildren().get(0).getName());
        Assert.assertEquals("value", child.getChildren().get(1).getName());
        for (Field storage : child.getChildren()) {
            Assert.assertFalse(storage.isNullable());
            Assert.assertEquals(new ArrowType.Binary(), storage.getType());
        }
        Schema schema = new Schema(Collections.singletonList(field));
        try (ReadChannel channel = new ReadChannel(Channels.newChannel(
                new ByteArrayInputStream(schema.serializeAsMessage())))) {
            Assert.assertEquals(schema, MessageSerializer.deserializeSchema(channel));
        }
        Field legacy = Deencapsulation.invoke(FlightSqlSchemaHelper.class, "buildField",
                "test_db", "test_table", variant, false);
        Assert.assertEquals(new ArrowType.Utf8(), legacy.getType());
    }

    @Test
    public void querySchemaPreservesNativeVariantInNestedFields() {
        Type nested = new StructType(new ArrayList<>(Arrays.asList(
                new StructField("scalar", Type.VARIANT),
                new StructField("array", new ArrayType(Type.VARIANT, true)),
                new StructField("map", new MapType(Type.STRING, Type.VARIANT)))));
        for (boolean nativeVariant : new boolean[] {false, true}) {
            Field result = Deencapsulation.invoke(FlightSqlQuerySchema.class, "field",
                    "s", nested, true, true, "UTC", nativeVariant);
            Field scalar = result.getChildren().get(0);
            Field item = result.getChildren().get(1).getChildren().get(0);
            Field value = result.getChildren().get(2).getChildren().get(0).getChildren().get(1);
            for (Field leaf : Arrays.asList(scalar, item, value)) {
                Assert.assertEquals(nativeVariant ? new ArrowType.Struct() : new ArrowType.Utf8(), leaf.getType());
                if (nativeVariant) {
                    Assert.assertEquals("arrow.parquet.variant", leaf.getMetadata().get("ARROW:extension:name"));
                    Assert.assertEquals("", leaf.getMetadata().get("ARROW:extension:metadata"));
                    Assert.assertEquals(Arrays.asList(Field.notNullable("metadata", new ArrowType.Binary()),
                            Field.notNullable("value", new ArrowType.Binary())), leaf.getChildren());
                }
                Assert.assertEquals("VARIANT", leaf.getMetadata().get("doris_type"));
            }
            // Model execution's metadata enrichment to catch Prepare/DoGet schema mismatches.
            Field execution = FlightSqlSchemaHelper.withDorisTypeMetadata(result, nested);
            Assert.assertTrue(FlightSqlQuerySchema.matchesExecutionSchema(
                    new Schema(Collections.singletonList(result)),
                    new Schema(Collections.singletonList(execution)), Collections.singletonList("s")));
        }
    }

    @Test
    public void unknownBackendKeepsUtf8DuringUpgrade() throws Exception {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        SystemInfoService system = Env.getCurrentSystemInfo();
        Object original = system.getAllBackendsByAllCluster();
        try {
            Backend upgraded = new Backend(12346, "127.0.0.1", 9051);
            upgraded.handleHbResponse(heartbeat(upgraded.getId(), true), true);
            Deencapsulation.setField(system, "idToBackendRef", ImmutableMap.of(upgraded.getId(), upgraded));
            context.getSessionVariable().setEnableArrowFlightSqlNativeVariant(true);
            Assert.assertTrue(FlightSqlNativeVariant.isEnabled(context));
            system.addBackend(new Backend(12345, "127.0.0.1", 9050));
            Assert.assertFalse(FlightSqlNativeVariant.isEnabled(context));
            ResultSink flight = new ResultSink(new PlanNodeId(0), TResultSinkType.ARROW_FLIGHT_PROTOCOL);
            TDataSink thrift = Deencapsulation.invoke(flight, "toThrift");
            Assert.assertFalse(thrift.getResultSink().isNativeVariant());
        } finally {
            Deencapsulation.setField(system, "idToBackendRef", original);
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void metadataWaitsForScopedQuerySessionToBeRestored() throws Exception {
        SystemInfoService system = Env.getCurrentSystemInfo();
        Object original = system.getAllBackendsByAllCluster();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Backend backend = new Backend(12350, "127.0.0.1", 9050);
            backend.handleHbResponse(heartbeat(backend.getId(), true), true);
            Deencapsulation.setField(system, "idToBackendRef", ImmutableMap.of(backend.getId(), backend));
            ConnectContext context = new ConnectContext();
            for (boolean nativeVariant : new boolean[] {false, true}) {
                SessionVariable permanent = context.getSessionVariable();
                permanent.setEnableArrowFlightSqlNativeVariant(nativeVariant);
                Assert.assertEquals(nativeVariant, FlightSqlNativeVariant.isEnabled(context));
                Future<Boolean> metadata;
                synchronized (context) {
                    SessionVariable scoped = new SessionVariable();
                    scoped.setEnableArrowFlightSqlNativeVariant(!nativeVariant);
                    context.setSessionVariable(scoped);
                    try {
                        // Schema analysis holds this monitor while a SET_VAR clone is temporarily installed.
                        CountDownLatch started = new CountDownLatch(1);
                        metadata = executor.submit(() -> {
                            started.countDown();
                            return FlightSqlNativeVariant.isEnabled(context);
                        });
                        Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
                        Assert.assertThrows(TimeoutException.class, () -> metadata.get(200, TimeUnit.MILLISECONDS));
                    } finally {
                        context.setSessionVariable(permanent);
                    }
                }
                Assert.assertEquals(nativeVariant, metadata.get(5, TimeUnit.SECONDS));
            }
        } finally {
            try {
                executor.shutdownNow();
                Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            } finally {
                Deencapsulation.setField(system, "idToBackendRef", original);
            }
        }
    }

    @Test
    public void sinkCapturesOptInWithoutChangingMysql() throws Exception {
        Assert.assertFalse(new SessionVariable().isEnableArrowFlightSqlNativeVariant());
        SystemInfoService system = Env.getCurrentSystemInfo();
        Object original = system.getAllBackendsByAllCluster();
        Backend backend = new Backend(12346, "127.0.0.1", 9050);
        BackendHbResponse heartbeat = heartbeat(backend.getId(), true);
        backend.handleHbResponse(heartbeat, true);
        Deencapsulation.setField(system, "idToBackendRef", ImmutableMap.of(backend.getId(), backend));
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            context.getSessionVariable().setEnableArrowFlightSqlNativeVariant(true);
            ResultSink flight = new ResultSink(new PlanNodeId(0), TResultSinkType.ARROW_FLIGHT_PROTOCOL);
            ResultSink mysql = new ResultSink(new PlanNodeId(0));
            context.getSessionVariable().setEnableArrowFlightSqlNativeVariant(false);
            TDataSink flightSink = Deencapsulation.invoke(flight, "toThrift");
            TDataSink mysqlSink = Deencapsulation.invoke(mysql, "toThrift");
            Assert.assertTrue(flightSink.getResultSink().isNativeVariant());
            Assert.assertFalse(mysqlSink.getResultSink().isNativeVariant());
        } finally {
            Deencapsulation.setField(system, "idToBackendRef", original);
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void heartbeatReplayPreservesLivenessAndCapability() {
        long originalTolerance = Config.max_backend_heartbeat_failure_tolerance_count;
        try {
            for (int tolerance : new int[] {0, 1, 3}) {
                Config.max_backend_heartbeat_failure_tolerance_count = tolerance;
                for (boolean supported : new boolean[] {false, true}) {
                    Backend leader = new Backend(12348, "127.0.0.1", 9050);
                    Backend follower = new Backend(12348, "127.0.0.1", 9050);
                    Assert.assertTrue(applyHeartbeatAndReplay(leader, follower, heartbeat(leader.getId(), supported)));
                    int deathThreshold = Math.max(1, tolerance);
                    for (int failure = 1; failure <= deathThreshold + 1; failure++) {
                        BackendHbResponse failed = new BackendHbResponse(leader.getId(), "127.0.0.1", 1, "timeout");
                        // HeartbeatMgr journals only changed responses; every journaled BAD means death on replay.
                        Assert.assertEquals(failure == deathThreshold, applyHeartbeatAndReplay(leader, follower, failed));
                        Assert.assertEquals(failure < deathThreshold, leader.isAlive());
                        Assert.assertEquals(leader.isAlive(), follower.isAlive());
                        Assert.assertEquals(supported && leader.isAlive(), leader.isArrowFlightNativeVariantSupported());
                        Assert.assertEquals(leader.isArrowFlightNativeVariantSupported(),
                                follower.isArrowFlightNativeVariantSupported());
                    }
                    Assert.assertTrue(applyHeartbeatAndReplay(leader, follower, heartbeat(leader.getId(), supported)));
                    Assert.assertTrue(leader.isAlive());
                    Assert.assertTrue(follower.isAlive());
                    Assert.assertEquals(supported, leader.isArrowFlightNativeVariantSupported());
                    Assert.assertEquals(supported, follower.isArrowFlightNativeVariantSupported());
                }
            }
        } finally {
            Config.max_backend_heartbeat_failure_tolerance_count = originalTolerance;
        }
    }

    @Test
    public void toleratedFailureAndRecoveryRetainLastSuccessfulCapability() throws Exception {
        SystemInfoService system = Env.getCurrentSystemInfo();
        Object original = system.getAllBackendsByAllCluster();
        long tolerance = Config.max_backend_heartbeat_failure_tolerance_count;
        try {
            Config.max_backend_heartbeat_failure_tolerance_count = 3;
            Backend leader = new Backend(12348, "127.0.0.1", 9050);
            Backend follower = new Backend(12348, "127.0.0.1", 9050);
            Deencapsulation.setField(system, "idToBackendRef", ImmutableMap.of(leader.getId(), leader));
            ConnectContext context = new ConnectContext();
            context.getSessionVariable().setEnableArrowFlightSqlNativeVariant(true);
            applyHeartbeatAndReplay(leader, follower, heartbeat(leader.getId(), true));
            BackendHbResponse failed = new BackendHbResponse(leader.getId(), "127.0.0.1", 1, "timeout");
            Assert.assertFalse(applyHeartbeatAndReplay(leader, follower, failed));
            Assert.assertTrue(leader.isAlive());
            Assert.assertTrue(follower.isAlive());
            Assert.assertTrue(FlightSqlNativeVariant.isEnabled(context));
            Assert.assertTrue(follower.isArrowFlightNativeVariantSupported());
            applyHeartbeatAndReplay(leader, follower, heartbeat(leader.getId(), true));
            // A successful heartbeat resets the failure count before a later missed heartbeat.
            Assert.assertFalse(applyHeartbeatAndReplay(leader, follower, failed));
            Assert.assertFalse(applyHeartbeatAndReplay(leader, follower, failed));
            Assert.assertTrue(applyHeartbeatAndReplay(leader, follower, failed));
            Assert.assertFalse(FlightSqlNativeVariant.isEnabled(context));
            Assert.assertFalse(leader.isAlive());
            Assert.assertFalse(follower.isAlive());
            applyHeartbeatAndReplay(leader, follower, heartbeat(leader.getId(), true));
            Assert.assertTrue(FlightSqlNativeVariant.isEnabled(context));
            // The next successful process report remains authoritative, including an absent legacy bit.
            String legacy = GsonUtils.GSON.toJson(heartbeat(leader.getId(), true))
                    .replace(",\"arrowFlightNativeVariantSupported\":true", "");
            applyHeartbeatAndReplay(leader, follower, GsonUtils.GSON.fromJson(legacy, BackendHbResponse.class));
            Assert.assertTrue(leader.isAlive());
            Assert.assertTrue(follower.isAlive());
            Assert.assertFalse(FlightSqlNativeVariant.isEnabled(context));
            Assert.assertFalse(follower.isArrowFlightNativeVariantSupported());
        } finally {
            Config.max_backend_heartbeat_failure_tolerance_count = tolerance;
            Deencapsulation.setField(system, "idToBackendRef", original);
        }
    }

    private static boolean applyHeartbeatAndReplay(Backend leader, Backend follower, BackendHbResponse response) {
        boolean changed = leader.handleHbResponse(response, false);
        if (changed) {
            follower.handleHbResponse(GsonUtils.GSON.fromJson(
                    GsonUtils.GSON.toJson(response), BackendHbResponse.class), true);
        }
        return changed;
    }

    @Test
    public void heartbeatCapabilitiesRemainIndependent() {
        // Field 11 already belongs to Paimon; sharing its wire ID would enable the wrong reader.
        Assert.assertEquals(11, TBackendInfo._Fields.SUPPORTS_PAIMON_RUST_READER.getThriftFieldId());
        Assert.assertEquals(12, TBackendInfo._Fields.ARROW_FLIGHT_NATIVE_VARIANT_SUPPORTED.getThriftFieldId());
        Backend backend = new Backend(12349, "127.0.0.1", 9050);
        for (boolean paimon : new boolean[] {false, true}) {
            for (boolean variant : new boolean[] {false, true}) {
                BackendHbResponse response = heartbeat(backend.getId(), variant);
                response.setPaimonRustReaderSupported(paimon);
                backend.handleHbResponse(GsonUtils.GSON.fromJson(
                        GsonUtils.GSON.toJson(response), BackendHbResponse.class), true);
                Assert.assertEquals(paimon, backend.isPaimonRustReaderSupported());
                Assert.assertEquals(variant, backend.isArrowFlightNativeVariantSupported());
            }
        }
    }

    private static BackendHbResponse heartbeat(long id, boolean supported) {
        BackendHbResponse response = new BackendHbResponse(id, 9060, 8040, 8060,
                1, 1, "test", "mix", 0, 0, false, 8815);
        response.setArrowFlightNativeVariantSupported(supported);
        return response;
    }

    @Test
    public void heartbeatCapabilitySurvivesReplayAndClearsOnDowngrade() {
        Backend backend = new Backend(12347, "127.0.0.1", 9050);
        Assert.assertFalse(backend.isArrowFlightNativeVariantSupported());
        BackendHbResponse advertised = heartbeat(backend.getId(), true);
        String serialized = GsonUtils.GSON.toJson(advertised);
        backend.handleHbResponse(GsonUtils.GSON.fromJson(serialized, BackendHbResponse.class), true);
        Assert.assertTrue(backend.isArrowFlightNativeVariantSupported());
        // An old BE omits the new heartbeat field after a rollback.
        String oldHeartbeat = serialized.replace(",\"arrowFlightNativeVariantSupported\":true", "");
        backend.handleHbResponse(GsonUtils.GSON.fromJson(oldHeartbeat, BackendHbResponse.class), true);
        Assert.assertFalse(backend.isArrowFlightNativeVariantSupported());
    }

}
