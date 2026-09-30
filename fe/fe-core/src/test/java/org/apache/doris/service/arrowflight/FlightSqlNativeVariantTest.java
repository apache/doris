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

import org.apache.doris.catalog.Env;
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
import java.util.Collections;

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
    public void failedHeartbeatClearsCapabilityBeforeBackendIsMarkedDead() throws Exception {
        SystemInfoService system = Env.getCurrentSystemInfo();
        Object original = system.getAllBackendsByAllCluster();
        long tolerance = Config.max_backend_heartbeat_failure_tolerance_count;
        try {
            Config.max_backend_heartbeat_failure_tolerance_count = 3;
            Backend backend = new Backend(12348, "127.0.0.1", 9050);
            Deencapsulation.setField(system, "idToBackendRef", ImmutableMap.of(backend.getId(), backend));
            ConnectContext context = new ConnectContext();
            context.getSessionVariable().setEnableArrowFlightSqlNativeVariant(true);
            backend.handleHbResponse(heartbeat(backend.getId(), true), false);
            Assert.assertTrue(FlightSqlNativeVariant.isEnabled(context));
            BackendHbResponse failed = new BackendHbResponse(backend.getId(), "127.0.0.1", 1, "timeout");
            // A capability change must be journaled even within the heartbeat failure tolerance.
            Assert.assertTrue(backend.handleHbResponse(failed, false));
            Assert.assertTrue(backend.isAlive());
            Assert.assertFalse(backend.isArrowFlightNativeVariantSupported());
            Assert.assertFalse(FlightSqlNativeVariant.isEnabled(context));
            backend.handleHbResponse(heartbeat(backend.getId(), true), false);
            Assert.assertTrue(FlightSqlNativeVariant.isEnabled(context));
            backend.handleHbResponse(failed, true);
            Assert.assertFalse(backend.isArrowFlightNativeVariantSupported());
            backend.handleHbResponse(heartbeat(backend.getId(), false), false);
            Assert.assertFalse(FlightSqlNativeVariant.isEnabled(context));
        } finally {
            Config.max_backend_heartbeat_failure_tolerance_count = tolerance;
            Deencapsulation.setField(system, "idToBackendRef", original);
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
