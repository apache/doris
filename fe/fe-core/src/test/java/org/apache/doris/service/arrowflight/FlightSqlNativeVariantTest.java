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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TColumnDesc;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TResultSinkType;

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
    public void sinkCapturesOptInWithoutChangingMysql() {
        Assert.assertFalse(new SessionVariable().isEnableArrowFlightSqlNativeVariant());
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
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }
}
