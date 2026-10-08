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

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.proto.InternalService.PFetchArrowFlightSchemaResult;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.service.arrowflight.results.FlightSqlEndpointsLocation;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import com.google.protobuf.ByteString;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

class FlightSqlConnectProcessorSchemaTest {
    private static Field field(String name, ArrowType type, boolean nullable, String marker, Field... children) {
        Map<String, String> metadata = marker == null ? Collections.emptyMap()
                : Collections.singletonMap("doris_type", marker);
        return new Field(name, new FieldType(nullable, type, null, metadata), Arrays.asList(children));
    }

    private static Schema schema(Field... fields) {
        return new Schema(Arrays.asList(fields), Collections.singletonMap("schema_key", "schema_value"));
    }

    private static Schema nestedSchema(boolean annotated) {
        return schema(
                field("a", new ArrowType.List(), true, null,
                        field("item", new ArrowType.Utf8(), true, annotated ? "LARGEINT" : null)),
                field("m", new ArrowType.Map(false), true, null,
                        field("entries", new ArrowType.Struct(), false, null,
                                field("key", new ArrowType.Utf8(), false, annotated ? "LARGEINT" : null),
                                field("value", new ArrowType.Struct(), true, null,
                                        field("ip4", new ArrowType.Int(32, true), true, annotated ? "IPV4" : null),
                                        field("ip6", new ArrowType.Utf8(), true, annotated ? "IPV6" : null),
                                        field("json", new ArrowType.Utf8(), true, annotated ? "JSON" : null),
                                        field("variant", new ArrowType.Utf8(), true, annotated ? "VARIANT" : null),
                                        field("text", new ArrowType.Utf8(), true, null)))),
                field("json", new ArrowType.Utf8(), true, annotated ? "JSON" : null),
                field("variant", new ArrowType.Utf8(), true, annotated ? "VARIANT" : null));
    }

    private static List<Type> resultTypes() {
        return Arrays.asList(new ArrayType(Type.LARGEINT),
                new MapType(Type.LARGEINT, new StructType(
                        new StructField("ip4", Type.IPV4), new StructField("ip6", Type.IPV6),
                        new StructField("json", Type.JSONB), new StructField("variant", Type.VARIANT),
                        new StructField("text", Type.STRING))), Type.JSONB, Type.VARIANT);
    }

    private static Schema fetch(List<Type> types, Schema... schemas) throws Exception {
        List<FlightSqlEndpointsLocation> endpoints = new ArrayList<>();
        List<CompletableFuture<PFetchArrowFlightSchemaResult>> responses = new ArrayList<>();
        for (int i = 0; i < schemas.length; i++) {
            ArrayList<Expr> exprs = new ArrayList<>();
            for (Type type : types) {
                Expr expr = Mockito.mock(Expr.class);
                Mockito.when(expr.getType()).thenReturn(type);
                exprs.add(expr);
            }
            endpoints.add(new FlightSqlEndpointsLocation(new TUniqueId(1, i),
                    new TNetworkAddress("localhost", 10000 + i),
                    new TNetworkAddress("localhost", 11000 + i), exprs));
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(bytes)), schemas[i]);
            responses.add(CompletableFuture.completedFuture(PFetchArrowFlightSchemaResult.newBuilder()
                    .setStatus(PStatus.newBuilder().setStatusCode(0))
                    .setSchema(ByteString.copyFrom(bytes.toByteArray())).build()));
        }
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(context.getFlightSqlEndpointsLocations()).thenReturn(endpoints);
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        AtomicInteger next = new AtomicInteger();
        Mockito.when(proxy.fetchArrowFlightSchema(Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> responses.get(next.getAndIncrement()));
        try (MockedStatic<BackendServiceProxy> singleton = Mockito.mockStatic(BackendServiceProxy.class);
                FlightSqlConnectProcessor processor = new FlightSqlConnectProcessor(context)) {
            singleton.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            processor.fetchArrowFlightSchema(1000);
            return processor.getArrowSchema();
        }
    }

    @Test
    void mixedVersionsAdvertiseCompleteMetadataInEitherOrder() throws Exception {
        Schema oldSchema = nestedSchema(false);
        Schema newSchema = nestedSchema(true);
        Assertions.assertEquals(newSchema, fetch(resultTypes(), oldSchema, newSchema));
        Assertions.assertEquals(newSchema, fetch(resultTypes(), newSchema, oldSchema));
    }

    @Test
    void oldOnlyAndNewOnlyAdvertiseTheSameSchema() throws Exception {
        Assertions.assertEquals(nestedSchema(true), fetch(resultTypes(), nestedSchema(false)));
        Assertions.assertEquals(nestedSchema(true), fetch(resultTypes(), nestedSchema(true), nestedSchema(true)));
    }

    private static void assertSchemaMismatch(Schema first, Schema second) {
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                () -> fetch(Collections.singletonList(Type.JSONB), first, second));
        Assertions.assertTrue(failure.getCause().getMessage()
                .startsWith("The schema returned by results BE is different"));
    }

    @Test
    void stillRejectsPhysicalDifferences() {
        Schema expected = schema(field("value", new ArrowType.Utf8(), true, "JSON"));
        for (Field incompatible : Arrays.asList(
                field("value", new ArrowType.Int(32, true), true, "JSON"),
                field("renamed", new ArrowType.Utf8(), true, "JSON"),
                field("value", new ArrowType.Utf8(), false, "JSON"))) {
            assertSchemaMismatch(expected, schema(incompatible));
        }
    }

    @Test
    void stillRejectsConflictingOrUnrelatedMetadata() {
        Schema expected = schema(field("value", new ArrowType.Utf8(), true, "JSON"));
        Schema wrongMarker = schema(field("value", new ArrowType.Utf8(), true, "VARIANT"));
        Field extraMetadata = new Field("value", new FieldType(true, new ArrowType.Utf8(), null,
                Collections.singletonMap("other_key", "other_value")), Collections.emptyList());
        assertSchemaMismatch(expected, wrongMarker);
        assertSchemaMismatch(expected, schema(extraMetadata));
        assertSchemaMismatch(expected, new Schema(expected.getFields()));
    }
}
