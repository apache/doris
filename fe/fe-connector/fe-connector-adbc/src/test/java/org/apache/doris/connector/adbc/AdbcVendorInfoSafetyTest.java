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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcInfoCode;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class AdbcVendorInfoSafetyTest {
    private static final int CONNECTION_SCOPED_VENDOR_INFO = 0x444f0001;

    @Test
    void refusesSharedFlightInfoBeforeRequestingAVendor() {
        try (RecordingClient client = new RecordingClient("ADBC Flight SQL Driver - Go", null)) {
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class, client::vendorName);
            Assertions.assertTrue(error.getMessage().contains("connection-scoped"), error.getMessage());
            Assertions.assertEquals(0, client.vendorCalls);
        }
    }

    @Test
    void refusesExplicitlyFalseIsolationCapability() {
        try (RecordingClient client = new RecordingClient("ADBC Flight SQL Driver - Go", false)) {
            Assertions.assertThrows(DorisConnectorException.class, client::vendorName);
            Assertions.assertEquals(0, client.vendorCalls);
        }
    }

    @Test
    void readsAnIsolatedFlightVendorOnce() {
        try (RecordingClient client = new RecordingClient("ADBC Flight SQL Driver - Go", true)) {
            Assertions.assertEquals(Optional.of("DorisFE"), client.vendorName());
            Assertions.assertEquals(Optional.of("DorisFE"), client.vendorName());
            Assertions.assertEquals(1, client.vendorCalls);
        }
    }

    @Test
    void otherDriversNeedNoFlightSpecificCapability() {
        try (RecordingClient client = new RecordingClient("Other ADBC Driver", null)) {
            Assertions.assertEquals(Optional.of("DorisFE"), client.vendorName());
            Assertions.assertEquals(1, client.vendorCalls);
        }
    }

    @Test
    void unavailableDriverNamesPreserveTheGenericVendorPath() {
        try (RecordingClient client = new RecordingClient(null, null)) {
            Assertions.assertEquals(Optional.of("DorisFE"), client.vendorName());
            Assertions.assertEquals(1, client.vendorCalls);
        }
    }

    private static final class RecordingClient extends AdbcClient {
        private final RootAllocator allocator = new RootAllocator();
        private final String driverName;
        private final Boolean isolated;
        private int vendorCalls;

        private RecordingClient(String driverName, Boolean isolated) {
            super(Path.of("driver.so"), "driver.so", null, "grpc://localhost:1", null, null, Map.of());
            this.driverName = driverName;
            this.isolated = isolated;
        }

        @Override
        public <T> T withConnection(AdbcConnectionCall<T> body) {
            try {
                return body.apply(new AdbcConnection() {
                    @Override
                    public ArrowReader getInfo(int[] codes) throws AdbcException {
                        int code = codes[0];
                        Object value;
                        if (code == AdbcInfoCode.DRIVER_NAME.getValue()) {
                            if (driverName == null) {
                                throw AdbcException.notImplemented("No driver name");
                            }
                            value = driverName;
                        } else if (code == CONNECTION_SCOPED_VENDOR_INFO) {
                            value = isolated;
                        } else {
                            Assertions.assertEquals(AdbcInfoCode.VENDOR_NAME.getValue(), code);
                            vendorCalls++;
                            value = "DorisFE";
                        }
                        try {
                            return response(code, value);
                        } catch (IOException e) {
                            throw new AdbcException("Cannot create info response", e,
                                    AdbcStatusCode.IO, null, 0);
                        }
                    }

                    @Override
                    public AdbcStatement createStatement() {
                        throw new AssertionError("Vendor safety checks execute no SQL");
                    }

                    @Override
                    public void close() {
                    }
                });
            } catch (AdbcException e) {
                throw AdbcClient.translate(e, "ADBC operation failed");
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        private ArrowReader response(int code, Object value) throws IOException {
            Schema schema = new Schema(List.of(Field.nullable("info_name", new ArrowType.Int(32, false)),
                    new Field("info_value", FieldType.nullable(new ArrowType.Union(UnionMode.Dense, new int[] {0, 1})),
                            List.of(Field.nullable("string_value", ArrowType.Utf8.INSTANCE),
                                    Field.nullable("bool_value", ArrowType.Bool.INSTANCE)))));
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                    ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(bytes))) {
                writer.start();
                if (value != null) {
                    root.allocateNew();
                    ((UInt4Vector) root.getVector("info_name")).setSafe(0, code);
                    DenseUnionVector union = (DenseUnionVector) root.getVector("info_value");
                    byte type = (byte) (value instanceof Boolean ? 1 : 0);
                    union.setTypeId(0, type);
                    union.setOffset(0, 0);
                    if (type == 1) {
                        ((BitVector) union.getVectorByType(type)).setSafe(0, Boolean.TRUE.equals(value) ? 1 : 0);
                    } else {
                        ((VarCharVector) union.getVectorByType(type)).setSafe(0,
                                value.toString().getBytes(StandardCharsets.UTF_8));
                    }
                    root.setRowCount(1);
                    writer.writeBatch();
                }
                writer.end();
            }
            return new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator);
        }

        @Override
        public void close() {
            super.close();
            allocator.close();
        }
    }
}
