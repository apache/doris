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

package org.apache.doris.iceberg;

import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.Schema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

final class IcebergSerializationCompat {
    private static final long ICEBERG_1_10_1_SCHEMA_UID = 6812231194765760118L;
    private static final long ICEBERG_1_11_0_SCHEMA_UID = -1265875184407129845L;
    private static final ObjectStreamClass LOCAL_SCHEMA_DESCRIPTOR = ObjectStreamClass.lookup(Schema.class);

    private IcebergSerializationCompat() {
    }

    @SuppressWarnings({"DangerousJavaDeserialization", "unchecked"})
    static <T> T deserializeFromBase64(String base64) {
        if (base64 == null) {
            return null;
        }
        byte[] bytes = Base64.getMimeDecoder().decode(base64.getBytes(StandardCharsets.UTF_8));
        try (ByteArrayInputStream input = new ByteArrayInputStream(bytes);
                ObjectInputStream objectInput = new SchemaCompatibleObjectInputStream(input)) {
            return (T) objectInput.readObject();
        } catch (InvalidClassException | ClassNotFoundException e) {
            // A class in the task differs from, or is missing in, this BE's Iceberg: the split was serialized by an
            // FE on another Iceberg version. Only Schema is shimmed; Iceberg 1.12 changed BaseFile, PartitionData and
            // their anonymous classes, which every task carries, so the task cannot be read here.
            throw new IllegalStateException("Cannot read an Iceberg system table split serialized with a different"
                    + " Iceberg version than this BE's (" + IcebergBuild.version() + "). Iceberg system tables are"
                    + " unavailable while FE and BE run different Doris versions, e.g. during a rolling upgrade.", e);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to deserialize object", e);
        }
    }

    private static final class SchemaCompatibleObjectInputStream extends ObjectInputStream {
        private SchemaCompatibleObjectInputStream(ByteArrayInputStream input) throws IOException {
            super(input);
        }

        @Override
        protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
            ObjectStreamClass descriptor = super.readClassDescriptor();
            // Iceberg 1.11 changed Schema's generated UID without changing its serialized fields. Accept only
            // that known rolling-upgrade pair so unrelated or future class-layout changes still fail closed.
            if (Schema.class.getName().equals(descriptor.getName())
                    && descriptor.getSerialVersionUID() == ICEBERG_1_10_1_SCHEMA_UID
                    && LOCAL_SCHEMA_DESCRIPTOR.getSerialVersionUID() == ICEBERG_1_11_0_SCHEMA_UID) {
                return LOCAL_SCHEMA_DESCRIPTOR;
            }
            return descriptor;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass descriptor) throws IOException, ClassNotFoundException {
            String className = descriptor.getName();
            if (className.indexOf('/') >= 0) {
                // Replacing Schema's stream descriptor exposes JVM-style array signatures with slashes; resolve
                // them through the scanner's isolated class loader after normalizing to Class.forName syntax.
                return Class.forName(className.replace('/', '.'), false,
                        IcebergSerializationCompat.class.getClassLoader());
            }
            return super.resolveClass(descriptor);
        }
    }
}
