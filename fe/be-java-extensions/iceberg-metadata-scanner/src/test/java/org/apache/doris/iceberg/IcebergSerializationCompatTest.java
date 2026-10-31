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

import com.google.cloud.storage.Storage;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamConstants;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class IcebergSerializationCompatTest {
    // BaseFile's serialVersionUID in Iceberg 1.11.0; 1.12.0 changed it (and added a field).
    private static final long ICEBERG_1_11_0_BASE_FILE_UID = -8856745894322453272L;

    // An empty StaticDataTask serialized by Iceberg 1.10.1. It carries the old Schema serialVersionUID while
    // keeping all data neutral and local, so the fixture remains stable and safe to commit.
    private static final String ICEBERG_1_10_1_TASK = "rO0ABXNyACFvcmcuYXBhY2hlLmljZWJlcmcuU3RhdGljRGF0YVRhc2t2PvVIpr/rlAIABEwADG1ldGFkYXRh"
            + "RmlsZXQAHUxvcmcvYXBhY2hlL2ljZWJlcmcvRGF0YUZpbGU7TAAPcHJvamVjdGVkU2NoZW1hdAAbTG9yZy9h"
            + "cGFjaGUvaWNlYmVyZy9TY2hlbWE7WwAEcm93c3QAIFtMb3JnL2FwYWNoZS9pY2ViZXJnL1N0cnVjdExpa2U7"
            + "TAALdGFibGVTY2hlbWFxAH4AAnhwcHNyABlvcmcuYXBhY2hlLmljZWJlcmcuU2NoZW1hXonoLcvZFnYCAARJ"
            + "AA5oaWdoZXN0RmllbGRJZEkACHNjaGVtYUlkWwASaWRlbnRpZmllckZpZWxkSWRzdAACW0lMAAZzdHJ1Y3R0"
            + "ACtMb3JnL2FwYWNoZS9pY2ViZXJnL3R5cGVzL1R5cGVzJFN0cnVjdFR5cGU7eHAAAAABAAAAAHVyAAJbSU26"
            + "YCZ26rKlAgAAeHAAAAAAc3IAKW9yZy5hcGFjaGUuaWNlYmVyZy50eXBlcy5UeXBlcyRTdHJ1Y3RUeXBlY2OW"
            + "YF+O53QCAAFbAAZmaWVsZHN0AC1bTG9yZy9hcGFjaGUvaWNlYmVyZy90eXBlcy9UeXBlcyROZXN0ZWRGaWVs"
            + "ZDt4cgAob3JnLmFwYWNoZS5pY2ViZXJnLnR5cGVzLlR5cGUkTmVzdGVkVHlwZWpUXj112XcBAgAAeHB1cgAt"
            + "W0xvcmcuYXBhY2hlLmljZWJlcmcudHlwZXMuVHlwZXMkTmVzdGVkRmllbGQ7A428r/O1h1gCAAB4cAAAAAFz"
            + "cgAqb3JnLmFwYWNoZS5pY2ViZXJnLnR5cGVzLlR5cGVzJE5lc3RlZEZpZWxkRnIcmOgj/wICAAdJAAJpZFoA"
            + "CmlzT3B0aW9uYWxMAANkb2N0ABJMamF2YS9sYW5nL1N0cmluZztMAA5pbml0aWFsRGVmYXVsdHQAKExvcmcv"
            + "YXBhY2hlL2ljZWJlcmcvZXhwcmVzc2lvbnMvTGl0ZXJhbDtMAARuYW1lcQB+ABJMAAR0eXBldAAfTG9yZy9h"
            + "cGFjaGUvaWNlYmVyZy90eXBlcy9UeXBlO0wADHdyaXRlRGVmYXVsdHEAfgATeHAAAAABAHBwdAACaWRzcgAs"
            + "b3JnLmFwYWNoZS5pY2ViZXJnLnR5cGVzLlByaW1pdGl2ZUxpa2VIb2xkZXLbKTPuyM89cAIAAUwADHR5cGVB"
            + "c1N0cmluZ3EAfgASeHB0AANpbnRwdXIAIFtMb3JnL2FwYWNoZS5pY2ViZXJnLlN0cnVjdExpa2U7MJKGbVap"
            + "uFMCAAB4cAAAAABxAH4ACA==";

    @Test
    public void deserializesIceberg1101SystemTableTask() {
        FileScanTask task = IcebergSerializationCompat.deserializeFromBase64(ICEBERG_1_10_1_TASK);

        Assertions.assertEquals("id", task.schema().columns().get(0).name());
    }

    @Test
    public void preservesCurrentIcebergSerialization() throws IOException {
        Schema expected = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

        Schema actual = IcebergSerializationCompat.deserializeFromBase64(
                serializeToBase64(expected));

        Assertions.assertTrue(expected.sameSchema(actual));
    }

    @Test
    public void reportsATaskFromAnotherIcebergVersion() throws IOException {
        // WHY: every system-table task carries a DataFile (StaticDataTask's metadata file is built like this one),
        // and an FE on Iceberg 1.11 writes BaseFile with its 1.11 UID. MUTATION: dropping the InvalidClassException
        // mapping -> a bare UncheckedIOException with no hint at the version skew -> red.
        byte[] bytes = serialize(DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/dummyPath").withFileSizeInBytes(0L).withRecordCount(1L)
                .withFormat(FileFormat.METADATA).build());
        rewriteUid(bytes, "org.apache.iceberg.BaseFile", ICEBERG_1_11_0_BASE_FILE_UID);

        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> IcebergSerializationCompat.deserializeFromBase64(Base64.getEncoder().encodeToString(bytes)));
        Assertions.assertTrue(e.getMessage().contains("different Iceberg version"), e.getMessage());
        Assertions.assertInstanceOf(InvalidClassException.class, e.getCause());
    }

    @Test
    public void reportsAClassThisIcebergDoesNotHave() throws IOException {
        // WHY: Iceberg 1.12 renumbered anonymous classes a task carries (e.g. BaseFile$1), so a task from another
        // version can name a class that does not exist here. MUTATION: mapping only InvalidClassException -> red.
        byte[] bytes = serialize(new Probe());
        String name = Probe.class.getName();
        replaceAll(bytes, name.getBytes(StandardCharsets.UTF_8),
                (name.substring(0, name.length() - 1) + "X").getBytes(StandardCharsets.UTF_8));

        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> IcebergSerializationCompat.deserializeFromBase64(Base64.getEncoder().encodeToString(bytes)));
        Assertions.assertInstanceOf(ClassNotFoundException.class, e.getCause());
    }

    @Test
    public void deserializesTheGcsFileIoATaskCarries() throws Exception {
        // WHY: a REST catalog configured with io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO serializes that FileIO
        // into every $files/$manifests task (BaseFilesTable.ManifestReadTask keeps table.io()), so the BE plugin
        // has to carry iceberg-gcp plus the google-cloud-storage client GCSFileIO builds from the task's
        // properties. MUTATION: dropping either dependency from the plugin pom -> ClassNotFoundException (mapped
        // to the "different Iceberg version" message) or NoClassDefFoundError on client() -> red.
        Map<String, String> properties = new HashMap<>();
        properties.put("gcs.project-id", "gcs-fileio-test");
        // NoCredentials: the client is built here only to prove the Google SDK closure links, so the build must not
        // depend on whatever Application Default Credentials the machine running this test happens to have.
        properties.put("gcs.no-auth", "true");
        GCSFileIO fileIo = new GCSFileIO();
        fileIo.initialize(properties);

        GCSFileIO deserialized = IcebergSerializationCompat.deserializeFromBase64(serializeToBase64(fileIo));

        Assertions.assertEquals("gcs-fileio-test", deserialized.properties().get("gcs.project-id"));
        try (Storage client = deserialized.client()) {
            Assertions.assertNotNull(client);
        }
    }

    private static final class Probe implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    private static byte[] serialize(Object value) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutput = new ObjectOutputStream(output)) {
            objectOutput.writeObject(value);
        }
        return output.toByteArray();
    }

    /** Overwrites the serialVersionUID that follows the class descriptor named {@code className}. */
    private static void rewriteUid(byte[] bytes, String className, long uid) {
        byte[] name = className.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i + 3 + name.length + Long.BYTES <= bytes.length; i++) {
            if (bytes[i] == ObjectStreamConstants.TC_CLASSDESC && bytes[i + 1] == (byte) (name.length >>> 8)
                    && bytes[i + 2] == (byte) name.length && matchesAt(bytes, i + 3, name)) {
                ByteBuffer.wrap(bytes, i + 3 + name.length, Long.BYTES).putLong(uid);
                return;
            }
        }
        Assertions.fail("no class descriptor for " + className);
    }

    private static void replaceAll(byte[] bytes, byte[] from, byte[] to) {
        for (int i = 0; i + from.length <= bytes.length; i++) {
            if (matchesAt(bytes, i, from)) {
                System.arraycopy(to, 0, bytes, i, to.length);
            }
        }
    }

    private static boolean matchesAt(byte[] bytes, int offset, byte[] pattern) {
        for (int j = 0; j < pattern.length; j++) {
            if (bytes[offset + j] != pattern[j]) {
                return false;
            }
        }
        return true;
    }

    private static String serializeToBase64(Object value) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutput = new ObjectOutputStream(output)) {
            objectOutput.writeObject(value);
        }
        return Base64.getEncoder().encodeToString(output.toByteArray());
    }
}
