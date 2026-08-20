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

package org.apache.doris.catalog;

import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Backward-compatibility guard for the deprecated legacy hive metadata stubs.
 *
 * <p>{@link HiveTable} (the legacy internal-catalog {@code engine=hive} table) and
 * {@link HMSResource} ({@code CREATE RESOURCE ... type=hms}) are no longer creatable and have no live
 * callers, so they now exist only as thin Gson persistence stubs. Older FE images / edit logs may still
 * contain them, so two invariants MUST hold or the FE will fail to start when replaying such an image:</p>
 * <ol>
 *   <li>the Gson subtype registrations must stay ({@code registerSubtype(HiveTable)} /
 *       {@code registerSubtype(HMSResource)} in {@code GsonUtils}, plus the {@code getLegacyClazz} HMS mapping
 *       in {@code Resource}) — the polymorphic {@code "clazz"} discriminator throws a {@code JsonParseException}
 *       for an unregistered tag; and</li>
 *   <li>the {@code @SerializedName} field labels must stay ({@code hdb}/{@code ht}/{@code hp} for HiveTable,
 *       {@code properties} for HMSResource) — a rename would silently drop persisted field values.</li>
 * </ol>
 *
 * <p>Each test first round-trips an empty stub (guarding invariant 1), then deserializes an old-image byte
 * stream that carries real field values and asserts they survive (guarding invariant 2). If a future change
 * removes a registration or renames a label thinking the now-callerless stubs are dead, these tests trip.</p>
 */
public class LegacyHiveMetaGsonCompatTest {

    @Test
    public void testLegacyHiveTableStillDeserializes() throws IOException {
        // (1) Registration guard: an empty stub must round-trip back to HiveTable.
        HiveTable table = new HiveTable();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        table.write(dos);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        Table restored = Table.read(dis);
        dis.close();

        Assertions.assertTrue(restored instanceof HiveTable,
                "a persisted legacy HiveTable must still deserialize as HiveTable "
                        + "(keep registerSubtype(HiveTable) in GsonUtils for old-image compatibility)");
        Assertions.assertEquals(TableIf.TableType.HIVE, restored.getType());

        // (2) Field-label guard: an old-image byte stream carrying real hdb/ht/hp values must map to the
        //     same @SerializedName labels; a rename would drop the value on re-serialization.
        String skeleton = GsonUtils.GSON.toJson(new HiveTable());
        Assertions.assertTrue(skeleton.contains("\"hp\":{}"),
                "expected empty hiveProperties to serialize under label 'hp': " + skeleton);
        String legacyJson = skeleton.replace("\"hp\":{}",
                "\"hdb\":\"db0\",\"ht\":\"tbl0\",\"hp\":{\"hive.metastore.uris\":\"thrift://127.0.0.1:9083\"}");
        Table withData = GsonUtils.GSON.fromJson(legacyJson, Table.class);
        Assertions.assertTrue(withData instanceof HiveTable);
        String reserialized = GsonUtils.GSON.toJson(withData);
        Assertions.assertTrue(reserialized.contains("\"hdb\":\"db0\""),
                "legacy hive db must survive under label 'hdb': " + reserialized);
        Assertions.assertTrue(reserialized.contains("\"ht\":\"tbl0\""),
                "legacy hive table must survive under label 'ht': " + reserialized);
        Assertions.assertTrue(reserialized.contains("hive.metastore.uris"),
                "legacy hive properties must survive under label 'hp': " + reserialized);
    }

    @Test
    public void testLegacyHmsResourceStillDeserializes() throws IOException {
        // (1) Registration guard: an empty stub must round-trip back to HMSResource.
        HMSResource resource = new HMSResource("legacy_hms_resource");
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        resource.write(dos);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        Resource restored = Resource.read(dis);
        dis.close();

        Assertions.assertTrue(restored instanceof HMSResource,
                "a persisted legacy HMSResource must still deserialize as HMSResource "
                        + "(keep registerSubtype(HMSResource) + getLegacyClazz(HMS) for old-image compatibility)");
        Assertions.assertEquals(Resource.ResourceType.HMS, restored.getType());
        Assertions.assertEquals("legacy_hms_resource", restored.getName());

        // (2) Field-label guard: an old-image resource carrying real properties must map to label 'properties'.
        String skeleton = GsonUtils.GSON.toJson(new HMSResource("legacy_hms_resource"));
        Assertions.assertTrue(skeleton.contains("\"properties\":{}"),
                "expected empty properties to serialize under label 'properties': " + skeleton);
        String legacyJson = skeleton.replace("\"properties\":{}",
                "\"properties\":{\"hive.metastore.uris\":\"thrift://127.0.0.1:9083\"}");
        Resource withData = GsonUtils.GSON.fromJson(legacyJson, Resource.class);
        Assertions.assertTrue(withData instanceof HMSResource);
        Assertions.assertEquals("thrift://127.0.0.1:9083",
                withData.getCopiedProperties().get("hive.metastore.uris"),
                "legacy hms properties must survive under label 'properties'");
    }
}
