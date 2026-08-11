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

import com.google.gson.annotations.SerializedName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Backward-compatibility guard for the deprecated legacy Elasticsearch metadata stubs.
 *
 * <p>{@link EsTable} (the legacy internal-catalog {@code engine=elasticsearch} table) and
 * {@link EsResource} ({@code CREATE RESOURCE ... type=es}) are no longer creatable and have no live
 * callers, so they now exist only as thin Gson persistence stubs. Older FE images / edit logs may still
 * contain them, so two invariants MUST hold or the FE will fail to start when replaying such an image:</p>
 * <ol>
 *   <li>the Gson subtype registrations must stay ({@code registerSubtype(EsTable)} /
 *       {@code registerSubtype(EsResource)} in {@code GsonUtils}, plus the {@code getLegacyClazz} ES mapping
 *       in {@code Resource}) — the polymorphic {@code "clazz"} discriminator throws a {@code JsonParseException}
 *       for an unregistered tag; and</li>
 *   <li>the {@code @SerializedName} field labels must stay ({@code pi}/{@code tc} for EsTable,
 *       {@code properties} for EsResource) — a rename would silently drop persisted field values.</li>
 * </ol>
 *
 * <p>Mirror of {@link LegacyHiveMetaGsonCompatTest}: each test first round-trips an empty stub (guarding
 * invariant 1), then deserializes an old-image byte stream that carries real field values and asserts they
 * survive (guarding invariant 2). If a future change removes a registration or renames a label thinking the
 * now-callerless stubs are dead, these tests trip.</p>
 */
public class LegacyEsMetaGsonCompatTest {

    @Test
    public void testLegacyEsTableStillDeserializes() throws IOException, NoSuchFieldException {
        // (1) Registration guard: an empty stub must round-trip back to EsTable.
        EsTable table = new EsTable();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        table.write(dos);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        Table restored = Table.read(dis);
        dis.close();

        Assertions.assertTrue(restored instanceof EsTable,
                "a persisted legacy EsTable must still deserialize as EsTable "
                        + "(keep registerSubtype(EsTable) in GsonUtils for old-image compatibility)");
        Assertions.assertEquals(TableIf.TableType.ELASTICSEARCH, restored.getType());

        // (2) Field-label guard: an old-image byte stream carrying a real tableContext entry must map to the
        //     same @SerializedName label 'tc'; a rename would drop the value on re-serialization.
        String skeleton = GsonUtils.GSON.toJson(new EsTable());
        Assertions.assertTrue(skeleton.contains("\"tc\":{}"),
                "expected empty tableContext to serialize under label 'tc': " + skeleton);
        String legacyJson = skeleton.replace("\"tc\":{}",
                "\"tc\":{\"hosts\":\"http://127.0.0.1:9200\"}");
        Table withData = GsonUtils.GSON.fromJson(legacyJson, Table.class);
        Assertions.assertTrue(withData instanceof EsTable);
        String reserialized = GsonUtils.GSON.toJson(withData);
        Assertions.assertTrue(reserialized.contains("\"tc\":{\"hosts\":\"http://127.0.0.1:9200\"}"),
                "legacy es tableContext must survive under label 'tc': " + reserialized);

        // The partitionInfo field is a structural sub-object that a fresh stub leaves null (so it cannot be
        // injected inline like 'tc'); guard its @SerializedName label 'pi' directly so a rename that would
        // silently drop an old image's partition metadata still trips.
        Field piField = EsTable.class.getDeclaredField("partitionInfo");
        Assertions.assertEquals("pi", piField.getAnnotation(SerializedName.class).value(),
                "EsTable.partitionInfo must stay under @SerializedName label 'pi' for old-image compatibility");
    }

    @Test
    public void testLegacyEsResourceStillDeserializes() throws IOException {
        // (1) Registration guard: an empty stub must round-trip back to EsResource.
        EsResource resource = new EsResource("legacy_es_resource");
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        resource.write(dos);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        Resource restored = Resource.read(dis);
        dis.close();

        Assertions.assertTrue(restored instanceof EsResource,
                "a persisted legacy EsResource must still deserialize as EsResource "
                        + "(keep registerSubtype(EsResource) + getLegacyClazz(ES) for old-image compatibility)");
        Assertions.assertEquals(Resource.ResourceType.ES, restored.getType());
        Assertions.assertEquals("legacy_es_resource", restored.getName());

        // (2) Field-label guard: an old-image resource carrying real properties must map to label 'properties'.
        String skeleton = GsonUtils.GSON.toJson(new EsResource("legacy_es_resource"));
        Assertions.assertTrue(skeleton.contains("\"properties\":{}"),
                "expected empty properties to serialize under label 'properties': " + skeleton);
        String legacyJson = skeleton.replace("\"properties\":{}",
                "\"properties\":{\"hosts\":\"http://127.0.0.1:9200\"}");
        Resource withData = GsonUtils.GSON.fromJson(legacyJson, Resource.class);
        Assertions.assertTrue(withData instanceof EsResource);
        Assertions.assertEquals("http://127.0.0.1:9200",
                withData.getCopiedProperties().get("hosts"),
                "legacy es properties must survive under label 'properties'");
    }
}
