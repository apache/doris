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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.ConnectorColumn;

import org.apache.avro.Schema;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Same-loader unit tests for HD-C4b: sourcing Hudi InternalSchema field ids onto the columns
 * ({@link HudiConnectorMetadata#attachTopLevelFieldIds}) and carrying them on {@link HudiColumnHandle}. The
 * field id is the rename-safe join key BE uses for schema-evolution BY_FIELD_ID matching; without it a renamed
 * column reads NULL on its old files.
 *
 * <p>Covers the non-evolution {@code AvroInternalSchemaConverter.convert(avro)} id source (positional ids). The
 * evolution-mode commit-metadata id source ({@code getTableInternalSchemaFromCommitMetadata}) needs a live
 * metaClient with schema.on.read commit history and is exercised only by the flip-time docker e2e.</p>
 */
public class HudiColumnFieldIdTest {

    // A representative Avro schema with mixed-case top-level names (Id/Name/Addr) exercising the case-insensitive
    // name match, plus a nested struct (only the TOP-LEVEL id is threaded onto the handle; nested ids for the BE
    // dictionary come straight from the InternalSchema via HudiSchemaUtils, not from here).
    private static final String SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"hudi_t\",\"fields\":["
            + "{\"name\":\"Id\",\"type\":[\"null\",\"int\"],\"default\":null},"
            + "{\"name\":\"Name\",\"type\":\"string\"},"
            + "{\"name\":\"Addr\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"addr_t\","
            + "\"fields\":[{\"name\":\"Street\",\"type\":[\"null\",\"string\"],\"default\":null}]}],"
            + "\"default\":null}"
            + "]}";

    private static Schema parse(String json) {
        return new Schema.Parser().parse(json);
    }

    /** Expected top-level id per (lower-cased) name, read back from the InternalSchema (not hard-coded). */
    private static Map<String, Integer> expectedIds(InternalSchema internalSchema) {
        Map<String, Integer> ids = new HashMap<>();
        for (Types.Field field : internalSchema.getRecord().fields()) {
            ids.put(field.name().toLowerCase(Locale.ROOT), field.fieldId());
        }
        return ids;
    }

    @Test
    public void fieldIdsSourcedByNameFromInternalSchema() {
        // getSchemaFromMetaClient builds columns from getTableAvroSchema(true) and sources ids from the mode-aware
        // InternalSchema; for a non-evolution table that InternalSchema is convert(latest avro). Each column must
        // carry the InternalSchema field id for its (lower-cased) name — the rename-safe BE join key. MUTATION:
        // leave uniqueId UNSET / source a fabricated positional value -> the id != the InternalSchema id -> red.
        Schema avro = parse(SCHEMA_JSON);
        List<ConnectorColumn> columns = HudiConnectorMetadata.avroSchemaToColumns(avro);
        InternalSchema internalSchema = AvroInternalSchemaConverter.convert(avro);
        Map<String, Integer> expected = expectedIds(internalSchema);

        List<ConnectorColumn> attached = HudiConnectorMetadata.attachTopLevelFieldIds(columns, internalSchema);

        Assertions.assertEquals(3, attached.size());
        for (ConnectorColumn col : attached) {
            Integer want = expected.get(col.getName());
            Assertions.assertNotNull(want, "no InternalSchema field for column " + col.getName());
            Assertions.assertEquals(want.intValue(), col.getUniqueId(),
                    "field id mismatch for column " + col.getName());
        }
        // Mixed-case avro name "Id" is matched case-insensitively to the lower-cased column "id" (the byte name BE
        // keys by). MUTATION: drop the toLowerCase on either side -> the mixed-case name misses -> UNSET -> red.
        Map<String, Integer> attachedById = new HashMap<>();
        for (ConnectorColumn col : attached) {
            attachedById.put(col.getName(), col.getUniqueId());
        }
        Assertions.assertTrue(attachedById.containsKey("id"));
        Assertions.assertTrue(attachedById.containsKey("name"));
        Assertions.assertTrue(attachedById.containsKey("addr"));
        Assertions.assertNotEquals(ConnectorColumn.UNSET_UNIQUE_ID, attachedById.get("addr").intValue());
    }

    @Test
    public void unmatchedColumnKeepsUnsetId() {
        // A column with no matching InternalSchema field (e.g. a _hoodie_* meta column absent from a
        // commit-metadata schema) must keep UNSET_UNIQUE_ID so BE falls back to BY_NAME — never a wrong id.
        // Here the columns include an extra "_hoodie_commit_time" that the (data-only) InternalSchema lacks.
        // MUTATION: default a matched-but-missing column to 0 / to a neighbour's id -> not UNSET -> red.
        Schema dataAvro = parse(SCHEMA_JSON);
        List<ConnectorColumn> columns = new ArrayList<>(
                HudiConnectorMetadata.avroSchemaToColumns(dataAvro));
        // An extra column not present in the (data-only) InternalSchema, reusing an existing column's type.
        columns.add(new ConnectorColumn("_hoodie_commit_time", columns.get(1).getType(), "", true, null));
        InternalSchema internalSchema = AvroInternalSchemaConverter.convert(dataAvro);

        List<ConnectorColumn> attached = HudiConnectorMetadata.attachTopLevelFieldIds(columns, internalSchema);

        ConnectorColumn attachedExtra = attached.get(attached.size() - 1);
        Assertions.assertEquals("_hoodie_commit_time", attachedExtra.getName());
        Assertions.assertEquals(ConnectorColumn.UNSET_UNIQUE_ID, attachedExtra.getUniqueId());
        // the real data columns are still resolved
        Assertions.assertNotEquals(ConnectorColumn.UNSET_UNIQUE_ID, attached.get(0).getUniqueId());
    }

    @Test
    public void fieldIdsMatchedByNameNotPosition() {
        // The load-bearing distinction of C4b: columns come from getTableAvroSchema(true) (which PREPENDS the 5
        // _hoodie_* meta columns) while ids come from an independent InternalSchema (data-only on an evolution
        // table), so ids MUST be joined BY NAME, not zipped positionally like legacy. Here the UNMATCHED column is
        // FIRST: columns = [_hoodie_commit_time, id, name] against a data-only InternalSchema [id, name].
        // MUTATION (regress to positional zip): _hoodie_commit_time would grab field[0]=id's id and id would grab
        // field[1]=name's id -> every column shifts onto the wrong field id (silent BY_FIELD_ID corruption).
        Schema metaFirst = parse("{\"type\":\"record\",\"name\":\"t\",\"fields\":["
                + "{\"name\":\"_hoodie_commit_time\",\"type\":[\"null\",\"string\"],\"default\":null},"
                + "{\"name\":\"Id\",\"type\":[\"null\",\"int\"],\"default\":null},"
                + "{\"name\":\"Name\",\"type\":\"string\"}]}");
        Schema dataOnly = parse("{\"type\":\"record\",\"name\":\"t\",\"fields\":["
                + "{\"name\":\"Id\",\"type\":[\"null\",\"int\"],\"default\":null},"
                + "{\"name\":\"Name\",\"type\":\"string\"}]}");
        List<ConnectorColumn> columns = HudiConnectorMetadata.avroSchemaToColumns(metaFirst);
        InternalSchema dataSchema = AvroInternalSchemaConverter.convert(dataOnly);
        Map<String, Integer> expected = expectedIds(dataSchema);

        List<ConnectorColumn> attached = HudiConnectorMetadata.attachTopLevelFieldIds(columns, dataSchema);
        Map<String, Integer> byName = new HashMap<>();
        for (ConnectorColumn col : attached) {
            byName.put(col.getName(), col.getUniqueId());
        }

        // the unmatched meta column (FIRST) stays UNSET — a positional zip would give it "id"'s field id
        Assertions.assertEquals(ConnectorColumn.UNSET_UNIQUE_ID, byName.get("_hoodie_commit_time").intValue());
        // the data columns keep THEIR OWN field id by name (not shifted by one position)
        Assertions.assertEquals(expected.get("id"), byName.get("id"));
        Assertions.assertEquals(expected.get("name"), byName.get("name"));
    }

    @Test
    public void handleCarriesFieldId() {
        // Pins only the ctor + getFieldId() round-trip (the field id is carried, not dropped/reordered). The
        // getColumnHandles threading of col.getUniqueId() onto the handle needs a live metaClient and is
        // covered only by the flip-time e2e, not by this same-loader test.
        HudiColumnHandle handle = new HudiColumnHandle("c", "int", false, 7);
        Assertions.assertEquals(7, handle.getFieldId());
    }

    @Test
    public void fieldIdIsNotPartOfHandleIdentity() {
        // The field id must NOT enter equals/hashCode (mirror IcebergColumnHandle: identity by name, not id).
        // Otherwise a plan that keys handles by identity would treat the same column with a differently-resolved
        // id as two columns. MUTATION: fold fieldId into equals/hashCode -> these two become unequal -> red.
        HudiColumnHandle a = new HudiColumnHandle("c", "int", false, 7);
        HudiColumnHandle b = new HudiColumnHandle("c", "int", false, ConnectorColumn.UNSET_UNIQUE_ID);
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
    }
}
