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

package org.apache.doris.cdcclient.source.deserialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/** Unit tests for the {@link DeserializeResult} factory methods. */
class DeserializeResultTest {

    @Test
    void dml_carriesRecordsOnly() {
        List<String> rows = List.of("{\"id\":1}");
        DeserializeResult result = DeserializeResult.dml(rows);

        assertEquals(DeserializeResult.Type.DML, result.getType());
        assertEquals(rows, result.getRecords());
        assertNull(result.getDdls());
        assertNull(result.getUpdatedSchemas());
    }

    @Test
    void schemaChange_carriesDdlsAndEmptyRecords() {
        List<String> ddls = List.of("ALTER TABLE t ADD COLUMN c INT");
        DeserializeResult result = DeserializeResult.schemaChange(ddls, Collections.emptyMap());

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertEquals(ddls, result.getDdls());
        assertTrue(result.getRecords().isEmpty());
    }

    @Test
    void schemaChange_withRecords_carriesBoth() {
        List<String> ddls = List.of("ALTER TABLE t ADD COLUMN c INT");
        List<String> rows = List.of("{\"id\":1}");
        DeserializeResult result =
                DeserializeResult.schemaChange(ddls, Collections.emptyMap(), rows);

        assertEquals(DeserializeResult.Type.SCHEMA_CHANGE, result.getType());
        assertEquals(ddls, result.getDdls());
        assertEquals(rows, result.getRecords());
    }

    @Test
    void empty_hasNoData() {
        DeserializeResult result = DeserializeResult.empty();

        assertEquals(DeserializeResult.Type.EMPTY, result.getType());
        assertTrue(result.getRecords().isEmpty());
        assertNull(result.getDdls());
    }
}
