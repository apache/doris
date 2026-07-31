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

package org.apache.doris.connector.es;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class EsTypeMappingTest {

    private static String wrapMapping(String propertiesJson) {
        return "{\"test_index\":{\"mappings\":{\"properties\":" + propertiesJson + "}}}";
    }

    // === Basic scalar type mapping ===

    @Test
    void testBooleanMapping() {
        String json = wrapMapping("{\"flag\":{\"type\":\"boolean\"}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        Assertions.assertEquals(1, cols.size());
        Assertions.assertEquals("flag", cols.get(0).getName());
        Assertions.assertEquals("BOOLEAN", cols.get(0).getType().getTypeName());
    }

    @Test
    void testIntegerTypes() {
        String json = wrapMapping("{\"a\":{\"type\":\"byte\"},\"b\":{\"type\":\"short\"},"
                + "\"c\":{\"type\":\"integer\"},\"d\":{\"type\":\"long\"}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        Assertions.assertEquals(4, cols.size());
        Assertions.assertEquals("TINYINT", cols.get(0).getType().getTypeName());
        Assertions.assertEquals("SMALLINT", cols.get(1).getType().getTypeName());
        Assertions.assertEquals("INT", cols.get(2).getType().getTypeName());
        Assertions.assertEquals("BIGINT", cols.get(3).getType().getTypeName());
    }

    @Test
    void testUnsignedLongMapping() {
        String json = wrapMapping("{\"big\":{\"type\":\"unsigned_long\"}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        Assertions.assertEquals("LARGEINT", cols.get(0).getType().getTypeName());
    }

    @Test
    void testFloatTypes() {
        String json = wrapMapping("{\"a\":{\"type\":\"float\"},\"b\":{\"type\":\"half_float\"},"
                + "\"c\":{\"type\":\"double\"},\"d\":{\"type\":\"scaled_float\"}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        Assertions.assertEquals("FLOAT", cols.get(0).getType().getTypeName());
        Assertions.assertEquals("FLOAT", cols.get(1).getType().getTypeName());
        Assertions.assertEquals("DOUBLE", cols.get(2).getType().getTypeName());
        Assertions.assertEquals("DOUBLE", cols.get(3).getType().getTypeName());
    }

    @Test
    void testStringTypes() {
        String json = wrapMapping("{\"a\":{\"type\":\"keyword\"},\"b\":{\"type\":\"text\"},"
                + "\"c\":{\"type\":\"ip\"},\"d\":{\"type\":\"wildcard\"}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        for (ConnectorColumn col : cols) {
            Assertions.assertEquals("STRING", col.getType().getTypeName(),
                    "Expected STRING for " + col.getName());
        }
    }

    @Test
    void testObjectAndNestedMapToJsonb() {
        String json = wrapMapping("{\"a\":{\"type\":\"object\"},\"b\":{\"type\":\"nested\"},"
                + "\"c\":{\"type\":\"flattened\"}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        for (ConnectorColumn col : cols) {
            Assertions.assertEquals("JSONB", col.getType().getTypeName(),
                    "Expected JSONB for " + col.getName());
        }
    }

    @Test
    void testFieldWithNoTypeReturnsJsonb() {
        // A field with sub-properties but no explicit type
        String json = wrapMapping("{\"addr\":{\"properties\":{\"city\":{\"type\":\"keyword\"}}}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        Assertions.assertEquals(1, cols.size());
        Assertions.assertEquals("JSONB", cols.get(0).getType().getTypeName());
    }

    @Test
    void testUnknownTypeReturnsUnsupported() {
        String json = wrapMapping("{\"geo\":{\"type\":\"geo_point\"}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        Assertions.assertEquals("UNSUPPORTED", cols.get(0).getType().getTypeName());
    }

    @Test
    void testNullTypeMapping() {
        String json = wrapMapping("{\"empty\":{\"type\":\"null\"}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        Assertions.assertEquals("NULL", cols.get(0).getType().getTypeName());
    }

    // === Date type special handling ===

    @Test
    void testDateWithoutFormatReturnsDatetimev2() {
        String json = wrapMapping("{\"created\":{\"type\":\"date\"}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        Assertions.assertEquals("DATETIMEV2", cols.get(0).getType().getTypeName());
    }

    @Test
    void testDateWithDateTimeFormat() {
        String json = wrapMapping("{\"ts\":{\"type\":\"date\","
                + "\"format\":\"yyyy-MM-dd HH:mm:ss\"}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        Assertions.assertEquals("DATETIMEV2", cols.get(0).getType().getTypeName());
    }

    @Test
    void testDateWithDateOnlyFormat() {
        String json = wrapMapping("{\"dt\":{\"type\":\"date\",\"format\":\"yyyy-MM-dd\"}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        Assertions.assertEquals("DATEV2", cols.get(0).getType().getTypeName());
    }

    @Test
    void testDateWithEpochMillisFormat() {
        String json = wrapMapping("{\"ts\":{\"type\":\"date\",\"format\":\"epoch_millis\"}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        Assertions.assertEquals("BIGINT", cols.get(0).getType().getTypeName());
    }

    @Test
    void testDateWithCustomFormatReturnsString() {
        String json = wrapMapping("{\"ts\":{\"type\":\"date\","
                + "\"format\":\"dd/MM/yyyy\"}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        Assertions.assertEquals("STRING", cols.get(0).getType().getTypeName());
    }

    @Test
    void testDateWithMultipleFormatsDateTimePriority() {
        String json = wrapMapping("{\"ts\":{\"type\":\"date\","
                + "\"format\":\"yyyy-MM-dd||yyyy-MM-dd HH:mm:ss\"}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        // datetime format takes priority over date-only
        Assertions.assertEquals("DATETIMEV2", cols.get(0).getType().getTypeName());
    }

    // === _id field mapping ===

    @Test
    void testMappingEsIdAddsIdColumn() {
        String json = wrapMapping("{\"name\":{\"type\":\"keyword\"}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, true);
        Assertions.assertEquals(2, cols.size());
        Assertions.assertEquals("_id", cols.get(0).getName());
        Assertions.assertEquals("VARCHAR", cols.get(0).getType().getTypeName());
        Assertions.assertEquals("name", cols.get(1).getName());
    }

    @Test
    void testMappingEsIdFalseNoIdColumn() {
        String json = wrapMapping("{\"name\":{\"type\":\"keyword\"}}");
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        Assertions.assertEquals(1, cols.size());
        Assertions.assertEquals("name", cols.get(0).getName());
    }

    // === Array fields via _meta ===

    @Test
    void testArrayFieldFromMeta() {
        String json = "{\"test_index\":{\"mappings\":{"
                + "\"_meta\":{\"doris\":{\"array_fields\":[\"tags\"]}},"
                + "\"properties\":{\"tags\":{\"type\":\"keyword\"}}}}}";
        List<ConnectorColumn> cols = EsTypeMapping.parseMapping("idx", json, false);
        Assertions.assertEquals(1, cols.size());
        Assertions.assertEquals("ARRAY", cols.get(0).getType().getTypeName());
    }

    // === Error handling ===

    @Test
    void testInvalidJsonThrows() {
        Assertions.assertThrows(DorisConnectorException.class,
                () -> EsTypeMapping.parseMapping("idx", "not-json", false));
    }

    @Test
    void testEmptyMappingThrows() {
        String json = "{\"test_index\":{\"mappings\":{\"dynamic\":\"strict\"}}}";
        Assertions.assertThrows(DorisConnectorException.class,
                () -> EsTypeMapping.parseMapping("idx", json, false));
    }
}
