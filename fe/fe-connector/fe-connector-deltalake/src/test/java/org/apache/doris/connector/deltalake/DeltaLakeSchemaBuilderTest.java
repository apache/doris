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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TSchema;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DeltaLakeSchemaBuilderTest {
    @Test
    void buildsLogicalAndPhysicalNamesWithStableNestedIds() {
        StructType nested = new StructType().add(field("Renamed", LongType.LONG, 2, "physical-child"));
        StructType schema = new StructType().add(field("Root", nested, 1, "physical-root"));
        TSchema logical = DeltaLakeSchemaBuilder.mappingSchema(schema, false);
        TSchema physical = DeltaLakeSchemaBuilder.mappingSchema(schema, true);
        TField logicalRoot = logical.getRootField().getFields().get(0).getFieldPtr();
        TField physicalRoot = physical.getRootField().getFields().get(0).getFieldPtr();
        Assertions.assertEquals(0, logical.getSchemaId());
        Assertions.assertEquals(1, physical.getSchemaId());
        Assertions.assertEquals("root", logicalRoot.getName());
        Assertions.assertEquals("physical-root", physicalRoot.getName());
        Assertions.assertEquals(1, logicalRoot.getId());
        TField child = logicalRoot.getNestedField().getStructField().getFields().get(0).getFieldPtr();
        Assertions.assertEquals("renamed", child.getName());
        Assertions.assertEquals(2, child.getId());
        Assertions.assertEquals("physical-child", physicalRoot.getNestedField().getStructField()
                .getFields().get(0).getFieldPtr().getName());
    }

    @Test
    void retainsOptionalCollectionIdsFromTheNearestStructField() {
        FieldMetadata ids = FieldMetadata.builder().putLong("col-map.key", 11)
                .putLong("col-map.value", 12).putLong("col-map.value.element", 13).build();
        StructField mapped = field("Map", new MapType(StringType.STRING,
                new ArrayType(LongType.LONG, true), true), 1, "col-map");
        mapped = mapped.withNewMetadata(FieldMetadata.builder().fromMetadata(mapped.getMetadata())
                .putFieldMetadata("delta.columnMapping.nested.ids", ids).build());
        for (boolean physical : new boolean[] {false, true}) {
            TField root = DeltaLakeSchemaBuilder.mappingSchema(new StructType().add(mapped), physical)
                    .getRootField().getFields().get(0).getFieldPtr();
            Assertions.assertEquals(11, root.getNestedField().getMapField().getKeyField().getFieldPtr().getId());
            TField value = root.getNestedField().getMapField().getValueField().getFieldPtr();
            Assertions.assertEquals(12, value.getId());
            Assertions.assertEquals(13, value.getNestedField().getArrayField().getItemField().getFieldPtr().getId());
        }
        StructField plain = field("Array", new ArrayType(LongType.LONG, true), 3, "col-array");
        TField array = DeltaLakeSchemaBuilder.mappingSchema(new StructType().add(plain), true)
                .getRootField().getFields().get(0).getFieldPtr();
        Assertions.assertFalse(array.getNestedField().getArrayField().getItemField().getFieldPtr().isSetId());
    }

    @Test
    void serializesBothSchemasAndTimestampSemanticsWithoutChangingNoneMode() {
        StructType schema = new StructType().add(field("TS", TimestampType.TIMESTAMP, 1, "col-ts"))
                .add(field("NTZ", TimestampNTZType.TIMESTAMP_NTZ, 2, "col-ntz"));
        TFileScanRangeParams params = new TFileScanRangeParams();
        DeltaLakeSchemaBuilder.applyMapping(params, null);
        Assertions.assertFalse(params.isSetCurrentSchemaId());
        DeltaLakeSchemaBuilder.applyMapping(params, DeltaLakeSchemaBuilder.encodeMapping(schema));
        Assertions.assertEquals(0, params.getCurrentSchemaId());
        Assertions.assertEquals(2, params.getHistorySchemaInfoSize());
        for (TSchema entry : params.getHistorySchemaInfo()) {
            Assertions.assertTrue(entry.getRootField().getFields().get(0).getFieldPtr().isTimestampIsAdjustedToUtc());
            Assertions.assertFalse(entry.getRootField().getFields().get(1).getFieldPtr().isTimestampIsAdjustedToUtc());
        }
        Assertions.assertThrows(DorisConnectorException.class, () -> DeltaLakeSchemaBuilder.applyMapping(params, "!"));
    }

    private static StructField field(String name, io.delta.kernel.types.DataType type, long id, String physical) {
        return new StructField(name, type, true, FieldMetadata.builder().putLong("delta.columnMapping.id", id)
                .putString("delta.columnMapping.physicalName", physical).build());
    }
}
