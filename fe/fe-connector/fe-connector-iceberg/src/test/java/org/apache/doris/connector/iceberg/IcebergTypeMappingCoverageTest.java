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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class IcebergTypeMappingCoverageTest {
    @Test
    public void nestedCollectionMetadataSurvivesReadAndCreateRoundTrip() {
        Types.StructType value = Types.StructType.of(
                Types.NestedField.required(71, "EventTime", Types.TimestampType.withZone(), "event instant"),
                Types.NestedField.optional(19, "Amounts", Types.ListType.ofRequired(113,
                        Types.DecimalType.of(38, 18)), "exact amounts"),
                Types.NestedField.required(104, "Labels", Types.MapType.ofOptional(92, 27,
                        Types.StringType.get(), Types.UUIDType.get()), "optional labels"));
        Type nested = Types.ListType.ofRequired(31,
                Types.MapType.ofRequired(46, 58, Types.StringType.get(), value));
        Schema original = new Schema(Types.NestedField.required(53, "Events", nested, "nested events"));

        for (boolean binaryFlag : new boolean[] {false, true}) {
            for (boolean timestampFlag : new boolean[] {false, true}) {
                ConnectorType mapped = IcebergTypeMapping.fromIcebergType(nested, binaryFlag, timestampFlag);
                assertNestedMetadata(nested, mapped);
                ConnectorColumn column = new ConnectorColumn("Events", mapped, "nested events", false, null, false);
                Schema rebuilt = IcebergSchemaBuilder.buildSchema(Collections.singletonList(column));
                // CREATE allocates new IDs; all other metadata must survive the recursive read/write conversion.
                Assertions.assertEquals(original.asStruct(), TypeUtil.reassignIds(rebuilt, original).asStruct());
                assertNestedMetadata(rebuilt.findType("Events"), IcebergTypeMapping.fromIcebergType(
                        rebuilt.findType("Events"), binaryFlag, timestampFlag));
            }
        }
    }

    @Test
    public void unsupportedNestedLeavesPreserveSupportedSiblingsAndFieldMetadata() {
        Types.StructType nested = Types.StructType.of(
                Types.NestedField.required(7, "Values", Types.ListType.ofOptional(8,
                        Types.StructType.of(
                                Types.NestedField.optional(9, "UnsupportedTime", Types.TimeType.get()),
                                Types.NestedField.required(10, "SupportedCount", Types.LongType.get(), "count")))),
                Types.NestedField.optional(11, "ZonedNanos", Types.TimestampNanoType.withZone()),
                Types.NestedField.required(12, "Payload", Types.VariantType.get()),
                Types.NestedField.optional(13, "Bytes", Types.FixedType.ofLength(16)));

        ConnectorType mapped = IcebergTypeMapping.fromIcebergType(nested, false, false);
        assertNestedMetadata(nested, mapped);
        ConnectorType element = mapped.getChildren().get(0).getChildren().get(0);
        Assertions.assertEquals("UNSUPPORTED", element.getChildren().get(0).getTypeName());
        Assertions.assertEquals("BIGINT", element.getChildren().get(1).getTypeName());
        Assertions.assertEquals("UNSUPPORTED", mapped.getChildren().get(1).getTypeName());
        Assertions.assertEquals("VARIANT_COMPUTE_V2", mapped.getChildren().get(2).getTypeName());
        Assertions.assertEquals(ConnectorType.of("VARBINARY", 16, 0), mapped.getChildren().get(3));
    }

    @Test
    public void decimalPrecisionAndScaleRoundTripAtEverySupportedBoundary() {
        for (int precision : new int[] {1, 9, 18, 28, 38}) {
            for (int scale : new int[] {0, precision / 2, precision}) {
                Types.DecimalType source = Types.DecimalType.of(precision, scale);
                ConnectorType mapped = IcebergTypeMapping.fromIcebergType(source, false, false);
                Assertions.assertEquals(ConnectorType.of("DECIMALV3", precision, scale), mapped);
                Assertions.assertEquals(source, IcebergTypeMapping.toIcebergPrimitive(mapped));
                Assertions.assertTrue(IcebergTypeMapping.isReadAs(source, mapped));
                Assertions.assertFalse(IcebergTypeMapping.isReadAs(source,
                        ConnectorType.of("DECIMALV3", precision + 1, scale)));
            }
        }
    }

    private static void assertNestedMetadata(Type source, ConnectorType mapped) {
        if (!source.isNestedType()) {
            return;
        }
        List<Types.NestedField> fields = source.asNestedType().fields();
        Assertions.assertEquals(fields.size(), mapped.getChildren().size());
        for (int i = 0; i < fields.size(); i++) {
            Types.NestedField field = fields.get(i);
            Assertions.assertEquals(field.fieldId(), mapped.getChildFieldId(i), field.toString());
            Assertions.assertEquals(field.isOptional(), mapped.isChildNullable(i), field.toString());
            if (source.isStructType()) {
                Assertions.assertEquals(field.name(), mapped.getFieldNames().get(i));
                Assertions.assertEquals(field.doc(), mapped.getChildComment(i));
            }
            assertNestedMetadata(field.type(), mapped.getChildren().get(i));
        }
    }
}
