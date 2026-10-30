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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
                Column mapped = IcebergUtils.parseField(original.columns().get(0), binaryFlag, timestampFlag);
                assertNestedMetadata(nested, mapped);
                Type rebuilt = IcebergUtils.dorisTypeToIcebergType(mapped.getType());
                // 4.2 deliberately uses nullable collection carriers for schema evolution.
                Assertions.assertTrue(rebuilt.asListType().isElementOptional());
                Assertions.assertEquals(nested.asListType().elementType().asMapType().keyType(),
                        rebuilt.asListType().elementType().asMapType().keyType());
                Assertions.assertEquals(Types.DecimalType.of(38, 18), rebuilt.asListType().elementType()
                        .asMapType().valueType().asStructType().field("amounts").type().asListType().elementType());
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

        Column mapped = IcebergUtils.parseField(Types.NestedField.optional(1, "root", nested), false, false);
        assertNestedMetadata(nested, mapped);
        StructType type = (StructType) mapped.getType();
        StructType element = (StructType) ((ArrayType) type.getFields().get(0).getType()).getItemType();
        Assertions.assertEquals(org.apache.doris.catalog.Type.UNSUPPORTED, element.getFields().get(0).getType());
        Assertions.assertEquals(org.apache.doris.catalog.Type.BIGINT, element.getFields().get(1).getType());
        Assertions.assertEquals(org.apache.doris.catalog.Type.UNSUPPORTED, type.getFields().get(1).getType());
        Assertions.assertTrue(type.getFields().get(2).getType().isVariantType());
        Assertions.assertEquals(16, ((ScalarType) type.getFields().get(3).getType()).getLength());

    }

    @Test
    public void decimalPrecisionAndScaleRoundTripAtEverySupportedBoundary() {
        for (int precision : new int[] {1, 9, 18, 28, 38}) {
            for (int scale : new int[] {0, precision / 2, precision}) {
                Types.DecimalType source = Types.DecimalType.of(precision, scale);
                org.apache.doris.catalog.Type mapped = IcebergUtils.icebergTypeToDorisType(source, false, false);
                Assertions.assertEquals(ScalarType.createDecimalV3Type(precision, scale), mapped);
                Assertions.assertEquals(source, IcebergUtils.dorisTypeToIcebergType(mapped));
            }
        }
    }

    private static void assertNestedMetadata(Type source, Column mapped) {
        if (!source.isNestedType()) {
            return;
        }
        List<Types.NestedField> fields = source.asNestedType().fields();
        Assertions.assertEquals(fields.size(), mapped.getChildren().size());
        for (int i = 0; i < fields.size(); i++) {
            Types.NestedField field = fields.get(i);
            Column child = mapped.getChildren().get(i);
            Assertions.assertEquals(field.fieldId(), child.getUniqueId(), field.toString());
            // Scan columns remain nullable; requiredness travels in the separate Iceberg field-id contract.
            Assertions.assertTrue(child.isAllowNull());
            if (source.isStructType()) {
                StructField struct = ((StructType) mapped.getType()).getFields().get(i);
                Assertions.assertEquals(field.name().toLowerCase(java.util.Locale.ROOT), child.getName());
                Assertions.assertEquals(field.doc(), struct.getComment());
                Assertions.assertEquals(field.isOptional(), struct.getContainsNull());
            }
            assertNestedMetadata(field.type(), child);
        }
    }
}
