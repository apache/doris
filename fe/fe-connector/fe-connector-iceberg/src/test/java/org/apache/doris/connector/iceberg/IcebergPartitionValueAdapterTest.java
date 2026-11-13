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

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Collections;

public class IcebergPartitionValueAdapterTest {
    @Test
    public void deleteFileViewPreservesAllMetadataAndCopies() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "p", Types.LongType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
        PartitionData partition = new PartitionData(spec.rawPartitionType());
        partition.set(0, 7L);
        DeleteFile file = FileMetadata.deleteFileBuilder(spec).ofPositionDeletes()
                .withPath("/delete.puffin").withFormat("PUFFIN").withPartition(partition)
                .withRecordCount(2).withFileSizeInBytes(100)
                .withReferencedDataFile("/data.parquet").withContentOffset(10).withContentSizeInBytes(20).build();
        Types.StructType target = Types.StructType.of(
                Types.NestedField.optional(1000, "p", Types.IntegerType.get()));
        DeleteFile view = new IcebergPartitionValueAdapter(spec.rawPartitionType(), target).adapt(file);
        for (Method method : DeleteFile.class.getMethods()) {
            if (method.getParameterCount() == 0 && !method.getName().equals("partition")
                    && !method.getName().startsWith("copy")) {
                Assertions.assertEquals(method.invoke(file), method.invoke(view), method.getName());
            }
        }
        for (ContentFile<?> copy : new ContentFile<?>[] {view, view.copy(), view.copyWithoutStats(),
                view.copyWithStats(Collections.emptySet())}) {
            Assertions.assertEquals(7, copy.partition().get(0, Integer.class));
        }
        ((PartitionData) view.partition()).set(0, 8);
        Assertions.assertEquals(7L, file.partition().get(0, Long.class));
    }

    @Test
    public void adaptationUsesFieldIdsAndPreservesNulls() {
        Types.StructType sourceType = Types.StructType.of(
                Types.NestedField.optional(1000, "old_name", Types.IntegerType.get()),
                Types.NestedField.optional(1001, "nullable", Types.StringType.get()));
        PartitionData source = new PartitionData(sourceType);
        source.set(0, 7);
        Types.StructType target = Types.StructType.of(sourceType.fields().get(1),
                Types.NestedField.optional(1000, "new_name", Types.LongType.get()));
        PartitionData result = new IcebergPartitionValueAdapter(sourceType, target).adapt(source);
        Assertions.assertNull(result.get(0, Object.class));
        Assertions.assertEquals(7L, result.get(1, Long.class));
        Assertions.assertEquals(7, source.get(0, Integer.class));
    }

    @Test
    public void lossyHistoricalConversionsFailInsteadOfTruncating() {
        Assertions.assertThrows(ArithmeticException.class,
                () -> convert(Types.LongType.get(), 3000000000L, Types.IntegerType.get()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> convert(Types.DoubleType.get(), 0.1D, Types.FloatType.get()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> convert(Types.DecimalType.of(18, 2), new BigDecimal("1234567890.12"),
                        Types.DecimalType.of(9, 2)));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> convert(Types.StringType.get(), "7", Types.IntegerType.get()));
    }

    @Test
    public void floatingSpecialValuesSurviveHistoricalConversion() {
        Assertions.assertEquals(Float.NaN, convert(Types.DoubleType.get(), Double.NaN, Types.FloatType.get()));
        Assertions.assertEquals(Float.NEGATIVE_INFINITY,
                convert(Types.DoubleType.get(), Double.NEGATIVE_INFINITY, Types.FloatType.get()));
        Assertions.assertEquals(-0.0F, convert(Types.DoubleType.get(), -0.0D, Types.FloatType.get()));
    }

    @Test
    public void droppedSourcePromotionsHaveEqualPartitionKeys() {
        assertUnknownPartitionKeysEqual(Types.IntegerType.get(), 7, Types.LongType.get(), 7L);
        assertUnknownPartitionKeysEqual(Types.IntegerType.get(), Integer.MIN_VALUE,
                Types.LongType.get(), (long) Integer.MIN_VALUE);
        assertUnknownPartitionKeysEqual(Types.FloatType.get(), 1.25F, Types.DoubleType.get(), 1.25D);
        assertUnknownPartitionKeysEqual(Types.FloatType.get(), 0.1F, Types.DoubleType.get(), (double) 0.1F);
        assertUnknownPartitionKeysEqual(Types.FloatType.get(), -0.0F, Types.DoubleType.get(), -0.0D);
        assertUnknownPartitionKeysEqual(Types.FloatType.get(), Float.NaN, Types.DoubleType.get(), Double.NaN);
        assertUnknownPartitionKeysEqual(Types.FloatType.get(), Float.POSITIVE_INFINITY,
                Types.DoubleType.get(), Double.POSITIVE_INFINITY);
        assertUnknownPartitionKeysEqual(Types.DecimalType.of(9, 2), new BigDecimal("1.25"),
                Types.DecimalType.of(18, 2), new BigDecimal("1.25"));
        Assertions.assertEquals(9007199254740993L,
                convert(Types.LongType.get(), 9007199254740993L, Types.UnknownType.get()));
        Assertions.assertNull(convert(Types.IntegerType.get(), null, Types.UnknownType.get()));
    }

    private void assertUnknownPartitionKeysEqual(org.apache.iceberg.types.Type from, Object original,
            org.apache.iceberg.types.Type promotedType, Object promoted) {
        Types.StructType target = Types.StructType.of(
                Types.NestedField.optional(1000, "p", Types.UnknownType.get()));
        PartitionData first = new PartitionData(target);
        first.set(0, convert(from, original, Types.UnknownType.get()));
        PartitionData second = new PartitionData(target);
        second.set(0, convert(promotedType, promoted, Types.UnknownType.get()));
        StructLikeWrapper left = StructLikeWrapper.forType(target).set(first);
        StructLikeWrapper right = StructLikeWrapper.forType(target).set(second);
        Assertions.assertEquals(left, right);
        Assertions.assertEquals(left.hashCode(), right.hashCode());
    }

    private Object convert(org.apache.iceberg.types.Type from, Object value, org.apache.iceberg.types.Type to) {
        PartitionData source = new PartitionData(Types.StructType.of(Types.NestedField.optional(1000, "p", from)));
        source.set(0, value);
        return new IcebergPartitionValueAdapter(source.getPartitionType(),
                Types.StructType.of(Types.NestedField.optional(1000, "p", to))).adapt(source).get(0, Object.class);
    }
}
