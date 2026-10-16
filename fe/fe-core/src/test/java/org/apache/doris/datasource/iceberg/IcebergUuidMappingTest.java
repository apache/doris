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

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.UuidLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.UuidType;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

public class IcebergUuidMappingTest {
    @Test
    public void uuidDefaultsAndWritesRetainLogicalType() {
        UUID value = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
        Types.NestedField field = Types.NestedField.optional("u").withId(1)
                .ofType(Types.UUIDType.get()).withInitialDefault(value).build();
        Assert.assertEquals(value.toString(), IcebergUtils.getSerializedInitialDefault(field, false));
        Assert.assertFalse(IcebergUtils.getBase64EncodedInitialDefaults(new Schema(field)).containsKey(1));
        Assert.assertEquals(Types.UUIDType.get(), new DorisTypeToIcebergType().atomic(Type.UUID));
        Assert.assertEquals(value, IcebergUtils.parsePartitionValueFromString(
                value.toString().replace("-", ""), Types.UUIDType.get()));
        Assert.assertEquals(value, IcebergUtils.parsePartitionValueFromString(
                value.toString().toUpperCase(java.util.Locale.ROOT), Types.UUIDType.get()));
        Assert.assertEquals(new UuidLiteral(value.toString()), IcebergWriteSchemaContext.toDorisExpression(
                Types.UUIDType.get(), value, UuidType.INSTANCE, false, false));
    }

    @Test
    public void uuidWriteCastsRejectInvalidTextAtEveryDepth() {
        SlotReference text = new SlotReference("value", StringType.INSTANCE);
        Assert.assertTrue(((Cast) IcebergUuidWriteConverter.convert(text, UuidType.INSTANCE)).isStrict());
        Assert.assertTrue(IcebergUuidWriteConverter.convert(
                new SlotReference("items", ArrayType.of(StringType.INSTANCE)),
                ArrayType.of(UuidType.INSTANCE)).anyMatch(e -> e instanceof Cast && ((Cast) e).isStrict()));
        Assert.assertTrue(IcebergUuidWriteConverter.convert(
                new SlotReference("map", MapType.of(StringType.INSTANCE, StringType.INSTANCE)),
                MapType.of(UuidType.INSTANCE, UuidType.INSTANCE)).anyMatch(e -> e instanceof Cast && ((Cast) e).isStrict()));
        Assert.assertTrue(IcebergUuidWriteConverter.convert(
                new SlotReference("record", new StructType(Collections.singletonList(
                        new StructField("u", StringType.INSTANCE, true, "")))),
                new StructType(Collections.singletonList(
                        new StructField("u", UuidType.INSTANCE, true, "")))).anyMatch(
                            e -> e instanceof Cast && ((Cast) e).isStrict()));
        Assert.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> IcebergUuidWriteConverter.convert(
                        new org.apache.doris.nereids.trees.expressions.literal.StringLiteral("invalid-uuid"),
                        UuidType.INSTANCE));
        Expression uuid = new SlotReference("uuid", UuidType.INSTANCE);
        Assert.assertSame(uuid, IcebergUuidWriteConverter.convert(uuid, UuidType.INSTANCE));
        Assert.assertEquals(new NullLiteral(UuidType.INSTANCE),
                IcebergUuidWriteConverter.convert(NullLiteral.INSTANCE, UuidType.INSTANCE));
        Assert.assertSame(text, IcebergUuidWriteConverter.convert(text, StringType.INSTANCE));
    }

    @Test
    public void incompatibleUuidBoundsDoNotPruneFiles() throws Exception {
        Assert.assertNull(IcebergUtils.extractDorisLiteral(Types.UUIDType.get(),
                new org.apache.doris.analysis.UuidLiteral("80000000-0000-0000-0000-000000000000")));
        Assert.assertNull(IcebergUtils.extractDorisLiteral(Types.UUIDType.get(),
                new org.apache.doris.analysis.StringLiteral("80000000-0000-0000-0000-000000000000")));
    }
}
