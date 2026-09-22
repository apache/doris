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

package org.apache.doris.datasource;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.paimon.PaimonUtil;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalTable;

import org.apache.iceberg.types.Types;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;

public class ExternalTimestampMappingTest {
    @Test
    public void testHudiInstantTimestampMapping() {
        Assert.assertEquals(ScalarType.createDatetimeV2Type(6),
                org.apache.doris.datasource.hudi.HudiUtils.fromAvroHudiTypeToDorisType(
                        org.apache.avro.LogicalTypes.localTimestampMicros()
                                .addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG))));
        Assert.assertEquals(ScalarType.createTimeStampTzType(3),
                org.apache.doris.datasource.hudi.HudiUtils.fromAvroHudiTypeToDorisType(
                        org.apache.avro.LogicalTypes.timestampMillis()
                                .addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG))));
        Assert.assertEquals(ScalarType.createTimeStampTzType(6),
                org.apache.doris.datasource.hudi.HudiUtils.fromAvroHudiTypeToDorisType(
                        org.apache.avro.LogicalTypes.timestampMicros()
                                .addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG))));
    }

    @Test
    public void testMaxComputeTimestampSemantics() throws Exception {
        org.apache.doris.datasource.maxcompute.MaxComputeExternalTable table = Mockito.mock(
                org.apache.doris.datasource.maxcompute.MaxComputeExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Method mapping = org.apache.doris.datasource.maxcompute.MaxComputeExternalTable.class.getDeclaredMethod(
                "mcTypeToDorisType", com.aliyun.odps.type.TypeInfo.class);
        mapping.setAccessible(true);
        Assert.assertEquals(ScalarType.createTimeStampTzType(6),
                mapping.invoke(table, com.aliyun.odps.type.TypeInfoFactory.TIMESTAMP));
        Assert.assertEquals(ScalarType.createDatetimeV2Type(6),
                mapping.invoke(table, com.aliyun.odps.type.TypeInfoFactory.TIMESTAMP_NTZ));
    }

    @Test
    public void testTrinoConnectorTimestampSemantics() throws Exception {
        TrinoConnectorExternalTable table = Mockito.mock(TrinoConnectorExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Method mapping = TrinoConnectorExternalTable.class.getDeclaredMethod(
                "trinoConnectorTypeToDorisType", io.trino.spi.type.Type.class);
        mapping.setAccessible(true);
        Assert.assertEquals(ScalarType.createTimeStampTzType(6), mapping.invoke(table,
                io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType(9)));
        Assert.assertEquals(ScalarType.createDatetimeV2Type(6), mapping.invoke(table,
                io.trino.spi.type.TimestampType.createTimestampType(9)));
    }

    @Test
    public void testHiveTimestampSemanticsIgnoreLegacyFlag() {
        for (boolean legacyFlag : new boolean[] {false, true}) {
            Assert.assertEquals(ScalarType.createDatetimeV2Type(6),
                    HiveMetaStoreClientHelper.hiveTypeToDorisType("timestamp", false, legacyFlag));
            Assert.assertEquals(ScalarType.createTimeStampTzType(6), HiveMetaStoreClientHelper.hiveTypeToDorisType(
                    "timestamp with local time zone", false, legacyFlag));
            StructType nested = (StructType) HiveMetaStoreClientHelper.hiveTypeToDorisType(
                    "struct<t:array<timestamp with local time zone>>", false, legacyFlag);
            Assert.assertEquals(ScalarType.createTimeStampTzType(6),
                    ((ArrayType) nested.getFields().get(0).getType()).getItemType());
        }
    }

    @Test
    public void testIcebergTimestampSemanticsIgnoreLegacyFlag() {
        for (boolean legacyFlag : new boolean[] {false, true}) {
            Assert.assertEquals(ScalarType.createDatetimeV2Type(6), IcebergUtils.icebergTypeToDorisType(
                    Types.TimestampType.withoutZone(), false, legacyFlag));
            Assert.assertEquals(ScalarType.createTimeStampTzType(6), IcebergUtils.icebergTypeToDorisType(
                    Types.TimestampType.withZone(), false, legacyFlag));
            ArrayType nested = (ArrayType) IcebergUtils.icebergTypeToDorisType(
                    Types.ListType.ofOptional(1, Types.TimestampType.withZone()), false, legacyFlag);
            Assert.assertEquals(ScalarType.createTimeStampTzType(6), nested.getItemType());
        }
    }

    @Test
    public void testPaimonTimestampPrecisionAndSemanticsIgnoreLegacyFlag() {
        for (boolean legacyFlag : new boolean[] {false, true}) {
            for (int precision : new int[] {0, 3, 6, 9}) {
                Assert.assertEquals(ScalarType.createDatetimeV2Type(Math.min(precision, 6)),
                        PaimonUtil.paimonTypeToDorisType(new TimestampType(precision), false, legacyFlag));
                Assert.assertEquals(ScalarType.createTimeStampTzType(Math.min(precision, 6)),
                        PaimonUtil.paimonTypeToDorisType(new LocalZonedTimestampType(precision), false, legacyFlag));
            }
        }
    }
}
