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

package org.apache.doris.datasource.hudi;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class HudiUuidMappingTest {
    @Test
    public void testUuidSurvivesCatalogSchemaRoundTrip() {
        Schema uuid = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
        Schema nullableUuid = Schema.createUnion(Schema.create(Schema.Type.NULL), uuid);
        Schema record = Schema.createRecord("uuid_record", null, null, false);
        record.setFields(Arrays.asList(new Schema.Field("id", nullableUuid),
                new Schema.Field("ids", Schema.createArray(nullableUuid)),
                new Schema.Field("by_name", Schema.createMap(nullableUuid))));
        Schema normalized = AvroInternalSchemaConverter.convert(
                AvroInternalSchemaConverter.convert(record), "uuid_record");
        Assert.assertEquals(Schema.Type.FIXED, normalized.getField("id").schema().getTypes().get(1).getType());
        for (Schema schema : Arrays.asList(record, normalized)) {
            StructType mapped = (StructType) HudiUtils.fromAvroHudiTypeToDorisType(schema);
            Assert.assertEquals(Type.UUID, mapped.getFields().get(0).getType());
            Assert.assertEquals(ArrayType.create(Type.UUID, true), mapped.getFields().get(1).getType());
            Assert.assertEquals(new MapType(Type.STRING, Type.UUID), mapped.getFields().get(2).getType());
        }
    }

    @Test
    public void testNormalizedUuidUsesBinaryHiveCarrier() {
        Schema uuid = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
        Schema record = Schema.createRecord("uuid_record", null, null, false);
        record.setFields(Arrays.asList(new Schema.Field("id", uuid),
                new Schema.Field("ids", Schema.createArray(uuid)),
                new Schema.Field("by_name", Schema.createMap(uuid))));
        Schema normalized = AvroInternalSchemaConverter.convert(
                AvroInternalSchemaConverter.convert(record), "uuid_record");
        Assert.assertEquals("struct<id:binary,ids:array<binary>,by_name:map<string,binary>>",
                HudiUtils.convertAvroToHiveType(normalized));
        Assert.assertEquals("string", HudiUtils.convertAvroToHiveType(uuid));
    }

    @Test
    public void testNonUuidMappingsArePreserved() {
        for (Schema schema : Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.BYTES),
                Schema.createFixed("ordinary_fixed", null, null, 16))) {
            Assert.assertEquals(Type.STRING, HudiUtils.fromAvroHudiTypeToDorisType(schema));
            Assert.assertEquals("string", HudiUtils.convertAvroToHiveType(schema));
        }
        Schema decimal = LogicalTypes.decimal(10, 2).addToSchema(Schema.createFixed("amount", null, null, 8));
        Assert.assertEquals("decimal(10,2)", HudiUtils.convertAvroToHiveType(decimal));
        Assert.assertTrue(HudiUtils.fromAvroHudiTypeToDorisType(decimal).isDecimalV3());
    }
}
