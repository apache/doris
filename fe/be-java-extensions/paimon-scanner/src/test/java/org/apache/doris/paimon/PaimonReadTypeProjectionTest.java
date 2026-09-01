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

package org.apache.doris.paimon;

import org.apache.doris.common.jni.vec.ColumnType;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class PaimonReadTypeProjectionTest {
    @Test
    public void testProjectNestedRowArrayAndMap() {
        RowType profileType = new RowType(false, Arrays.asList(
                new DataField(2, "city", DataTypes.STRING(), "city description"),
                new DataField(3, "zip", DataTypes.INT())));
        RowType eventType = new RowType(false, Arrays.asList(
                new DataField(5, "score", DataTypes.INT()),
                new DataField(6, "detail", DataTypes.STRING())));
        RowType attributeType = new RowType(false, Arrays.asList(
                new DataField(8, "code", DataTypes.BIGINT()),
                new DataField(9, "detail", DataTypes.STRING())));
        RowType tableType = new RowType(false, Arrays.asList(
                new DataField(1, "profile", profileType, "profile description"),
                new DataField(4, "events", new ArrayType(false, eventType)),
                new DataField(7, "attributes",
                        new MapType(false, DataTypes.STRING(), attributeType))));
        ColumnType requiredType = ColumnType.parseType("root",
                "struct<PROFILE:struct<city:string>,events:array<struct<score:int>>,"
                        + "attributes:map<string,struct<code:bigint>>>");
        RowType projected = (RowType) PaimonReadTypeProjection.project(tableType, requiredType);
        Assert.assertEquals(Arrays.asList("profile", "events", "attributes"),
                projected.getFieldNames());
        DataField profile = projected.getFields().get(0);
        Assert.assertEquals(1, profile.id());
        Assert.assertEquals("profile description", profile.description());
        Assert.assertEquals(Arrays.asList("city"),
                ((RowType) profile.type()).getFieldNames());
        Assert.assertEquals(Arrays.asList("score"),
                ((RowType) ((ArrayType) projected.getTypeAt(1)).getElementType()).getFieldNames());
        Assert.assertEquals(Arrays.asList("code"),
                ((RowType) ((MapType) projected.getTypeAt(2)).getValueType()).getFieldNames());
    }

    @Test
    public void testRejectMissingNestedField() {
        RowType tableType = new RowType(Arrays.asList(
                new DataField(1, "known", DataTypes.INT())));
        ColumnType requiredType = ColumnType.parseType("root", "struct<missing:int>");
        IllegalArgumentException exception = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonReadTypeProjection.project(tableType, requiredType));
        Assert.assertTrue(exception.getMessage().contains("missing"));
    }
}
