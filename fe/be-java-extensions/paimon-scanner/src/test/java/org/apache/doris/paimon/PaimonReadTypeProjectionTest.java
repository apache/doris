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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class PaimonReadTypeProjectionTest {

    @Test
    public void projectNestedRowArrayAndMapKeepingPaimonIdentity() {
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
        // Upper case on purpose: the JNI type string does not preserve the source spelling.
        String requestedType = "struct<PROFILE:struct<city:string>,"
                + "events:array<struct<score:int>>,"
                + "attributes:map<string,struct<code:bigint>>>";
        ColumnType requiredType = ColumnType.parseType("root", requestedType);

        RowType projected = (RowType) PaimonReadTypeProjection.project(tableType, requiredType);

        Assert.assertFalse(projected.isNullable());
        Assert.assertEquals(Arrays.asList("profile", "events", "attributes"), projected.getFieldNames());

        DataField profile = projected.getFields().get(0);
        Assert.assertEquals(1, profile.id());
        Assert.assertEquals("profile description", profile.description());
        RowType projectedProfile = (RowType) profile.type();
        Assert.assertFalse(projectedProfile.isNullable());
        Assert.assertEquals(Arrays.asList("city"), projectedProfile.getFieldNames());
        Assert.assertEquals(2, projectedProfile.getFields().get(0).id());
        Assert.assertEquals("city description", projectedProfile.getFields().get(0).description());

        ArrayType projectedEvents = (ArrayType) projected.getTypeAt(1);
        Assert.assertFalse(projectedEvents.isNullable());
        Assert.assertEquals(Arrays.asList("score"),
                ((RowType) projectedEvents.getElementType()).getFieldNames());

        MapType projectedAttributes = (MapType) projected.getTypeAt(2);
        Assert.assertFalse(projectedAttributes.isNullable());
        Assert.assertEquals(DataTypes.STRING(), projectedAttributes.getKeyType());
        Assert.assertEquals(Arrays.asList("code"),
                ((RowType) projectedAttributes.getValueType()).getFieldNames());
    }

    @Test
    public void rejectMissingNestedField() {
        RowType tableType = new RowType(Arrays.asList(new DataField(1, "known", DataTypes.INT())));
        ColumnType requiredType = ColumnType.parseType("root", "struct<missing:int>");

        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class,
                () -> PaimonReadTypeProjection.project(tableType, requiredType));
        Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("missing"));
    }

    @Test
    public void unprunedColumnReturnsTheTableTypeItself() {
        // The scanner decides between withReadType and withProjection by comparing against the table
        // type, so an unpruned column must come back identical -- not merely equal-looking.
        RowType tableType = new RowType(Arrays.asList(
                new DataField(1, "a", DataTypes.INT()),
                new DataField(2, "b", DataTypes.STRING())));
        ColumnType requiredType = ColumnType.parseType("root", "struct<a:int,b:string>");

        Assert.assertSame(tableType, PaimonReadTypeProjection.project(tableType, requiredType));
    }

    @Test
    public void scalarColumnIsReturnedUntouched() {
        // Doris DATETIMEV2 scale and paimon TIMESTAMP precision do not have to agree; the paimon side
        // must win, or the reader decodes with a precision the file was not written at.
        DataType tableType = DataTypes.TIMESTAMP(9);

        Assert.assertSame(tableType, PaimonReadTypeProjection.project(tableType,
                ColumnType.parseType("root", "datetimev2(6)")));
    }

    @Test
    public void projectNonFirstFieldKeepsItsSourceId() {
        // The requested field must be located by its OWN position in the source, not by its position in
        // the request -- a table where the wanted field is not first is what tells the two apart. A
        // rebuild that resolved by request position would silently return "city" (source index 0) here.
        RowType tableType = new RowType(Arrays.asList(
                new DataField(2, "city", DataTypes.STRING()),
                new DataField(3, "zip", DataTypes.INT())));
        ColumnType requiredType = ColumnType.parseType("root", "struct<zip:int>");

        RowType projected = (RowType) PaimonReadTypeProjection.project(tableType, requiredType);

        Assert.assertEquals(Arrays.asList("zip"), projected.getFieldNames());
        Assert.assertEquals(3, projected.getFields().get(0).id());
        Assert.assertEquals("zip", projected.getFields().get(0).name());
    }

    @Test
    public void projectReorderedFieldsKeepPerFieldSourceId() {
        // Requesting fields out of source order must reorder the output to match the request while each
        // field keeps its OWN source id -- pins down that the rebuild truly reorders rather than just
        // subsetting a prefix, which the non-first-field test alone would not catch.
        RowType tableType = new RowType(Arrays.asList(
                new DataField(2, "city", DataTypes.STRING()),
                new DataField(3, "zip", DataTypes.INT())));
        ColumnType requiredType = ColumnType.parseType("root", "struct<zip:int,city:string>");

        RowType projected = (RowType) PaimonReadTypeProjection.project(tableType, requiredType);

        Assert.assertEquals(Arrays.asList("zip", "city"), projected.getFieldNames());
        Assert.assertEquals(3, projected.getFields().get(0).id());
        Assert.assertEquals("zip", projected.getFields().get(0).name());
        Assert.assertEquals(2, projected.getFields().get(1).id());
        Assert.assertEquals("city", projected.getFields().get(1).name());
    }
}
