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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.analysis.AddPartitionFieldClause;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class AddPartitionFieldOpTest {

    @Test
    public void testIdentityTransform() {
        AddPartitionFieldOp op = new AddPartitionFieldOp(null, null, "category");
        Assertions.assertEquals(AlterOpType.SCHEMA_CHANGE, op.getOpType());
        Assertions.assertNull(op.getTransformName());
        Assertions.assertNull(op.getTransformArg());
        Assertions.assertEquals("category", op.getColumnName());
        Assertions.assertEquals("ADD PARTITION KEY category", op.toSql());

        AddPartitionFieldClause clause = (AddPartitionFieldClause) op.translateToLegacyAlterClause();
        Assertions.assertNotNull(clause);
        Assertions.assertNull(clause.getTransformName());
        Assertions.assertNull(clause.getTransformArg());
        Assertions.assertEquals("category", clause.getColumnName());
    }

    @Test
    public void testTimeTransform() {
        AddPartitionFieldOp op = new AddPartitionFieldOp("year", null, "ts");
        Assertions.assertEquals("year", op.getTransformName());
        Assertions.assertNull(op.getTransformArg());
        Assertions.assertEquals("ts", op.getColumnName());
        Assertions.assertEquals("ADD PARTITION KEY year(ts)", op.toSql());

        AddPartitionFieldClause clause = (AddPartitionFieldClause) op.translateToLegacyAlterClause();
        Assertions.assertEquals("year", clause.getTransformName());
        Assertions.assertNull(clause.getTransformArg());
        Assertions.assertEquals("ts", clause.getColumnName());
    }

    @Test
    public void testBucketTransform() {
        AddPartitionFieldOp op = new AddPartitionFieldOp("bucket", 16, "id");
        Assertions.assertEquals("bucket", op.getTransformName());
        Assertions.assertEquals(16, op.getTransformArg());
        Assertions.assertEquals("id", op.getColumnName());
        Assertions.assertEquals("ADD PARTITION KEY bucket(16, id)", op.toSql());

        AddPartitionFieldClause clause = (AddPartitionFieldClause) op.translateToLegacyAlterClause();
        Assertions.assertEquals("bucket", clause.getTransformName());
        Assertions.assertEquals(16, clause.getTransformArg());
        Assertions.assertEquals("id", clause.getColumnName());
    }

    @Test
    public void testTruncateTransform() {
        AddPartitionFieldOp op = new AddPartitionFieldOp("truncate", 10, "name");
        Assertions.assertEquals("truncate", op.getTransformName());
        Assertions.assertEquals(10, op.getTransformArg());
        Assertions.assertEquals("name", op.getColumnName());
        Assertions.assertEquals("ADD PARTITION KEY truncate(10, name)", op.toSql());
    }

    @Test
    public void testMonthTransform() {
        AddPartitionFieldOp op = new AddPartitionFieldOp("month", null, "created_date");
        Assertions.assertEquals("ADD PARTITION KEY month(created_date)", op.toSql());
    }

    @Test
    public void testDayTransform() {
        AddPartitionFieldOp op = new AddPartitionFieldOp("day", null, "ts");
        Assertions.assertEquals("ADD PARTITION KEY day(ts)", op.toSql());
    }

    @Test
    public void testHourTransform() {
        AddPartitionFieldOp op = new AddPartitionFieldOp("hour", null, "ts");
        Assertions.assertEquals("ADD PARTITION KEY hour(ts)", op.toSql());
    }

    @Test
    public void testProperties() {
        AddPartitionFieldOp op = new AddPartitionFieldOp("bucket", 32, "id");
        Map<String, String> properties = op.getProperties();
        Assertions.assertNotNull(properties);
        Assertions.assertTrue(properties.isEmpty());
    }

    @Test
    public void testAllowOpMTMV() {
        AddPartitionFieldOp op = new AddPartitionFieldOp("bucket", 16, "id");
        Assertions.assertFalse(op.allowOpMTMV());
    }

    @Test
    public void testNeedChangeMTMVState() {
        AddPartitionFieldOp op = new AddPartitionFieldOp("bucket", 16, "id");
        Assertions.assertFalse(op.needChangeMTMVState());
    }
}

