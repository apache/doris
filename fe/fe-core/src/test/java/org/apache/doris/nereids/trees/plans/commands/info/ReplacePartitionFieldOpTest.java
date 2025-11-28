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
import org.apache.doris.analysis.ReplacePartitionFieldClause;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ReplacePartitionFieldOpTest {

    @Test
    public void testReplaceTimeTransform() {
        ReplacePartitionFieldOp op = new ReplacePartitionFieldOp("ts_year", null, null, null,
                "month", null, "ts", null);
        Assertions.assertEquals(AlterOpType.REPLACE_PARTITION_FIELD, op.getOpType());
        Assertions.assertEquals("ts_year", op.getOldPartitionFieldName());
        Assertions.assertEquals("month", op.getNewTransformName());
        Assertions.assertEquals("ts", op.getNewColumnName());
        Assertions.assertNull(op.getNewPartitionFieldName());
        Assertions.assertEquals("REPLACE PARTITION KEY ts_year WITH month(ts)", op.toSql());
    }

    @Test
    public void testReplaceTimeTransformWithCustomName() {
        ReplacePartitionFieldOp op = new ReplacePartitionFieldOp("ts_year", null, null, null,
                "month", null, "ts", "ts_month");
        Assertions.assertEquals("ts_year", op.getOldPartitionFieldName());
        Assertions.assertEquals("month", op.getNewTransformName());
        Assertions.assertEquals("ts", op.getNewColumnName());
        Assertions.assertEquals("ts_month", op.getNewPartitionFieldName());
        Assertions.assertEquals("REPLACE PARTITION KEY ts_year WITH month(ts) AS ts_month", op.toSql());

        ReplacePartitionFieldClause clause = (ReplacePartitionFieldClause) op.translateToLegacyAlterClause();
        Assertions.assertNotNull(clause);
        Assertions.assertEquals("ts_year", clause.getOldPartitionFieldName());
        Assertions.assertNull(clause.getOldTransformName());
        Assertions.assertEquals("month", clause.getNewTransformName());
    }

    @Test
    public void testReplaceBucketTransform() {
        ReplacePartitionFieldOp op = new ReplacePartitionFieldOp("id_bucket_10", null, null, null,
                "bucket", 16, "id", null);
        Assertions.assertEquals("id_bucket_10", op.getOldPartitionFieldName());
        Assertions.assertEquals("bucket", op.getNewTransformName());
        Assertions.assertEquals(16, op.getNewTransformArg());
        Assertions.assertEquals("id", op.getNewColumnName());
        Assertions.assertEquals("REPLACE PARTITION KEY id_bucket_10 WITH bucket(16, id)", op.toSql());
    }

    @Test
    public void testReplaceBucketTransformWithCustomName() {
        ReplacePartitionFieldOp op = new ReplacePartitionFieldOp("id_bucket_10", null, null, null,
                "bucket", 16, "id", "id_bucket_16");
        Assertions.assertEquals("id_bucket_10", op.getOldPartitionFieldName());
        Assertions.assertEquals("id_bucket_16", op.getNewPartitionFieldName());
        Assertions.assertEquals("REPLACE PARTITION KEY id_bucket_10 WITH bucket(16, id) AS id_bucket_16", op.toSql());
    }

    @Test
    public void testReplaceIdentityToTransform() {
        ReplacePartitionFieldOp op = new ReplacePartitionFieldOp("category", null, null, null,
                "bucket", 8, "id", null);
        Assertions.assertEquals("category", op.getOldPartitionFieldName());
        Assertions.assertEquals("bucket", op.getNewTransformName());
        Assertions.assertEquals(8, op.getNewTransformArg());
        Assertions.assertEquals("REPLACE PARTITION KEY category WITH bucket(8, id)", op.toSql());
    }

    @Test
    public void testReplaceIdentityToTransformWithCustomName() {
        ReplacePartitionFieldOp op = new ReplacePartitionFieldOp("category", null, null, null,
                "bucket", 8, "id", "id_bucket");
        Assertions.assertEquals("category", op.getOldPartitionFieldName());
        Assertions.assertEquals("id_bucket", op.getNewPartitionFieldName());
        Assertions.assertEquals("REPLACE PARTITION KEY category WITH bucket(8, id) AS id_bucket", op.toSql());
    }

    @Test
    public void testReplaceTransformToIdentity() {
        ReplacePartitionFieldOp op = new ReplacePartitionFieldOp("ts_year", null, null, null,
                null, null, "category", null);
        Assertions.assertEquals("ts_year", op.getOldPartitionFieldName());
        Assertions.assertNull(op.getNewTransformName());
        Assertions.assertEquals("category", op.getNewColumnName());
        Assertions.assertEquals("REPLACE PARTITION KEY ts_year WITH category", op.toSql());
    }

    @Test
    public void testReplaceTransformToIdentityWithCustomName() {
        ReplacePartitionFieldOp op = new ReplacePartitionFieldOp("ts_year", null, null, null,
                null, null, "category", "category_partition");
        Assertions.assertEquals("ts_year", op.getOldPartitionFieldName());
        Assertions.assertEquals("category_partition", op.getNewPartitionFieldName());
        Assertions.assertEquals("REPLACE PARTITION KEY ts_year WITH category AS category_partition", op.toSql());
    }

    @Test
    public void testReplaceByOldTransformExpression() {
        ReplacePartitionFieldOp op = new ReplacePartitionFieldOp(null, "bucket", 16, "id",
                "truncate", 5, "code", "code_trunc");
        Assertions.assertNull(op.getOldPartitionFieldName());
        Assertions.assertEquals("bucket", op.getOldTransformName());
        Assertions.assertEquals(16, op.getOldTransformArg());
        Assertions.assertEquals("id", op.getOldColumnName());
        Assertions.assertEquals("REPLACE PARTITION KEY bucket(16, id) WITH truncate(5, code) AS code_trunc", op.toSql());

        ReplacePartitionFieldClause clause = (ReplacePartitionFieldClause) op.translateToLegacyAlterClause();
        Assertions.assertNull(clause.getOldPartitionFieldName());
        Assertions.assertEquals("bucket", clause.getOldTransformName());
        Assertions.assertEquals(16, clause.getOldTransformArg());
        Assertions.assertEquals("id", clause.getOldColumnName());
    }

    @Test
    public void testProperties() {
        ReplacePartitionFieldOp op = new ReplacePartitionFieldOp("id_bucket_10", null, null, null,
                "bucket", 16, "id", null);
        Map<String, String> properties = op.getProperties();
        Assertions.assertNotNull(properties);
        Assertions.assertTrue(properties.isEmpty());
    }

    @Test
    public void testAllowOpMTMV() {
        ReplacePartitionFieldOp op = new ReplacePartitionFieldOp("ts_year", null, null, null,
                "month", null, "ts", null);
        Assertions.assertFalse(op.allowOpMTMV());
    }

    @Test
    public void testNeedChangeMTMVState() {
        ReplacePartitionFieldOp op = new ReplacePartitionFieldOp("ts_year", null, null, null,
                "month", null, "ts", null);
        Assertions.assertFalse(op.needChangeMTMVState());
    }
}
