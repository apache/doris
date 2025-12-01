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
import org.apache.doris.analysis.DropPartitionFieldClause;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class DropPartitionFieldOpTest {

    @Test
    public void testDropPartitionFieldByName() {
        DropPartitionFieldOp op = new DropPartitionFieldOp("ts_year");
        Assertions.assertEquals(AlterOpType.DROP_PARTITION_FIELD, op.getOpType());
        Assertions.assertEquals("ts_year", op.getPartitionFieldName());
        Assertions.assertNull(op.getTransformName());
        Assertions.assertEquals("DROP PARTITION KEY ts_year", op.toSql());

        DropPartitionFieldClause clause = (DropPartitionFieldClause) op.translateToLegacyAlterClause();
        Assertions.assertNotNull(clause);
        Assertions.assertEquals("ts_year", clause.getPartitionFieldName());
        Assertions.assertNull(clause.getTransformName());
    }

    @Test
    public void testDropPartitionFieldByTransform() {
        DropPartitionFieldOp op = new DropPartitionFieldOp("bucket", 16, "id");
        Assertions.assertEquals(AlterOpType.DROP_PARTITION_FIELD, op.getOpType());
        Assertions.assertNull(op.getPartitionFieldName());
        Assertions.assertEquals("bucket", op.getTransformName());
        Assertions.assertEquals(16, op.getTransformArg());
        Assertions.assertEquals("id", op.getColumnName());
        Assertions.assertEquals("DROP PARTITION KEY bucket(16, id)", op.toSql());

        DropPartitionFieldClause clause = (DropPartitionFieldClause) op.translateToLegacyAlterClause();
        Assertions.assertNotNull(clause);
        Assertions.assertNull(clause.getPartitionFieldName());
        Assertions.assertEquals("bucket", clause.getTransformName());
        Assertions.assertEquals(16, clause.getTransformArg());
        Assertions.assertEquals("id", clause.getColumnName());
    }

    @Test
    public void testProperties() {
        DropPartitionFieldOp op = new DropPartitionFieldOp("id_bucket_16");
        Map<String, String> properties = op.getProperties();
        Assertions.assertNotNull(properties);
        Assertions.assertTrue(properties.isEmpty());
    }

    @Test
    public void testAllowOpMTMV() {
        DropPartitionFieldOp op = new DropPartitionFieldOp("id_bucket_16");
        Assertions.assertFalse(op.allowOpMTMV());
    }

    @Test
    public void testNeedChangeMTMVState() {
        DropPartitionFieldOp op = new DropPartitionFieldOp("id_bucket_16");
        Assertions.assertFalse(op.needChangeMTMVState());
    }
}

