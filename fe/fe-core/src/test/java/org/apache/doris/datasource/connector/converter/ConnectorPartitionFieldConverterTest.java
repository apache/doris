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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.connector.api.ddl.PartitionFieldChange;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplacePartitionFieldOp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConnectorPartitionFieldConverter}: every nereids op field must land on the right neutral
 * carrier field (Rule 9). Add/drop populate only the primary field; replace also maps the {@code new*} side to the
 * primary field and the {@code old*} side to the old field.
 */
public class ConnectorPartitionFieldConverterTest {

    @Test
    public void testAddCopiesPrimaryFieldAndLeavesOldNull() {
        PartitionFieldChange c = ConnectorPartitionFieldConverter.toAddChange(
                new AddPartitionFieldOp("bucket", 8, "id", "id_b"));
        Assertions.assertEquals("bucket", c.getTransformName());
        Assertions.assertEquals(8, c.getTransformArg().intValue());
        Assertions.assertEquals("id", c.getColumnName());
        Assertions.assertEquals("id_b", c.getPartitionFieldName());
        // The old side belongs to replace only.
        Assertions.assertNull(c.getOldPartitionFieldName());
        Assertions.assertNull(c.getOldTransformName());
        Assertions.assertNull(c.getOldTransformArg());
        Assertions.assertNull(c.getOldColumnName());
    }

    @Test
    public void testAddIdentityKeepsNullTransform() {
        // Identity transform: a null transform name is preserved (the connector reads it as Expressions.ref).
        PartitionFieldChange c = ConnectorPartitionFieldConverter.toAddChange(
                new AddPartitionFieldOp(null, null, "id", null));
        Assertions.assertNull(c.getTransformName());
        Assertions.assertNull(c.getTransformArg());
        Assertions.assertEquals("id", c.getColumnName());
        Assertions.assertNull(c.getPartitionFieldName());
    }

    @Test
    public void testDropByNameCopiesFieldName() {
        PartitionFieldChange c = ConnectorPartitionFieldConverter.toDropChange(
                new DropPartitionFieldOp("p_id"));
        Assertions.assertEquals("p_id", c.getPartitionFieldName());
        Assertions.assertNull(c.getTransformName());
        Assertions.assertNull(c.getColumnName());
    }

    @Test
    public void testDropByTransformCopiesTriple() {
        PartitionFieldChange c = ConnectorPartitionFieldConverter.toDropChange(
                new DropPartitionFieldOp("bucket", 8, "id"));
        Assertions.assertNull(c.getPartitionFieldName());
        Assertions.assertEquals("bucket", c.getTransformName());
        Assertions.assertEquals(8, c.getTransformArg().intValue());
        Assertions.assertEquals("id", c.getColumnName());
    }

    @Test
    public void testReplaceMapsNewToPrimaryAndOldToOldSide() {
        // OLD identified by name "p"; NEW is bucket(4) on id aliased "p2".
        PartitionFieldChange c = ConnectorPartitionFieldConverter.toReplaceChange(
                new ReplacePartitionFieldOp("p", null, null, null, "bucket", 4, "id", "p2"));
        // new* -> primary
        Assertions.assertEquals("bucket", c.getTransformName());
        Assertions.assertEquals(4, c.getTransformArg().intValue());
        Assertions.assertEquals("id", c.getColumnName());
        Assertions.assertEquals("p2", c.getPartitionFieldName());
        // old* -> old side
        Assertions.assertEquals("p", c.getOldPartitionFieldName());
        Assertions.assertNull(c.getOldTransformName());
        Assertions.assertNull(c.getOldColumnName());
    }

    @Test
    public void testReplaceMapsOldIdentityTransform() {
        // OLD identified by an identity transform (oldTransformName null, oldColumnName non-null); NEW is year on ts.
        PartitionFieldChange c = ConnectorPartitionFieldConverter.toReplaceChange(
                new ReplacePartitionFieldOp(null, null, null, "id", "year", null, "ts", "ty"));
        // new* -> primary
        Assertions.assertEquals("year", c.getTransformName());
        Assertions.assertEquals("ts", c.getColumnName());
        Assertions.assertEquals("ty", c.getPartitionFieldName());
        // old* -> old side: identity means name null + transformName null, only the column is carried.
        Assertions.assertNull(c.getOldPartitionFieldName());
        Assertions.assertNull(c.getOldTransformName());
        Assertions.assertNull(c.getOldTransformArg());
        Assertions.assertEquals("id", c.getOldColumnName());
    }

    @Test
    public void testReplaceMapsOldTransformTriple() {
        // OLD identified by transform bucket(8) on id; NEW is truncate(4) on name.
        PartitionFieldChange c = ConnectorPartitionFieldConverter.toReplaceChange(
                new ReplacePartitionFieldOp(null, "bucket", 8, "id", "truncate", 4, "name", null));
        Assertions.assertEquals("truncate", c.getTransformName());
        Assertions.assertEquals(4, c.getTransformArg().intValue());
        Assertions.assertEquals("name", c.getColumnName());
        Assertions.assertNull(c.getPartitionFieldName());
        Assertions.assertNull(c.getOldPartitionFieldName());
        Assertions.assertEquals("bucket", c.getOldTransformName());
        Assertions.assertEquals(8, c.getOldTransformArg().intValue());
        Assertions.assertEquals("id", c.getOldColumnName());
    }
}
