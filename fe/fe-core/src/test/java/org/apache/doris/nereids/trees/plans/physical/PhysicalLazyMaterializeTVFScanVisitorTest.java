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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.table.Local;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Guard the visitor dispatch of {@link PhysicalLazyMaterializeTVFScan}: it must reach the dedicated
 * visitor method instead of falling back to the {@link PhysicalTVFRelation} one, otherwise the
 * lazy-materialization translation of a TVF scan is unreachable dead code.
 */
public class PhysicalLazyMaterializeTVFScanVisitorTest {

    @Test
    public void acceptDispatchesToTheDedicatedVisitorMethod() {
        SlotReference value = new SlotReference("c1", BigIntType.INSTANCE, true);
        List<Slot> outputs = ImmutableList.of(value);
        LogicalProperties logicalProperties =
                new LogicalProperties(() -> outputs, () -> DataTrait.EMPTY_TRAIT);
        PhysicalTVFRelation tvf = new PhysicalTVFRelation(RelationId.createGenerator().getNextId(),
                new Local(new Properties(Collections.emptyMap())), ImmutableList.of(value), logicalProperties);
        SlotReference rowId = new SlotReference("__DORIS_GLOBAL_ROWID_COL__local",
                VarcharType.createVarcharType(-1), false);

        PhysicalLazyMaterializeTVFScan lazyScan =
                new PhysicalLazyMaterializeTVFScan(tvf, rowId, ImmutableList.of(value));

        String dispatched = lazyScan.accept(new PlanVisitor<String, Void>() {
            @Override
            public String visit(Plan plan, Void context) {
                return "generic";
            }

            @Override
            public String visitPhysicalTVFRelation(PhysicalTVFRelation tvfRelation, Void context) {
                return "parent";
            }

            @Override
            public String visitPhysicalLazyMaterializeTVFScan(PhysicalLazyMaterializeTVFScan scan, Void context) {
                return "lazy";
            }
        }, null);

        Assertions.assertEquals("lazy", dispatched);
    }
}
