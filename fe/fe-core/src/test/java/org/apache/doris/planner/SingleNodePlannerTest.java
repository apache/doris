/*
 * // Licensed to the Apache Software Foundation (ASF) under one
 * // or more contributor license agreements.  See the NOTICE file
 * // distributed with this work for additional information
 * // regarding copyright ownership.  The ASF licenses this file
 * // to you under the Apache License, Version 2.0 (the
 * // "License"); you may not use this file except in compliance
 * // with the License.  You may obtain a copy of the License at
 * //
 * //   http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing,
 * // software distributed under the License is distributed on an
 * // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * // KIND, either express or implied.  See the License for the
 * // specific language governing permissions and limitations
 * // under the License.
 *
 */

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.jmockit.Deencapsulation;

import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.List;

import mockit.Expectations;
import mockit.Injectable;

public class SingleNodePlannerTest {

    @Test
    public void testMaterializeBaseTableRefResultForCrossJoinOrCountStar(@Injectable Table table,
                                                                         @Injectable TableName tableName,
                                                                         @Injectable Analyzer analyzer,
                                                                         @Injectable PlannerContext plannerContext,
                                                                         @Injectable Column column) {
        TableRef tableRef = new TableRef();
        Deencapsulation.setField(tableRef, "isAnalyzed", true);
        BaseTableRef baseTableRef = new BaseTableRef(tableRef, table, tableName);
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(1));
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(1), tupleDescriptor);
        slotDescriptor.setIsMaterialized(false);
        tupleDescriptor.addSlot(slotDescriptor);
        Deencapsulation.setField(tableRef, "desc", tupleDescriptor);
        Deencapsulation.setField(baseTableRef, "desc", tupleDescriptor);
        tupleDescriptor.setTable(table);
        List<Column> columnList = Lists.newArrayList();
        columnList.add(column);
        new Expectations() {
            {
                table.getBaseSchema();
                result = columnList;
            }
        };
        SingleNodePlanner singleNodePlanner = new SingleNodePlanner(plannerContext);
        Deencapsulation.invoke(singleNodePlanner, "materializeSlotForEmptyMaterializedTableRef",
                               baseTableRef, analyzer);
    }
}
