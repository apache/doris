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

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CreateTableInfoTest {

    @Test
    public void testCheckLegalityOfPartitionExprs() {
        UnboundSlot slot1 = new UnboundSlot("col1");
        UnboundSlot slot2 = new UnboundSlot("col1");
        List<Expression> innerExprs = Lists.newArrayList();
        innerExprs.add(new EqualTo(slot1, slot2));
        UnboundFunction unboundFunction = new UnboundFunction("test_func", innerExprs);

        List<Expression> partitionFields = new ArrayList<>();
        partitionFields.add(unboundFunction);
        PartitionTableInfo partitionTableInfo1 = new PartitionTableInfo(false, null, new ArrayList<>(), partitionFields);
        CreateTableInfo createTableInfo = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo1, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo.checkLegalityOfPartitionExprs(partitionTableInfo1),
                "only Auto Range Partition support UnboundFunction");

        PartitionTableInfo partitionTableInfo2 = new PartitionTableInfo(true, "RANGE", new ArrayList<>(), partitionFields);
        CreateTableInfo createTableInfo2 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo2, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo2.checkLegalityOfPartitionExprs(partitionTableInfo2),
                "partition expression test_func has unrecognized parameter in slot 0");

        List<Expression> innerExprs2 = Lists.newArrayList();
        innerExprs2.add(slot1);
        innerExprs2.add(slot2);
        UnboundFunction unboundFunction2 = new UnboundFunction("test_func", innerExprs2);
        List<Expression> partitionFields2 = new ArrayList<>();
        partitionFields2.add(unboundFunction2);
        PartitionTableInfo partitionTableInfo3 = new PartitionTableInfo(true, "RANGE", new ArrayList<>(), partitionFields2);
        CreateTableInfo createTableInfo3 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo3, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertDoesNotThrow(() -> createTableInfo3.checkLegalityOfPartitionExprs(partitionTableInfo3));

        List<Expression> partitionFields3 = new ArrayList<>();
        partitionFields3.add(slot1);
        PartitionTableInfo partitionTableInfo4 = new PartitionTableInfo(true, "RANGE", new ArrayList<>(), partitionFields3);
        CreateTableInfo createTableInfo4 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo4, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo4.checkLegalityOfPartitionExprs(partitionTableInfo4),
                "Auto Range Partition need UnboundFunction");

        PartitionTableInfo partitionTableInfo5 = new PartitionTableInfo(false, "RANGE", new ArrayList<>(), partitionFields3);
        CreateTableInfo createTableInfo5 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo5, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertDoesNotThrow(() -> createTableInfo5.checkLegalityOfPartitionExprs(partitionTableInfo5));

        List<Expression> partitionFields4 = new ArrayList<>();
        partitionFields4.add(new StringLiteral("test"));
        PartitionTableInfo partitionTableInfo6 = new PartitionTableInfo(true, "RANGE", new ArrayList<>(), partitionFields4);
        CreateTableInfo createTableInfo6 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo6, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo6.checkLegalityOfPartitionExprs(partitionTableInfo6),
                "partition expression literal is illegal!");
    }

    @Test
    public void testCheckPartitionNullity1() {
        List<ColumnDefinition> columnDefs = new ArrayList<>();
        //isNullable == true
        ColumnDefinition columnDef = new ColumnDefinition("col1", null, false, null, true, null, null);
        columnDefs.add(columnDef);
        UnboundSlot slot = new UnboundSlot("col2");
        List<Expression> partitionFields = new ArrayList<>();
        partitionFields.add(slot);
        PartitionTableInfo partitionTableInfo = new PartitionTableInfo(false, "RANGE", new ArrayList<>(), partitionFields);
        CreateTableInfo createTableInfo = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo.checkPartitionNullity(columnDefs, partitionTableInfo),
                "Unknown partition column name:col2");

        //partitionDefs is empty
        UnboundSlot slot2 = new UnboundSlot("col1");
        List<Expression> partitionFields2 = new ArrayList<>();
        partitionFields2.add(slot2);
        PartitionTableInfo partitionTableInfo2 = new PartitionTableInfo(false, "RANGE", new ArrayList<>(), partitionFields2);
        CreateTableInfo createTableInfo2 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo2, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertDoesNotThrow(() -> createTableInfo2.checkPartitionNullity(columnDefs, partitionTableInfo2));
    }

    /**
     * partitionDef instance of InPartition
     */
    @Test
    public void testCheckPartitionNullity2() {
        List<ColumnDefinition> columnDefs = new ArrayList<>();
        //isNullable == true
        ColumnDefinition columnDef = new ColumnDefinition("col1", null, false, null, true, null, null);
        columnDefs.add(columnDef);
        List<PartitionDefinition> partitionDefs = new ArrayList<>();
        String partName = "col1";
        List<List<Expression>> values = new ArrayList<>();
        List<Expression> innerValues = new ArrayList<>();
        values.add(innerValues);
        StringLiteral expr = new StringLiteral("col1");
        innerValues.add(expr);
        PartitionDefinition inPartition = new InPartition(true, partName, values);
        partitionDefs.add(inPartition);
        UnboundSlot slot = new UnboundSlot("col1");
        List<Expression> partitionFields = new ArrayList<>();
        partitionFields.add(slot);
        PartitionTableInfo partitionTableInfo = new PartitionTableInfo(false, "RANGE", partitionDefs, partitionFields);
        CreateTableInfo createTableInfo = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertDoesNotThrow(() -> createTableInfo.checkPartitionNullity(columnDefs, partitionTableInfo));

        List<ColumnDefinition> columnDefs2 = new ArrayList<>();
        //isNullable == false
        ColumnDefinition columnDef2 = new ColumnDefinition("col1", null, false, null, false, null, null);
        columnDefs2.add(columnDef2);
        List<List<Expression>> values2 = new ArrayList<>();
        List<Expression> innerValues2 = new ArrayList<>();
        values2.add(innerValues2);
        NullLiteral expr2 = new NullLiteral();
        innerValues2.add(expr2);
        PartitionDefinition inPartition2 = new InPartition(true, partName, values2);
        List<PartitionDefinition> partitionDefs2 = new ArrayList<>();
        partitionDefs2.add(inPartition2);
        PartitionTableInfo partitionTableInfo2 = new PartitionTableInfo(false, "RANGE", partitionDefs2, partitionFields);
        CreateTableInfo createTableInfo2 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo2, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo2.checkPartitionNullity(columnDefs2, partitionTableInfo2),
                "Can't have null partition is for NOT NULL partition column in partition expr's index 0");
    }

    /**
     * partitionDef instance of LessThanPartition
     */
    @Test
    public void testCheckPartitionNullity3() {
        List<ColumnDefinition> columnDefs = new ArrayList<>();
        //isNullable == true
        ColumnDefinition columnDef = new ColumnDefinition("col1", null, false, null, true, null, null);
        columnDefs.add(columnDef);
        List<PartitionDefinition> partitionDefs = new ArrayList<>();
        String partName = "col1";
        List<Expression> values = new ArrayList<>();
        StringLiteral expr = new StringLiteral("col1");
        values.add(expr);
        PartitionDefinition lessThanPartition = new LessThanPartition(true, partName, values);
        partitionDefs.add(lessThanPartition);
        UnboundSlot slot = new UnboundSlot("col1");
        List<Expression> partitionFields = new ArrayList<>();
        partitionFields.add(slot);
        PartitionTableInfo partitionTableInfo = new PartitionTableInfo(false, "RANGE", partitionDefs, partitionFields);
        CreateTableInfo createTableInfo = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertDoesNotThrow(() -> createTableInfo.checkPartitionNullity(columnDefs, partitionTableInfo));

        List<ColumnDefinition> columnDefs2 = new ArrayList<>();
        //isNullable == false
        ColumnDefinition columnDef2 = new ColumnDefinition("col1", null, false, null, false, null, null);
        columnDefs2.add(columnDef2);
        List<Expression> values2 = new ArrayList<>();
        NullLiteral expr2 = new NullLiteral();
        values2.add(expr2);
        PartitionDefinition lessThanPartition2 = new LessThanPartition(true, partName, values2);
        List<PartitionDefinition> partitionDefs2 = new ArrayList<>();
        partitionDefs2.add(lessThanPartition2);
        PartitionTableInfo partitionTableInfo2 = new PartitionTableInfo(false, "RANGE", partitionDefs2, partitionFields);
        CreateTableInfo createTableInfo2 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo2, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo2.checkPartitionNullity(columnDefs2, partitionTableInfo2),
                "Can't have null partition is for NOT NULL partition column in partition expr's index 0");
    }


    /**
     * partitionDef instance of FixedRangePartition
     */
    @Test
    public void testCheckPartitionNullity4() {
        List<ColumnDefinition> columnDefs = new ArrayList<>();
        //isNullable == true
        ColumnDefinition columnDef = new ColumnDefinition("col1", null, false, null, true, null, null);
        columnDefs.add(columnDef);
        List<PartitionDefinition> partitionDefs = new ArrayList<>();
        String partName = "col1";
        List<Expression> lowValues = new ArrayList<>();
        StringLiteral lowExpr = new StringLiteral("col1");
        lowValues.add(lowExpr);

        List<Expression> upperValues = new ArrayList<>();
        StringLiteral upperExpr = new StringLiteral("col1");
        upperValues.add(upperExpr);

        PartitionDefinition fixedRangePartition = new FixedRangePartition(true, partName, lowValues, upperValues);
        partitionDefs.add(fixedRangePartition);
        UnboundSlot slot = new UnboundSlot("col1");
        List<Expression> partitionFields = new ArrayList<>();
        partitionFields.add(slot);
        PartitionTableInfo partitionTableInfo = new PartitionTableInfo(false, "RANGE", partitionDefs, partitionFields);
        CreateTableInfo createTableInfo = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertDoesNotThrow(() -> createTableInfo.checkPartitionNullity(columnDefs, partitionTableInfo));

        List<ColumnDefinition> columnDefs2 = new ArrayList<>();
        //isNullable == false
        ColumnDefinition columnDef2 = new ColumnDefinition("col1", null, false, null, false, null, null);
        columnDefs2.add(columnDef2);
        List<Expression> lowValues2 = new ArrayList<>();
        NullLiteral lowExpr2 = new NullLiteral();
        lowValues2.add(lowExpr2);

        List<Expression> upperValues2 = new ArrayList<>();
        NullLiteral upperExpr2 = new NullLiteral();
        upperValues2.add(upperExpr2);

        PartitionDefinition fixedRangePartition2 = new FixedRangePartition(true, partName, lowValues2, upperValues2);
        List<PartitionDefinition> partitionDefs2 = new ArrayList<>();
        partitionDefs2.add(fixedRangePartition2);
        PartitionTableInfo partitionTableInfo2 = new PartitionTableInfo(false, "RANGE", partitionDefs2, partitionFields);
        CreateTableInfo createTableInfo2 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo2, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo2.checkPartitionNullity(columnDefs2, partitionTableInfo2),
                "Can't have null partition is for NOT NULL partition column in partition expr's index 0");
    }

    /**
     * partitionDef instance of StepPartition
     */
    @Test
    public void testCheckPartitionNullity5() {
        List<ColumnDefinition> columnDefs = new ArrayList<>();
        //isNullable == true
        ColumnDefinition columnDef = new ColumnDefinition("col1", null, false, null, true, null, null);
        columnDefs.add(columnDef);
        List<PartitionDefinition> partitionDefs = new ArrayList<>();
        String partName = "col1";
        List<Expression> fromValues = new ArrayList<>();
        StringLiteral fromExpr = new StringLiteral("col1");
        fromValues.add(fromExpr);

        List<Expression> toValues = new ArrayList<>();
        StringLiteral toExpr = new StringLiteral("col1");
        toValues.add(toExpr);

        PartitionDefinition stepPartition = new StepPartition(true, partName, fromValues, toValues, 1, null);
        partitionDefs.add(stepPartition);
        UnboundSlot slot = new UnboundSlot("col1");
        List<Expression> partitionFields = new ArrayList<>();
        partitionFields.add(slot);
        PartitionTableInfo partitionTableInfo = new PartitionTableInfo(false, "RANGE", partitionDefs, partitionFields);
        CreateTableInfo createTableInfo = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertDoesNotThrow(() -> createTableInfo.checkPartitionNullity(columnDefs, partitionTableInfo));

        List<ColumnDefinition> columnDefs2 = new ArrayList<>();
        //isNullable == false
        ColumnDefinition columnDef2 = new ColumnDefinition("col1", null, false, null, false, null, null);
        columnDefs2.add(columnDef2);
        List<Expression> fromValues2 = new ArrayList<>();
        NullLiteral fromExpr2 = new NullLiteral();
        fromValues2.add(fromExpr2);

        List<Expression> toValues2 = new ArrayList<>();
        NullLiteral toExpr2 = new NullLiteral();
        toValues2.add(toExpr2);

        PartitionDefinition stepPartition2 = new StepPartition(true, partName, fromValues2, toValues2, 1, null);
        List<PartitionDefinition> partitionDefs2 = new ArrayList<>();
        partitionDefs2.add(stepPartition2);
        PartitionTableInfo partitionTableInfo2 = new PartitionTableInfo(false, "RANGE", partitionDefs2, partitionFields);
        CreateTableInfo createTableInfo2 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo2, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo2.checkPartitionNullity(columnDefs2, partitionTableInfo2),
                "Can't have null partition is for NOT NULL partition column in partition expr's index 0");
    }
}
