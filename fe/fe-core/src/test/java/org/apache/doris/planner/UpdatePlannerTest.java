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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.update.UpdatePlanner;

import java.util.List;

import com.clearspring.analytics.util.Lists;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.doris.alter.SchemaChangeHandler.SHADOW_NAME_PRFIX;

public class UpdatePlannerTest {

    private final IdGenerator<TupleId> tupleIdGenerator_ = TupleId.createGenerator();
    private final IdGenerator<SlotId> slotIdGenerator_ = SlotId.createGenerator();

    /**
     * Full columns: k1, k2 v1, shadow_column
     * Shadow column: SHADOW_NAME_PRFIX + v1
     * Set expr: v1=1
     * Expect output exprs: k1, k2, 1, 1
     */
    @Test
    public void testComputeOutputExprsWithShadowColumnAndSetExpr(@Injectable OlapTable targetTable,
                                                                 @Injectable Column k1,
                                                                 @Injectable Column k2,
                                                                 @Injectable Column v1,
                                                                 @Injectable Column shadow_v1,
                                                                 @Injectable Analyzer analyzer) {
        List<Expr> setExprs = Lists.newArrayList();
        TableName tableName = new TableName(null, "test");
        SlotRef slotRef = new SlotRef(tableName, "V1");
        IntLiteral intLiteral = new IntLiteral(1);
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                slotRef, intLiteral);
        setExprs.add(binaryPredicate);
        TupleDescriptor srcTupleDesc = new TupleDescriptor(tupleIdGenerator_.getNextId());
        SlotDescriptor k1SlotDesc = new SlotDescriptor(slotIdGenerator_.getNextId(), srcTupleDesc);
        k1SlotDesc.setColumn(k1);
        srcTupleDesc.addSlot(k1SlotDesc);
        SlotDescriptor k2SlotDesc = new SlotDescriptor(slotIdGenerator_.getNextId(), srcTupleDesc);
        k2SlotDesc.setColumn(k2);
        srcTupleDesc.addSlot(k2SlotDesc);
        SlotDescriptor v1SlotDesc = new SlotDescriptor(slotIdGenerator_.getNextId(), srcTupleDesc);
        v1SlotDesc.setColumn(v1);
        srcTupleDesc.addSlot(v1SlotDesc);
        List<Column> fullSchema = Lists.newArrayList();
        fullSchema.add(k1);
        fullSchema.add(k2);
        fullSchema.add(v1);
        fullSchema.add(shadow_v1);

        new Expectations(){
            {
                targetTable.getFullSchema();
                result = fullSchema;
                k1.getName();
                result = "k1";
                k2.getName();
                result = "k2";
                v1.getName();
                result = "v1";
                shadow_v1.getName();
                result = SHADOW_NAME_PRFIX + "v1";
            }
        };

        UpdatePlanner updatePlanner = new UpdatePlanner(1, targetTable, setExprs, srcTupleDesc, analyzer);
        List<Expr> outputExpr = Deencapsulation.invoke(updatePlanner, "computeOutputExprs");
        Assert.assertEquals(4, outputExpr.size());
        Expr outputExpr1 = outputExpr.get(0);
        Assert.assertTrue(outputExpr1 instanceof SlotRef);
        Assert.assertEquals(((SlotRef) outputExpr1).getDesc().getColumn().getName(), "k1");
        Expr outputExpr2 = outputExpr.get(1);
        Assert.assertTrue(outputExpr2 instanceof SlotRef);
        Assert.assertEquals(((SlotRef) outputExpr2).getDesc().getColumn().getName(), "k2");
        Expr outputExpr3 = outputExpr.get(2);
        Assert.assertTrue(outputExpr3 instanceof IntLiteral);
        Assert.assertEquals(((IntLiteral) outputExpr3).getValue(), 1);
        Expr outputExpr4 = outputExpr.get(3);
        Assert.assertTrue(outputExpr4 instanceof IntLiteral);
        Assert.assertEquals(((IntLiteral) outputExpr4).getValue(), 1);
    }

    @Test
    public void testNewColumnBySchemaChange(@Injectable OlapTable targetTable,
                                            @Injectable Column k1,
                                            @Injectable Column k2,
                                            @Injectable Column v1,
                                            @Injectable Column new_v2,
                                            @Injectable Analyzer analyzer) throws AnalysisException {
        List<Expr> setExprs = Lists.newArrayList();
        TableName tableName = new TableName(null, "test");
        SlotRef slotRef = new SlotRef(tableName, "V1");
        IntLiteral intLiteral = new IntLiteral(1);
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                slotRef, intLiteral);
        setExprs.add(binaryPredicate);
        TupleDescriptor srcTupleDesc = new TupleDescriptor(tupleIdGenerator_.getNextId());
        SlotDescriptor k1SlotDesc = new SlotDescriptor(slotIdGenerator_.getNextId(), srcTupleDesc);
        k1SlotDesc.setColumn(k1);
        srcTupleDesc.addSlot(k1SlotDesc);
        SlotDescriptor k2SlotDesc = new SlotDescriptor(slotIdGenerator_.getNextId(), srcTupleDesc);
        k2SlotDesc.setColumn(k2);
        srcTupleDesc.addSlot(k2SlotDesc);
        SlotDescriptor v1SlotDesc = new SlotDescriptor(slotIdGenerator_.getNextId(), srcTupleDesc);
        v1SlotDesc.setColumn(v1);
        srcTupleDesc.addSlot(v1SlotDesc);
        List<Column> fullSchema = Lists.newArrayList();
        fullSchema.add(k1);
        fullSchema.add(k2);
        fullSchema.add(v1);
        fullSchema.add(new_v2);

        new Expectations(){
            {
                targetTable.getFullSchema();
                result = fullSchema;
                k1.getName();
                result = "k1";
                k2.getName();
                result = "k2";
                v1.getName();
                result = "v1";
                new_v2.getName();
                result = "v2";
                new_v2.getDefaultValue();
                result = "1";
                new_v2.getDefaultValueExpr();
                result = new IntLiteral(1);
            }
        };

        UpdatePlanner updatePlanner = new UpdatePlanner(1, targetTable, setExprs, srcTupleDesc, analyzer);
        List<Expr> outputExpr = Deencapsulation.invoke(updatePlanner, "computeOutputExprs");
        Assert.assertEquals(4, outputExpr.size());
        Expr outputExpr1 = outputExpr.get(0);
        Assert.assertTrue(outputExpr1 instanceof SlotRef);
        Assert.assertEquals(((SlotRef) outputExpr1).getDesc().getColumn().getName(), "k1");
        Expr outputExpr2 = outputExpr.get(1);
        Assert.assertTrue(outputExpr2 instanceof SlotRef);
        Assert.assertEquals(((SlotRef) outputExpr2).getDesc().getColumn().getName(), "k2");
        Expr outputExpr3 = outputExpr.get(2);
        Assert.assertTrue(outputExpr3 instanceof IntLiteral);
        Assert.assertEquals(((IntLiteral) outputExpr3).getValue(), 1);
        Expr outputExpr4 = outputExpr.get(3);
        Assert.assertTrue(outputExpr4 instanceof IntLiteral);
        Assert.assertEquals(((IntLiteral) outputExpr4).getValue(), 1);
    }
}
