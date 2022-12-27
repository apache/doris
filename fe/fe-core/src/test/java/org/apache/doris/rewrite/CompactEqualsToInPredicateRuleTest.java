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

package org.apache.doris.rewrite;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;

public class CompactEqualsToInPredicateRuleTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    //a=1 or b=2 or a=3 or b=4
    //=> a in (1, 2) or b in (3, 4)
    @Test
    public void testCompactEquals() {
        SlotRef a = new SlotRef(new TableName(internalCtl, "db1", "tb1"), "a");
        SlotRef b = new SlotRef(new TableName(internalCtl, "db1", "tb1"), "b");
        IntLiteral i1 = new IntLiteral(1);
        IntLiteral i2 = new IntLiteral(2);
        IntLiteral i3 = new IntLiteral(3);
        IntLiteral i4 = new IntLiteral(4);
        BinaryPredicate aeq1 = new BinaryPredicate(BinaryPredicate.Operator.EQ, i1, a);
        BinaryPredicate aeq3 = new BinaryPredicate(BinaryPredicate.Operator.EQ, a, i3);
        BinaryPredicate beq2 = new BinaryPredicate(BinaryPredicate.Operator.EQ, b, i2);
        BinaryPredicate beq4 = new BinaryPredicate(BinaryPredicate.Operator.EQ, b, i4);
        CompoundPredicate or1 = new CompoundPredicate(Operator.OR, aeq1, beq2);
        CompoundPredicate or2 = new CompoundPredicate(Operator.OR, or1, aeq3);
        CompoundPredicate or3 = new CompoundPredicate(Operator.OR, or2, beq4);
        CompactEqualsToInPredicateRule rule = new CompactEqualsToInPredicateRule();
        Pair result = Deencapsulation.invoke(rule,
                "compactEqualsToInPredicate", or3);
        Assertions.assertEquals(true, result.first);
        Assertions.assertTrue(result.second instanceof CompoundPredicate);
        CompoundPredicate or = (CompoundPredicate) result.second;
        Assertions.assertEquals(Operator.OR, or.getOp());
        InPredicate in1 = (InPredicate) or.getChild(0);
        InPredicate in2 = (InPredicate) or.getChild(1);
        SlotRef s1 = (SlotRef) in1.getChildren().get(0);
        InPredicate tmp;
        if (s1.getColumnName().equals("b")) {
            tmp = in1;
            in1 = in2;
            in2 = tmp;
        }
        Assertions.assertEquals(in1.getChild(0), a);
        Assertions.assertEquals(in2.getChild(0), b);

        HashSet<IntLiteral> seta = new HashSet<>();
        seta.add(i1);
        seta.add(i3);
        HashSet<IntLiteral> setb = new HashSet<>();
        setb.add(i2);
        setb.add(i4);

        Assertions.assertTrue(seta.contains(in1.getChild(1)));
        Assertions.assertTrue(seta.contains(in1.getChild(2)));

        Assertions.assertTrue(setb.contains(in2.getChild(1)));
        Assertions.assertTrue(setb.contains(in2.getChild(2)));
    }

    //a=1 or a in (3, 2) => a in (1, 2, 3)
    @Test
    public void testCompactEqualsAndIn() {
        SlotRef a = new SlotRef(new TableName(internalCtl, "db1", "tb1"), "a");
        IntLiteral i1 = new IntLiteral(1);
        IntLiteral i2 = new IntLiteral(2);
        IntLiteral i3 = new IntLiteral(3);
        BinaryPredicate aeq1 = new BinaryPredicate(BinaryPredicate.Operator.EQ, i1, a);
        InPredicate ain23 = new InPredicate(a, Lists.newArrayList(i2, i3), false);
        CompoundPredicate or1 = new CompoundPredicate(Operator.OR, aeq1, ain23);
        CompactEqualsToInPredicateRule rule = new CompactEqualsToInPredicateRule();
        Pair result = Deencapsulation.invoke(rule,
                "compactEqualsToInPredicate", or1);
        Assertions.assertEquals(true, result.first);
        Assertions.assertTrue(result.second instanceof InPredicate);
        InPredicate in123 = (InPredicate) result.second;
        Assertions.assertEquals(in123.getChild(0), a);
        HashSet<IntLiteral> seta = new HashSet<>();
        seta.add(i1);
        seta.add(i2);
        seta.add(i3);

        Assertions.assertTrue(seta.contains(in123.getChild(1)));
        Assertions.assertTrue(seta.contains(in123.getChild(2)));
        Assertions.assertTrue(seta.contains(in123.getChild(3)));
    }
}
