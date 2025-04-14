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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.literal.CharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class BooleanUtilsTest {

    @Test
    public void testProcessInPredicateChildren() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();
        LessThan lessThan1 = new LessThan(new IntegerLiteral(1), new IntegerLiteral(2));
        Cast cast1 = new Cast(lessThan1, VarcharType.createVarcharType(10));
        List<Expression> expressionList1 = Lists.newArrayList();
        expressionList1.add(new VarcharLiteral("true"));
        expressionList1.add(new VarcharLiteral("false"));
        expressionList1.add(new VarcharLiteral("unknown"));
        InPredicate inPredicate1 = new InPredicate(cast1, expressionList1);
        List<Expression> result1 = BooleanUtils.processInPredicateChildren(inPredicate1.children());
        Assertions.assertEquals(4, result1.size());
        Assertions.assertEquals(cast1, result1.get(0));
        Assertions.assertEquals(new VarcharLiteral("1"), result1.get(1));
        Assertions.assertEquals(new VarcharLiteral("0"), result1.get(2));
        Assertions.assertEquals(new VarcharLiteral("unknown"), result1.get(3));

        LessThan lessThan2 = new LessThan(new IntegerLiteral(1), new IntegerLiteral(2));
        Cast cast2 = new Cast(lessThan2, CharType.createCharType(1));
        List<Expression> expressionList2 = Lists.newArrayList();
        expressionList2.add(new CharLiteral("true", 4));
        expressionList2.add(new CharLiteral("false", 5));
        expressionList2.add(new CharLiteral("unknown", 7));
        InPredicate inPredicate2 = new InPredicate(cast2, expressionList2);
        List<Expression> result2 = BooleanUtils.processInPredicateChildren(inPredicate2.children());
        Assertions.assertEquals(4, result2.size());
        Assertions.assertEquals(cast2, result2.get(0));
        Assertions.assertEquals(new CharLiteral("1", 1), result2.get(1));
        Assertions.assertEquals(new CharLiteral("0", 1), result2.get(2));
        Assertions.assertEquals(new CharLiteral("unknown", 7), result2.get(3));

        LessThan lessThan3 = new LessThan(new IntegerLiteral(1), new IntegerLiteral(2));
        Cast cast3 = new Cast(lessThan3, StringType.INSTANCE);
        List<Expression> expressionList3 = Lists.newArrayList();
        expressionList3.add(new StringLiteral("true"));
        expressionList3.add(new StringLiteral("false"));
        expressionList3.add(new StringLiteral("unknown"));
        InPredicate inPredicate3 = new InPredicate(cast3, expressionList3);
        List<Expression> result3 = BooleanUtils.processInPredicateChildren(inPredicate3.children());
        Assertions.assertEquals(4, result3.size());
        Assertions.assertEquals(cast3, result3.get(0));
        Assertions.assertEquals(new StringLiteral("1"), result3.get(1));
        Assertions.assertEquals(new StringLiteral("0"), result3.get(2));
        Assertions.assertEquals(new StringLiteral("unknown"), result3.get(3));
    }

    @Test
    public void testProcessEqualPredicate() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        LessThan lessThan1 = new LessThan(new IntegerLiteral(1), new IntegerLiteral(2));
        Cast cast1 = new Cast(lessThan1, VarcharType.createVarcharType(10));
        EqualPredicate equalTo1 = new EqualTo(cast1, new VarcharLiteral("true"));
        EqualPredicate equalTo2 = new EqualTo(cast1, new VarcharLiteral("false"));
        EqualPredicate equalTo3 = new EqualTo(cast1, new VarcharLiteral("unknown"));
        Expression result1 = BooleanUtils.processEqualPredicate(equalTo1);
        Expression result2 = BooleanUtils.processEqualPredicate(equalTo2);
        Expression result3 = BooleanUtils.processEqualPredicate(equalTo3);

        Assertions.assertEquals(cast1, result1.child(0));
        Assertions.assertEquals(cast1, result2.child(0));
        Assertions.assertEquals(cast1, result3.child(0));
        Assertions.assertEquals(new VarcharLiteral("1"), result1.child(1));
        Assertions.assertEquals(new VarcharLiteral("0"), result2.child(1));
        Assertions.assertEquals(new VarcharLiteral("unknown"), result3.child(1));

        LessThan lessThan2 = new LessThan(new IntegerLiteral(1), new IntegerLiteral(2));
        Cast cast2 = new Cast(lessThan2, CharType.createCharType(1));
        EqualPredicate equalTo4 = new EqualTo(cast2, new CharLiteral("true", 4));
        EqualPredicate equalTo5 = new EqualTo(cast2, new CharLiteral("false", 5));
        EqualPredicate equalTo6 = new EqualTo(cast2, new CharLiteral("unknown", 7));
        Expression result4 = BooleanUtils.processEqualPredicate(equalTo4);
        Expression result5 = BooleanUtils.processEqualPredicate(equalTo5);
        Expression result6 = BooleanUtils.processEqualPredicate(equalTo6);

        Assertions.assertEquals(cast2, result4.child(0));
        Assertions.assertEquals(cast2, result5.child(0));
        Assertions.assertEquals(cast2, result6.child(0));
        Assertions.assertEquals(new CharLiteral("1", 1), result4.child(1));
        Assertions.assertEquals(new CharLiteral("0", 1), result5.child(1));
        Assertions.assertEquals(new CharLiteral("unknown", 7), result6.child(1));

        LessThan lessThan3 = new LessThan(new IntegerLiteral(1), new IntegerLiteral(2));
        Cast cast3 = new Cast(lessThan3, StringType.INSTANCE);
        EqualPredicate equalTo7 = new EqualTo(cast3, new StringLiteral("true"));
        EqualPredicate equalTo8 = new EqualTo(cast3, new StringLiteral("false"));
        EqualPredicate equalTo9 = new EqualTo(cast3, new StringLiteral("unknown"));
        Expression result7 = BooleanUtils.processEqualPredicate(equalTo7);
        Expression result8 = BooleanUtils.processEqualPredicate(equalTo8);
        Expression result9 = BooleanUtils.processEqualPredicate(equalTo9);

        Assertions.assertEquals(cast3, result7.child(0));
        Assertions.assertEquals(cast3, result8.child(0));
        Assertions.assertEquals(cast3, result9.child(0));
        Assertions.assertEquals(new StringLiteral("1"), result7.child(1));
        Assertions.assertEquals(new StringLiteral("0"), result8.child(1));
        Assertions.assertEquals(new StringLiteral("unknown"), result9.child(1));
    }

}
