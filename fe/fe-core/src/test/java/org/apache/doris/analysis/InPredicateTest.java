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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class InPredicateTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    /*
    InPredicate1: k1 in (1,2)
    InPredicate2: k1 in (2,3)
    Intersection: k1 in (2)
     */
    @Test
    public void testIntersection() throws AnalysisException {
        SlotRef slotRef1 = new SlotRef(new TableName(internalCtl, "db1", "tb1"), "k1");
        LiteralExpr literalChild1 = new IntLiteral(1);
        LiteralExpr literalChild2 = new IntLiteral(2);
        List<Expr> literalChildren1 = Lists.newArrayList();
        literalChildren1.add(literalChild1);
        literalChildren1.add(literalChild2);
        InPredicate inPredicate1 = new InPredicate(slotRef1, literalChildren1, false);

        SlotRef slotRef2 = new SlotRef(new TableName(internalCtl, "db1", "tb1"), "k1");
        LiteralExpr literalChild3 = new LargeIntLiteral("2");
        LiteralExpr literalChild4 = new LargeIntLiteral("3");
        List<Expr> literalChildren2 = Lists.newArrayList();
        literalChildren2.add(literalChild3);
        literalChildren2.add(literalChild4);
        InPredicate inPredicate2 = new InPredicate(slotRef2, literalChildren2, false);

        // check result
        InPredicate intersection = inPredicate1.intersection(inPredicate2);
        Assert.assertEquals(slotRef1, intersection.getChild(0));
        Assert.assertTrue(intersection.isLiteralChildren());
        Assert.assertEquals(1, intersection.getListChildren().size());
        Assert.assertEquals(literalChild2, intersection.getChild(1));

        // keep origin predicate
        Assert.assertTrue(inPredicate1.isLiteralChildren());
        Assert.assertEquals(2, inPredicate1.getListChildren().size());
        Assert.assertTrue(inPredicate1.contains(literalChild1));
        Assert.assertTrue(inPredicate1.contains(literalChild2));
        Assert.assertTrue(inPredicate2.isLiteralChildren());
        Assert.assertEquals(2, inPredicate2.getListChildren().size());
        Assert.assertTrue(inPredicate2.contains(literalChild3));
        Assert.assertTrue(inPredicate2.contains(literalChild4));
    }

    /*
    InPredicate1: k1 in (1,2)
    InPredicate2: k1 in (1)
    Union: k1 in (1,2)
     */
    @Test
    public void testUnion() throws AnalysisException {
        SlotRef slotRef1 = new SlotRef(new TableName(internalCtl, "db1", "tb1"), "k1");
        LiteralExpr literalChild1 = new IntLiteral(1);
        LiteralExpr literalChild2 = new IntLiteral(2);
        List<Expr> literalChildren1 = Lists.newArrayList();
        literalChildren1.add(literalChild1);
        literalChildren1.add(literalChild2);
        InPredicate inPredicate1 = new InPredicate(slotRef1, literalChildren1, false);

        SlotRef slotRef2 = new SlotRef(new TableName(internalCtl, "db1", "tb1"), "k1");
        LiteralExpr literalChild3 = new LargeIntLiteral("1");
        List<Expr> literalChildren2 = Lists.newArrayList();
        literalChildren2.add(literalChild3);
        InPredicate inPredicate2 = new InPredicate(slotRef2, literalChildren2, false);

        // check result
        InPredicate union = inPredicate1.union(inPredicate2);
        Assert.assertEquals(slotRef1, union.getChild(0));
        Assert.assertTrue(union.isLiteralChildren());
        Assert.assertEquals(2, union.getListChildren().size());
        Assert.assertTrue(union.getListChildren().contains(literalChild1));
        Assert.assertTrue(union.getListChildren().contains(literalChild2));

        // keep origin predicate
        Assert.assertTrue(inPredicate1.isLiteralChildren());
        Assert.assertEquals(2, inPredicate1.getListChildren().size());
        Assert.assertTrue(inPredicate1.contains(literalChild1));
        Assert.assertTrue(inPredicate1.contains(literalChild2));
        Assert.assertTrue(inPredicate2.isLiteralChildren());
        Assert.assertEquals(1, inPredicate2.getListChildren().size());
        Assert.assertTrue(inPredicate2.contains(literalChild3));
    }

    @Test
    public void testIntersectionWithDateV2() throws AnalysisException {
        SlotRef slotRef1 = new SlotRef(new TableName(internalCtl, "db1", "tb1"), "k1");
        LiteralExpr literalChild1 = new DateLiteral(2022, 5, 19, Type.DATE);
        LiteralExpr literalChild2 = new DateLiteral(2022, 5, 20, Type.DATEV2);
        List<Expr> literalChildren1 = Lists.newArrayList();
        literalChildren1.add(literalChild1);
        literalChildren1.add(literalChild2);
        InPredicate inPredicate1 = new InPredicate(slotRef1, literalChildren1, false);

        SlotRef slotRef2 = new SlotRef(new TableName(internalCtl, "db1", "tb1"), "k1");
        LiteralExpr literalChild3 = new DateLiteral(2022, 5, 19, Type.DATEV2);
        LiteralExpr literalChild4 = new DateLiteral(2022, 5, 21, Type.DATE);
        List<Expr> literalChildren2 = Lists.newArrayList();
        literalChildren2.add(literalChild3);
        literalChildren2.add(literalChild4);
        InPredicate inPredicate2 = new InPredicate(slotRef2, literalChildren2, false);

        // check result
        InPredicate intersection = inPredicate1.intersection(inPredicate2);
        Assert.assertEquals(slotRef1, intersection.getChild(0));
        Assert.assertTrue(intersection.isLiteralChildren());
        Assert.assertEquals(1, intersection.getListChildren().size());
        Assert.assertEquals(literalChild1, intersection.getChild(1));

        // keep origin predicate
        Assert.assertTrue(inPredicate1.isLiteralChildren());
        Assert.assertEquals(2, inPredicate1.getListChildren().size());
        Assert.assertTrue(inPredicate1.contains(literalChild1));
        Assert.assertTrue(inPredicate1.contains(literalChild2));
        Assert.assertTrue(inPredicate2.isLiteralChildren());
        Assert.assertEquals(2, inPredicate2.getListChildren().size());
        Assert.assertTrue(inPredicate2.contains(literalChild3));
        Assert.assertTrue(inPredicate2.contains(literalChild4));
    }

    /*
    InPredicate1: k1 in (1,2)
    InPredicate2: k1 in (1)
    Union: k1 in (1,2)
     */
    @Test
    public void testUnionWithDateV2() throws AnalysisException {
        SlotRef slotRef1 = new SlotRef(new TableName(internalCtl, "db1", "tb1"), "k1");
        LiteralExpr literalChild1 = new DateLiteral(2022, 5, 19, Type.DATE);
        LiteralExpr literalChild2 = new DateLiteral(2022, 5, 20, Type.DATEV2);
        LiteralExpr literalChild3 = new DateLiteral(2022, 5, 20, 0, 0, 0, Type.DATETIME);
        List<Expr> literalChildren1 = Lists.newArrayList();
        literalChildren1.add(literalChild1);
        literalChildren1.add(literalChild2);
        literalChildren1.add(literalChild3);
        InPredicate inPredicate1 = new InPredicate(slotRef1, literalChildren1, false);

        SlotRef slotRef2 = new SlotRef(new TableName(internalCtl, "db1", "tb1"), "k1");
        LiteralExpr literalChild4 = new DateLiteral(2022, 5, 19, Type.DATE);
        LiteralExpr literalChild5 = new DateLiteral(2022, 5, 20, 0, 0, 0, Type.DATETIMEV2);
        List<Expr> literalChildren2 = Lists.newArrayList();
        literalChildren2.add(literalChild4);
        literalChildren2.add(literalChild5);
        InPredicate inPredicate2 = new InPredicate(slotRef2, literalChildren2, false);

        // check result
        InPredicate union = inPredicate1.union(inPredicate2);
        Assert.assertEquals(slotRef1, union.getChild(0));
        Assert.assertTrue(union.isLiteralChildren());
        Assert.assertEquals(3, union.getListChildren().size());
        Assert.assertTrue(union.getListChildren().contains(literalChild1));
        Assert.assertTrue(union.getListChildren().contains(literalChild2));
        Assert.assertTrue(union.getListChildren().contains(literalChild5));

        // keep origin predicate
        Assert.assertTrue(inPredicate1.isLiteralChildren());
        Assert.assertEquals(3, inPredicate1.getListChildren().size());
        Assert.assertTrue(inPredicate1.contains(literalChild1));
        Assert.assertTrue(inPredicate1.contains(literalChild2));
        Assert.assertTrue(inPredicate2.isLiteralChildren());
        Assert.assertEquals(2, inPredicate2.getListChildren().size());
        Assert.assertTrue(inPredicate2.contains(literalChild3));
    }
}
