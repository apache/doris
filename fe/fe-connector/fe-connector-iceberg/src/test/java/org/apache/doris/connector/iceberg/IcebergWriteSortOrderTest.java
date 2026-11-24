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

package org.apache.doris.connector.iceberg;

import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TSortInfo;

import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IcebergWriteSortOrderTest {
    private final Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "s", Types.StructType.of(
                    Types.NestedField.optional(4, "k", Types.IntegerType.get()))));

    @Test
    public void completeIdentityOrderKeepsDirectionAndNullOrdering() {
        SortOrder order = identityOrder();
        TSortInfo sortInfo = sortInfo(Arrays.asList(true, false), Arrays.asList(false, true));

        SortOrder supported = IcebergWriteSortOrder.supportedOrder(schema, order);
        SortOrder fileOrder = IcebergWriteSortOrder.fileSortOrder(schema, order, sortInfo);
        Assertions.assertEquals(order.orderId(), supported.orderId());
        Assertions.assertTrue(order.sameOrder(supported));
        Assertions.assertEquals(order.orderId(), fileOrder.orderId());
        Assertions.assertTrue(order.sameOrder(fileOrder));
    }

    @Test
    public void unsupportedTransformsUseUnsortedOrder() {
        for (SortOrder order : Arrays.asList(
                SortOrder.builderFor(schema).asc(Expressions.truncate("id", 10)).build(),
                SortOrder.builderFor(schema).asc(Expressions.bucket("id", 4)).build())) {
            Assertions.assertTrue(IcebergWriteSortOrder.supportedOrder(schema, order).isUnsorted());
            Assertions.assertTrue(IcebergWriteSortOrder.fileSortOrder(schema, order,
                    sortInfo(Collections.singletonList(true), Collections.singletonList(true))).isUnsorted());
        }
    }

    @Test
    public void nestedIdentityUsesUnsortedOrder() {
        SortOrder order = SortOrder.builderFor(schema).asc("s.k").build();

        Assertions.assertTrue(IcebergWriteSortOrder.supportedOrder(schema, order).isUnsorted());
        Assertions.assertTrue(IcebergWriteSortOrder.fileSortOrder(schema, order,
                sortInfo(Collections.singletonList(true), Collections.singletonList(true))).isUnsorted());
    }

    @Test
    public void mixedOrdersDoNotClaimAnIdentitySubset() {
        for (SortOrder order : Arrays.asList(
                SortOrder.builderFor(schema).asc(Expressions.bucket("id", 4)).desc("name").build(),
                SortOrder.builderFor(schema).asc("name").asc(Expressions.truncate("id", 10)).build(),
                SortOrder.builderFor(schema).asc("s.k").desc("name").build(),
                SortOrder.builderFor(schema).asc("name").desc("s.k").build())) {
            Assertions.assertTrue(IcebergWriteSortOrder.supportedOrder(schema, order).isUnsorted());
            Assertions.assertTrue(IcebergWriteSortOrder.fileSortOrder(schema, order,
                    sortInfo(Arrays.asList(true, false), Arrays.asList(true, false))).isUnsorted());
        }
    }

    @Test
    public void absentAndPartialSortInfoUseUnsortedOrder() {
        SortOrder order = identityOrder();

        Assertions.assertTrue(IcebergWriteSortOrder.fileSortOrder(schema, order, null).isUnsorted());
        Assertions.assertTrue(IcebergWriteSortOrder.fileSortOrder(schema, order, new TSortInfo()).isUnsorted());
        Assertions.assertTrue(IcebergWriteSortOrder.fileSortOrder(schema, order,
                sortInfo(Collections.singletonList(true), Collections.singletonList(false))).isUnsorted());
        TSortInfo noExpressions = sortInfo(Arrays.asList(true, false), Arrays.asList(false, true));
        noExpressions.setOrderingExprs(Collections.emptyList());
        Assertions.assertTrue(IcebergWriteSortOrder.fileSortOrder(schema, order, noExpressions).isUnsorted());
    }

    @Test
    public void directionAndNullOrderMustMatchEveryField() {
        SortOrder order = identityOrder();

        Assertions.assertTrue(IcebergWriteSortOrder.fileSortOrder(schema, order,
                sortInfo(Arrays.asList(true, true), Arrays.asList(false, true))).isUnsorted());
        Assertions.assertTrue(IcebergWriteSortOrder.fileSortOrder(schema, order,
                sortInfo(Arrays.asList(true, false), Arrays.asList(true, true))).isUnsorted());
        TSortInfo missingDirection = sortInfo(Arrays.asList(true, false), Arrays.asList(false, true));
        missingDirection.setIsAscOrder(Collections.singletonList(true));
        Assertions.assertTrue(IcebergWriteSortOrder.fileSortOrder(schema, order, missingDirection).isUnsorted());
        TSortInfo missingNullOrder = sortInfo(Arrays.asList(true, false), Arrays.asList(false, true));
        missingNullOrder.setNullsFirst(Collections.singletonList(false));
        Assertions.assertTrue(IcebergWriteSortOrder.fileSortOrder(schema, order, missingNullOrder).isUnsorted());
    }

    @Test
    public void unsortedTableDoesNotClaimAnOrder() {
        SortOrder order = SortOrder.unsorted();

        Assertions.assertTrue(IcebergWriteSortOrder.supportedOrder(schema, order).isUnsorted());
        Assertions.assertTrue(IcebergWriteSortOrder.fileSortOrder(schema, order, null).isUnsorted());
        Assertions.assertTrue(IcebergWriteSortOrder.fileSortOrder(schema, order,
                sortInfo(Collections.singletonList(true), Collections.singletonList(true))).isUnsorted());
    }

    private SortOrder identityOrder() {
        return SortOrder.builderFor(schema).asc("id", NullOrder.NULLS_LAST)
                .desc("name", NullOrder.NULLS_FIRST).build();
    }

    private TSortInfo sortInfo(List<Boolean> ascending, List<Boolean> nullsFirst) {
        List<TExpr> expressions = new ArrayList<>();
        for (int i = 0; i < ascending.size(); i++) {
            expressions.add(new TExpr());
        }
        TSortInfo sortInfo = new TSortInfo();
        sortInfo.setOrderingExprs(expressions);
        sortInfo.setIsAscOrder(ascending);
        sortInfo.setNullsFirst(nullsFirst);
        return sortInfo;
    }
}
