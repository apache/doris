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

import org.apache.doris.thrift.TSortInfo;

import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;

/** Resolves the complete sort order that the data writer can actually execute. */
final class IcebergWriteSortOrder {
    private IcebergWriteSortOrder() {
    }

    static SortOrder supportedOrder(Schema schema, SortOrder order) {
        for (SortField field : order.fields()) {
            if (!field.transform().isIdentity() || schema.asStruct().field(field.sourceId()) == null) {
                // Sorting a subset of the fields does not satisfy the declared lexicographic order.
                return SortOrder.unsorted();
            }
        }
        return order;
    }

    static SortOrder fileSortOrder(Schema schema, SortOrder order, TSortInfo sortInfo) {
        SortOrder supported = supportedOrder(schema, order);
        if (!supported.isSorted() || sortInfo == null) {
            return SortOrder.unsorted();
        }
        int fieldCount = supported.fields().size();
        if (sortInfo.getOrderingExprsSize() != fieldCount
                || sortInfo.getIsAscOrderSize() != fieldCount || sortInfo.getNullsFirstSize() != fieldCount) {
            return SortOrder.unsorted();
        }
        for (int i = 0; i < fieldCount; i++) {
            SortField field = supported.fields().get(i);
            if (sortInfo.getIsAscOrder().get(i) != (field.direction() == SortDirection.ASC)
                    || sortInfo.getNullsFirst().get(i) != (field.nullOrder() == NullOrder.NULLS_FIRST)) {
                return SortOrder.unsorted();
            }
        }
        return supported;
    }
}
