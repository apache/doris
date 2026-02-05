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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/SortInfo.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.thrift.TSortInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Encapsulates all the information needed to compute ORDER BY
 * This doesn't contain aliases or positional exprs.
 * TODO: reorganize this completely, this doesn't really encapsulate anything; this
 * should move into planner/ and encapsulate the implementation of the sort of a
 * particular input row (materialize all row slots)
 */
public class SortInfo {

    private List<Expr> orderingExprs;
    private final List<Boolean> isAscOrder;
    // True if "NULLS FIRST", false if "NULLS LAST", null if not specified.
    private final List<Boolean> nullsFirstParams;
    // The single tuple that is materialized, sorted, and output by a sort operator
    // (i.e. SortNode or TopNNode)
    private TupleDescriptor sortTupleDesc;
    // Input expressions materialized into sortTupleDesc_. One expr per slot in
    // sortTupleDesc_.
    private boolean useTwoPhaseRead = false;

    /**
     * Used by new optimizer.
     */
    public SortInfo(List<Expr> orderingExprs,
                    List<Boolean> isAscOrder,
                    List<Boolean> nullsFirstParams,
                    TupleDescriptor sortTupleDesc) {
        this.orderingExprs = orderingExprs;
        this.isAscOrder = isAscOrder;
        this.nullsFirstParams = nullsFirstParams;
        this.sortTupleDesc = sortTupleDesc;
    }

    /**
     * C'tor for cloning.
     */
    private SortInfo(SortInfo other) {
        orderingExprs = Expr.cloneList(other.orderingExprs);
        isAscOrder = Lists.newArrayList(other.isAscOrder);
        nullsFirstParams = Lists.newArrayList(other.nullsFirstParams);
        sortTupleDesc = other.sortTupleDesc;
    }

    public List<Expr> getOrderingExprs() {
        return orderingExprs;
    }

    public List<Boolean> getIsAscOrder() {
        return isAscOrder;
    }

    public List<Boolean> getNullsFirstParams() {
        return nullsFirstParams;
    }

    public void setUseTwoPhaseRead() {
        useTwoPhaseRead = true;
    }

    public TupleDescriptor getSortTupleDescriptor() {
        return sortTupleDesc;
    }

    /**
     * Gets the list of booleans indicating whether nulls come first or last, independent
     * of asc/desc.
     */
    public List<Boolean> getNullsFirst() {
        Preconditions.checkState(orderingExprs.size() == nullsFirstParams.size());
        List<Boolean> nullsFirst = Lists.newArrayList();
        for (int i = 0; i < orderingExprs.size(); ++i) {
            nullsFirst.add(OrderByElement.nullsFirst(nullsFirstParams.get(i),
                    isAscOrder.get(i)));
        }
        return nullsFirst;
    }

    @Override
    public SortInfo clone() {
        return new SortInfo(this);
    }

    /**
     * Convert the sort info to TSortInfo.
     */
    public TSortInfo toThrift() {
        TSortInfo sortInfo = new TSortInfo(
                Expr.treesToThrift(orderingExprs),
                isAscOrder,
                nullsFirstParams);
        if (useTwoPhaseRead) {
            sortInfo.setUseTwoPhaseRead(true);
        }
        return sortInfo;
    }
}
