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

import java.util.List;
import java.util.Objects;

public class GroupingInfo {
    public static final String GROUPING_PREFIX = "GROUPING_PREFIX_";
    private TupleDescriptor virtualTuple;
    private TupleDescriptor outputTupleDesc;
    private GroupByClause.GroupingType groupingType;

    private List<Expr> preRepeatExprs;

    /**
     * Used by new optimizer.
     */
    public GroupingInfo(GroupByClause.GroupingType groupingType, TupleDescriptor virtualTuple,
            TupleDescriptor outputTupleDesc, List<Expr> preRepeatExprs) {
        this.groupingType = groupingType;
        this.virtualTuple = Objects.requireNonNull(virtualTuple, "virtualTuple can not be null");
        this.outputTupleDesc = Objects.requireNonNull(outputTupleDesc, "outputTupleDesc can not be null");
        this.preRepeatExprs = Objects.requireNonNull(preRepeatExprs, "preRepeatExprs can not be null");
    }


    public TupleDescriptor getOutputTupleDesc() {
        return outputTupleDesc;
    }

    public List<Expr> getPreRepeatExprs() {
        return preRepeatExprs;
    }
}
