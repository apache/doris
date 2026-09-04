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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.base.Preconditions;

import java.util.List;

/** Internal Map constructor that drops null entries produced by a Map-filter Lambda. */
public class MapFromFilteredEntriesUnique extends MapFromEntries {

    public MapFromFilteredEntriesUnique(Expression entries) {
        super("%map_from_filtered_entries_unique%", entries);
    }

    private MapFromFilteredEntriesUnique(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public MapFromFilteredEntriesUnique withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new MapFromFilteredEntriesUnique(getFunctionParams(children));
    }
}
