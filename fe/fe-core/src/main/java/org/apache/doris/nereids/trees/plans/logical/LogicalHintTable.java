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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.trees.plans.DummyPlan;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * LogicalHintTable
 */
public class LogicalHintTable extends DummyPlan {
    private List<String> nameParts;

    public LogicalHintTable(List<String> nameParts) {
        this.nameParts = nameParts;
    }

    @Override
    public void collectParamsForLeadingHint(List<String> params, Map<String, DistributeHint> strToHint,
            List<String> errs) {
        params.add(StringUtils.join(nameParts, '.'));
    }

    @Override
    public DummyPlan withChildren(List<DummyPlan> children) {
        Preconditions.checkArgument(children.isEmpty());
        return this;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalHintTable", "tableName", nameParts);
    }
}
