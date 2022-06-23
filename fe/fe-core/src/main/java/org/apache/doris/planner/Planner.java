// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.planner;

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.UserException;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public abstract class Planner {

    protected ArrayList<PlanFragment> fragments = Lists.newArrayList();

    protected boolean isBlockQuery = false;

    public abstract List<ScanNode> getScanNodes();

    public abstract void plan(StatementBase queryStmt,
            org.apache.doris.thrift.TQueryOptions queryOptions) throws UserException;

    public String getExplainString(List<PlanFragment> fragments, ExplainOptions explainOptions) {
        return "Not implemented yet";
    }

    public List<PlanFragment> getFragments() {
        return fragments;
    }

    public boolean isBlockQuery() {
        return isBlockQuery;
    }

}
