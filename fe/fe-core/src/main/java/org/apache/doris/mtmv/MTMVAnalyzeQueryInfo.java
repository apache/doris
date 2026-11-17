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

package org.apache.doris.mtmv;

import org.apache.doris.mtmv.ivm.IvmRewriteResult;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;

import java.util.List;
import java.util.Map;

public class MTMVAnalyzeQueryInfo {
    private MTMVRelation relation;
    private MTMVPartitionInfo mvPartitionInfo;
    private List<ColumnDefinition> columnDefinitions;
    private List<String> keys;
    private Map<String, String> properties;
    // set when IVM rewrite is enabled; carries normalizedPlan + aggMeta
    private IvmRewriteResult ivmRewriteResult;
    // The plan the query was analysed into. Kept for the callers that have to ask what the query reads
    // rather than only that it reads something; set by MTMVPlanUtil#analyzeQueryWithSql.
    private Plan analyzedPlan;

    public MTMVAnalyzeQueryInfo(List<ColumnDefinition> columnDefinitions, List<String> keys,
            MTMVPartitionInfo mvPartitionInfo, MTMVRelation relation, Map<String, String> properties,
            IvmRewriteResult ivmRewriteResult, Plan analyzedPlan) {
        this.columnDefinitions = columnDefinitions;
        this.keys = keys;
        this.mvPartitionInfo = mvPartitionInfo;
        this.relation = relation;
        this.properties = properties;
        this.ivmRewriteResult = ivmRewriteResult;
        this.analyzedPlan = analyzedPlan;
    }

    public List<ColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }

    public List<String> getKeys() {
        return keys;
    }

    public MTMVPartitionInfo getMvPartitionInfo() {
        return mvPartitionInfo;
    }

    public MTMVRelation getRelation() {
        return relation;
    }

    public IvmRewriteResult getIvmRewriteResult() {
        return ivmRewriteResult;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Plan getAnalyzedPlan() {
        return analyzedPlan;
    }

    /** Convenience accessor — returns the normalized plan, or null if IVM is not active. */
    public Plan getIvmNormalizedPlan() {
        return ivmRewriteResult != null ? ivmRewriteResult.getNormalizedPlan() : null;
    }
}
