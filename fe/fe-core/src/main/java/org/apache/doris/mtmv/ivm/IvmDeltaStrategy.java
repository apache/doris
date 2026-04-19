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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.nereids.trees.plans.Plan;

import java.util.List;

/**
 * Strategy interface for IVM delta rewriting.
 * Each strategy handles a specific normalized plan pattern (e.g. scan-only, agg).
 */
public interface IvmDeltaStrategy {

    /**
     * Rewrites a normalized MV plan into delta command bundles.
     *
     * @param normalizedPlan the plan produced by IvmNormalizeMtmv (with ResultSink stripped)
     * @return one or more delta command bundles for execution
     */
    List<IvmDeltaCommandBundle> rewrite(Plan normalizedPlan);
}
