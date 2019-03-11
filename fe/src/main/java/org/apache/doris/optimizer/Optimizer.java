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

package org.apache.doris.optimizer;

import org.apache.doris.optimizer.search.*;

/**
 * Optimizer's entrance class
 */
public class Optimizer {

    private OptMemo memo;
    private OptGroup root;

    public Optimizer(OptExpression query) {
        memo = new OptMemo();
        final MultiExpression mExpr = memo.copyIn(query);
        root = mExpr.getGroup();
    }

    public void optimize() {
        final OptimizationContext oContext = new OptimizationContext();
        final Scheduler scheduler = DefaultScheduler.create();
        final SearchContext sContext = SearchContext.create(memo, root, oContext, scheduler);
        scheduler.run(sContext);
    }
}
