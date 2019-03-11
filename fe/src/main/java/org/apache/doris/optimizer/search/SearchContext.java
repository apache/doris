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

package org.apache.doris.optimizer.search;

import org.apache.doris.optimizer.OptGroup;
import org.apache.doris.optimizer.OptMemo;

public class SearchContext {

    private final OptMemo memo;
    private final Scheduler scheduler;

    private SearchContext(OptMemo memo, Scheduler scheduler) {
        this.memo = memo;
        this.scheduler = scheduler;
    }

    public static SearchContext create(OptMemo memo, OptGroup firstGroup,
                                       OptimizationContext oContext, Scheduler scheduler) {
        final SearchContext sContext = new SearchContext(memo, scheduler);
        TaskGroupOptimization.schedule(sContext, firstGroup, oContext, null);
        return sContext;
    }

    public void schedule(Task task) {
        scheduler.add(task);
    }

    public OptMemo getMemo() { return memo; }
}
