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

package org.apache.doris.nereids.metrics.event;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

/**
 * stats state event
 */
public class StatsStateEvent extends StateEvent {
    private final Statistics statistics;

    private StatsStateEvent(GroupExpression groupExpression, Statistics statistics) {
        super(groupExpression);
        this.statistics = statistics;
    }

    public static StatsStateEvent of(GroupExpression groupExpression, Statistics statistics) {
        return checkConnectContext(StatsStateEvent.class)
                ? new StatsStateEvent(groupExpression, statistics) : null;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("StatsStateEvent", "Statistics", statistics);
    }
}
