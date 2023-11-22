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

package org.apache.doris.qe;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.PrepareStmt;
import org.apache.doris.planner.OriginalPlanner;
import org.apache.doris.planner.Planner;

import com.google.common.base.Preconditions;

public class PrepareStmtContext {
    public PrepareStmt stmt;
    public ConnectContext ctx;
    public Planner planner;
    public Analyzer analyzer;
    public String stmtString;

    // Timestamp in millisecond last command starts at
    protected volatile long startTime;

    public PrepareStmtContext(PrepareStmt stmt, ConnectContext ctx, Planner planner,
                                    Analyzer analyzer, String stmtString) {
        this.stmt = stmt;
        this.ctx = ctx;
        this.planner = planner;
        // Only support OriginalPlanner for now
        if (planner != null) {
            Preconditions.checkState(planner instanceof OriginalPlanner);
        }
        this.analyzer = analyzer;
        this.stmtString = stmtString;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime() {
        startTime = System.currentTimeMillis();
    }
}
