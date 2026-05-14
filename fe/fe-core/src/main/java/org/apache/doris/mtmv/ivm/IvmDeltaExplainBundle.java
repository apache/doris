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

import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.Objects;

/**
 * One dry-run delta bundle produced for EXPLAIN REFRESH.
 */
public class IvmDeltaExplainBundle {
    private final int deltaId;
    private final TableNameInfo baseTable;
    private final int occurrence;
    private final long consumedTso;
    private final long latestTso;
    private final boolean noOp;
    private final Plan deltaPlan;

    public IvmDeltaExplainBundle(int deltaId, TableNameInfo baseTable, int occurrence,
            long consumedTso, long latestTso, boolean noOp, Plan deltaPlan) {
        this.deltaId = deltaId;
        this.baseTable = Objects.requireNonNull(baseTable, "baseTable can not be null");
        this.occurrence = occurrence;
        this.consumedTso = consumedTso;
        this.latestTso = latestTso;
        this.noOp = noOp;
        this.deltaPlan = Objects.requireNonNull(deltaPlan, "deltaPlan can not be null");
    }

    public int getDeltaId() {
        return deltaId;
    }

    public TableNameInfo getBaseTable() {
        return baseTable;
    }

    public int getOccurrence() {
        return occurrence;
    }

    public long getConsumedTso() {
        return consumedTso;
    }

    public long getLatestTso() {
        return latestTso;
    }

    public boolean isNoOp() {
        return noOp;
    }

    public Plan getDeltaPlan() {
        return deltaPlan;
    }
}
