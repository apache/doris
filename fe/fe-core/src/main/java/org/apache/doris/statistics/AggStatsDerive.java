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

package org.apache.doris.statistics;

import org.apache.doris.analysis.Expr;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.AggregationNode;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Derive AggNode statistics.
 */
public class AggStatsDerive extends BaseStatsDerive {
    private static final Logger LOG = LogManager.getLogger(AggStatsDerive.class);
    List<Expr> groupingExprs = new ArrayList<>();

    @Override
    public void init(PlanStats node) throws UserException {
        Preconditions.checkState(node instanceof AggregationNode);
        super.init(node);
        groupingExprs.addAll(((AggregationNode) node).getAggInfo().getGroupingExprs());
    }

    @Override
    protected long deriveRowCount() {
        rowCount = 1;
        // rowCount: product of # of distinct values produced by grouping exprs
        for (Expr groupingExpr : groupingExprs) {
            long numDistinct = groupingExpr.getNumDistinctValues();
            if (LOG.isDebugEnabled()) {
                LOG.debug("grouping expr: " + groupingExpr.toSql() + " #distinct=" + Long.toString(
                        numDistinct));
            }
            if (numDistinct == -1) {
                rowCount = -1;
                break;
            }
            // This is prone to overflow, because we keep multiplying cardinalities,
            // even if the grouping exprs are functionally dependent (example:
            // group by the primary key of a table plus a number of other columns from that
            // same table)
            // TODO: try to recognize functional dependencies
            // TODO: as a shortcut, instead of recognizing functional dependencies,
            // limit the contribution of a single table to the number of rows
            // of that table (so that when we're grouping by the primary key col plus
            // some others, the estimate doesn't overshoot dramatically)
            rowCount *= numDistinct;
        }
        if (rowCount > 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("sel=" + Double.toString(computeSelectivity()));
            }
            applyConjunctsSelectivity();
        }
        // if we ended up with an overflow, the estimate is certain to be wrong
        if (rowCount < 0) {
            rowCount = -1;
        }

        capRowCountAtLimit();
        if (LOG.isDebugEnabled()) {
            LOG.debug("stats Agg: rowCount={}", rowCount);
        }
        return rowCount;
    }
}
