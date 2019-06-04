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

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.base.*;
import org.apache.doris.optimizer.utils.MemoStatus;
import org.apache.doris.optimizer.utils.PlansStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;

public class UTMemoStatus {
    private static final Logger LOG = LogManager.getLogger(ImplementationSearchingTest.class);

    private final int relNum;
    private final OptMemo memo;
    private OptimizationContext rootOptContext;
    private OptColumnRefSet statsColumns;
    private final MemoStatus memoChecker;

    public UTMemoStatus(int relNum, OptMemo memo) {
        this(relNum, memo, null, null);
    }

    public UTMemoStatus(int relNum, OptMemo memo, OptimizationContext rootOptContext, OptColumnRefSet statsColumns) {
        this.relNum = relNum;
        this.memo = memo;
        this.rootOptContext = rootOptContext;
        this.statsColumns = statsColumns;
        this.memoChecker = new MemoStatus(memo);
    }

    public void checkExploreStatus() {
        Preconditions.checkState(memoChecker.checkStatusForExplore());
        Preconditions.checkState(!memoChecker.checkLeakInDag());

        final long groupNum = getGroupsNum();
        Assert.assertEquals(groupNum, memo.getGroups().size());

        final long logicalMExprNum = getLogicalMExprNum();
        Assert.assertEquals(logicalMExprNum, memo.getLogicalMExprs().size());

        final long planNum = getPlanNum();
        Assert.assertEquals(planNum, PlansStatus.getLogicalPlanCount(memo.getRoot()));

        final StringBuilder logBuilder = new StringBuilder("Memo status ->");
        logBuilder.append(" Group Num:").append(groupNum).append(", ");
        logBuilder.append(" Logical MExpr:").append(logicalMExprNum).append(", ");
        logBuilder.append(" Plan Num:").append(planNum);
        LOG.info(logBuilder.toString());
    }

    public void checkImplementStatus() {
        Preconditions.checkState(memoChecker.checkStatusForImplement());
        Preconditions.checkState(!memoChecker.checkLeakInDag());

        final long groupNum = getGroupsNum();
        Assert.assertEquals(groupNum, memo.getGroups().size());

        final long logicalMExprNum = getLogicalMExprNum();
        Assert.assertEquals(logicalMExprNum, memo.getLogicalMExprs().size());

        final long physicalMExprNum = getPhysicalMExprNum();
        Assert.assertEquals(physicalMExprNum, memo.getPhysicalMExprs().size());

        final long planNum = getPlanNum();
        Assert.assertEquals(planNum, PlansStatus.getLogicalPlanCount(memo.getRoot()));
        Assert.assertEquals(planNum, PlansStatus.getPhysicalPlanCount(memo.getRoot()));

        final StringBuilder logBuilder = new StringBuilder("Memo status ->");
        logBuilder.append(" Rel Num:").append(relNum).append(", ");
        logBuilder.append(" Group Num:").append(groupNum).append(", ");
        logBuilder.append(" Logical MExpr:").append(logicalMExprNum).append(", ");
        logBuilder.append(" Physical MExpr:").append(physicalMExprNum).append(", ");
        logBuilder.append(" Plan Num:").append(planNum);
        LOG.info(logBuilder.toString());

        final OptLogicalProperty logicalProperty = (OptLogicalProperty)memo.getRoot().getProperty();
        Assert.assertEquals(false, logicalProperty.getOutputColumns().isDifferent(statsColumns));

        final OptPhysicalProperty physicalProperty = rootOptContext.getBestCostCtx().getPhysicalProperty();
        final RequiredPhysicalProperty requiredPhysicalProperty = rootOptContext.getRequiredPhysicalProperty();
        Assert.assertEquals(
                requiredPhysicalProperty.getDistributionProperty().getPropertySpec(),
                physicalProperty.getDistributionSpec());
        Assert.assertEquals(
                requiredPhysicalProperty.getOrderProperty().getPropertySpec(),
                physicalProperty.getOrderSpec());

    }

    private long getGroupsNum() {
        return (long)(Math.pow(2, relNum) + 0.5) -1;
    }

    private long getLogicalMExprNum() {
        return (long)(Math.pow(3, relNum) - Math.pow(2, relNum + 1) + 1) + relNum;
    }

    private long getPhysicalMExprNum() {
        return getLogicalMExprNum();
    }

    private long getPlanNum() {
        return factorial(2 * relNum - 2) / factorial(relNum - 1);
    }

    public static long factorial(long num){
        long sum=1;
        if(num < 0)
            return num;
        if(num==1){
            return 1;
        }else{
            sum = num * factorial(num-1);
            return sum;
        }
    }
}
