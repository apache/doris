package org.apache.doris.optimizer.utils;

import com.google.common.collect.Lists;
import org.apache.doris.optimizer.MultiExpression;
import org.apache.doris.optimizer.OptGroup;
import org.apache.doris.optimizer.OptMemo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class MemoStatus {
    private static final Logger LOG = LogManager.getLogger(MemoStatus.class);

    public static boolean checkStatusForExplore(OptMemo memo) {
        return checkStatus(memo, true);
    }

    public static boolean checkStatusForImplement(OptMemo memo) {
        return checkStatus(memo, false);
    }

    private static boolean checkStatus(OptMemo memo, boolean isExplore) {
        final List<OptGroup> groups = memo.getGroups();
        LOG.info("Groups:" + memo.getGroups().size() + " MultExpressions:" + memo.getMExprs().size());
        final List<OptGroup> unoptimizedGroups = Lists.newArrayList();
        final List<MultiExpression> unimplementedMExprs = Lists.newArrayList();
        for (OptGroup group : groups) {
            if (!group.isOptimized()) {
                unoptimizedGroups.add(group);
            }
            MultiExpression firstLogicalExpr;
            if (isExplore) {
                firstLogicalExpr = group.getFirstLogicalMultiExpression();
            } else {
                firstLogicalExpr = group.getFirstPhysicalMultiExpression();
            }
            for (; firstLogicalExpr != null; firstLogicalExpr = firstLogicalExpr.next()) {
                if (!firstLogicalExpr.isOptimized()) {
                    unimplementedMExprs.add(firstLogicalExpr);
                }
            }
        }

        final StringBuilder unoptimizedGroupsLog = new StringBuilder("Unoptimized Group ids:");
        for (OptGroup group : unoptimizedGroups) {
            unoptimizedGroupsLog.append(group.getId()).append(" status:")
                    .append(group.getStatus()).append(" MultiExpression ids:");
            for (MultiExpression mExpr : group.getMultiExpressions()) {
                unoptimizedGroupsLog.append(mExpr.getId()).append(" status:").append(mExpr.getStatus()).append(", ");
            }
        }
        LOG.info(unoptimizedGroupsLog);

        final StringBuilder unimplementedMExprLog = new StringBuilder("Unoptimized MultiExpression ids:");
        for (MultiExpression mExpr : unimplementedMExprs) {
            unimplementedMExprLog.append(mExpr.getOp()).append(" ").append(mExpr.getId()).append(", ");
        }
        LOG.info(unimplementedMExprLog);

        return unoptimizedGroups.size() == 0 && unimplementedMExprs.size() == 0 ? true : false;
    }
}
