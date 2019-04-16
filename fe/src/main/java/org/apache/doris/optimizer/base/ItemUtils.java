package org.apache.doris.optimizer.base;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.operator.OptItemColumnRef;

import java.util.List;

public class ItemUtils {

    public static List<OptColumnRef> collectItemColumnRefs(List<OptExpression> exprs) {
        final List<OptColumnRef> columnRefs = Lists.newArrayList();
        exprs.stream().forEach((expr)->collectItemColumnRefs(expr, columnRefs));
        return columnRefs;
    }

    public static List<OptColumnRef> collectItemColumnRefs(OptExpression expr) {
        final List<OptColumnRef> columnRefs = Lists.newArrayList();
        collectItemColumnRefs(expr, columnRefs);
        return columnRefs;
    }

    private static void collectItemColumnRefs(OptExpression expr, List<OptColumnRef> columnRefs) {
        if (expr.getOp() instanceof OptItemColumnRef) {
            final OptItemColumnRef columnRef = (OptItemColumnRef) expr.getOp();
            columnRefs.add(columnRef.getColumnRef());
        }

        expr.getInputs().stream().forEach((child)->collectItemColumnRefs(child, columnRefs));
    }

    public static void collectSlotId(Expr expr, OptColumnRefSet columns, boolean isAllSlot) {
        if (expr instanceof SlotRef) {
            final SlotRef slot = (SlotRef) expr;
            columns.include(slot.getId().asInt());
        } else {
            if (isAllSlot) {
                Preconditions.checkArgument(
                        false, "SlotRef tree contains other type expr.");
            }
        }
        expr.getChildren().stream().forEach((child) -> collectSlotId(child, columns, isAllSlot));
    }
}
