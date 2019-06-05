package org.apache.doris.optimizer.utils;

import com.google.common.collect.Lists;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.base.OptColumnRef;
import org.apache.doris.optimizer.base.OptColumnRefFactory;
import org.apache.doris.optimizer.operator.*;

import java.util.Collection;
import java.util.List;

public class ExpressionFacotry {

    public static OptExpression createLogicalScanExpression(OlapTable table, OptColumnRefFactory factory) {
        final Collection<Column> metaColumns = table.getColumns();
        final List<OptColumnRef> columnRefs = Lists.newArrayList();
        for (Column column : metaColumns) {
            final OptColumnRef columnRef = factory.create(column.getName(), column.getType());
            columnRefs.add(columnRef);
        }
        return OptExpression.create(new OptLogicalScan(table, columnRefs));
    }

    public static OptExpression createLogicalAggregateExpression(
            List<OptColumnRef> groupBys, List<OptExpression> inputs, OptColumnRefFactory factory) {
        final List<OptExpression> projectElementExpressions = Lists.newArrayList();
        for (OptColumnRef columnRef : groupBys) {
            final OptItemColumnRef itemColumnRef = new OptItemColumnRef(columnRef);
            final OptExpression itemColumnRefExpression = OptExpression.create(itemColumnRef);
            final OptColumnRef newColumnRef = factory.create(columnRef.getType());
            final OptItemProjectElement projectElement = new OptItemProjectElement(newColumnRef);
            projectElementExpressions.add(OptExpression.create(projectElement, itemColumnRefExpression));

        }
        final OptItemProjectList projectList = new OptItemProjectList();
        final OptExpression projectListExpression = OptExpression.create(projectList, projectElementExpressions);
        final List<OptExpression> newInputs = Lists.newArrayList();
        newInputs.addAll(inputs);
        newInputs.add(projectListExpression);
        return OptExpression.create(new OptLogicalAggregate(groupBys), newInputs);
    }

}
