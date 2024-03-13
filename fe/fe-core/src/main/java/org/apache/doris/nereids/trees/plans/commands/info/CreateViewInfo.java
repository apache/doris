package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.CreateViewStmt;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.util.List;

/**
 * CreateViewInfo
 */
public class CreateViewInfo {
    private final boolean ifNotExists;
    private final TableNameInfo viewName;
    private final String comment;
    private final LogicalPlan logicalQuery;
    private final String querySql;

    private final List<SimpleColumnDefinition> simpleColumnDefinitions;

    /** constructor*/
    public CreateViewInfo(boolean ifNotExists, TableNameInfo viewName, String comment, LogicalPlan logicalQuery,
            String querySql, List<SimpleColumnDefinition> simpleColumnDefinitions) {
        this.ifNotExists = ifNotExists;
        this.viewName = viewName;
        this.comment = comment;
        this.logicalQuery = logicalQuery;
        this.querySql = querySql;
        this.simpleColumnDefinitions = simpleColumnDefinitions;
    }

    public CreateViewStmt translateToLegacyStmt() {

    }
}
