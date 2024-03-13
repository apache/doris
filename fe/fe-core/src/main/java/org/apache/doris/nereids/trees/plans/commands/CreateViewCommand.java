package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.CreateViewInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

public class CreateViewCommand extends Command {
    private final CreateViewInfo createViewInfo;

    public CreateViewCommand(CreateViewInfo createViewInfo) {
        super(PlanType.CREATE_VIEW_COMMAND);
        this.createViewInfo = createViewInfo;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {

    }

}
