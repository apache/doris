package org.apache.doris.nereids.trees.plans.commands;

import mockit.Expectations;
import mockit.Mocked;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KillConnectionCommandTest {

    @Mocked
    private ConnectContext connectContext;
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessControllerManager;
    private StmtExecutor stmtExecutor = new StmtExecutor(connectContext, "select 1");

    private void runBefore() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testKillConnection() throws Exception {
        runBefore();
        stmtExecutor.execute();
        String queryId = DebugUtil.printId(stmtExecutor.getContext().queryId());
        KillConnectionCommand command = new KillConnectionCommand(stmtExecutor.getContext().getConnectionId());
        Assertions.assertDoesNotThrow(() -> command.doRun(connectContext, stmtExecutor));
        Assertions.assertEquals(connectContext.getState().getStateType(), QueryState.MysqlStateType.OK);
    }

}
