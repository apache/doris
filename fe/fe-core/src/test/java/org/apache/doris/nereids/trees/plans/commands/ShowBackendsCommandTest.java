package org.apache.doris.nereids.trees.plans.commands;

import mockit.Expectations;
import mockit.Mocked;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;


/**
 * @Author zhaorongsheng
 * @Date 2025/4/23 19:43
 * @Version 1.0
 */
public class ShowBackendsCommandTest extends TestWithFeService {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final String infoDB = InfoSchemaDb.DATABASE_NAME;

    @Mocked
    private Env env;
    @Mocked
    private ConnectContext ctx;
    @Mocked
    private AccessControllerManager accessControllerManager;

    private void runBefore() throws Exception {
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

                accessControllerManager.checkDbPriv(connectContext, internalCtl, infoDB, PrivPredicate.SELECT);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testNormal() throws Exception {
        runBefore();
        ShowBackendsCommand command = new ShowBackendsCommand();
        Assertions.assertDoesNotThrow(() -> command.run(connectContext, null));
    }

    @Test
    public void testNoPrivilege() throws Exception {
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

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.SHOW);
                accessControllerManager.checkDbPriv(connectContext, internalCtl, infoDB, PrivPredicate.SELECT);
                minTimes = 0;
                result = false;
            }
        };
        ShowBackendsCommand command = new ShowBackendsCommand();
        Assertions.assertThrows(AnalysisException.class, () -> command.run(connectContext, null));
    }
}
