package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class ShowMetaServicesCommandTest {
    @Test
    public void testShowMetaServices() throws Exception {
        ConnectContext ctx = new ConnectContext(null);
        ShowMetaServicesCommand command = new ShowMetaServicesCommand();
        StmtExecutor executor = new StmtExecutor(ctx, null);
        ShowResultSet result = command.doRun(ctx, executor);
        assertEquals(1, result.getRows().size());
        assertEquals("127.0.0.1", result.getRows().get(0).get(0));
    }
}
