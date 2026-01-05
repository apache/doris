// TestColocationGroupProcNode.java
package org.apache.doris.proc;

import org.apache.doris.catalog.ColocationGroup;
import org.apache.doris.catalog.ColocationGroupMgr;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TestColocationGroupProcNode {

    private long testGroupId;

    @Before
    public void setUp() throws Exception {
        // 初始化测试环境
        UtFrameUtils.createMinimalDorisCluster();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ConnectContext.setCurrent(ctx);

        // 创建测试用Colocation Group
        ColocationGroupMgr cgMgr = Env.getCurrentEnv().getColocationGroupMgr();
        ColocationGroup testGroup = new ColocationGroup(1000L, "test_cg", 100L, 
                List.of(10001L), 3, "id", true);
        cgMgr.addColocationGroup(testGroup);
        testGroupId = testGroup.getId();
    }

    @Test
    public void testFetchResult() throws AnalysisException {
        // 测试存在的Group ID
        ColocationGroupProcNode procNode = new ColocationGroupProcNode(testGroupId);
        ProcResult result = procNode.fetchResult();
        
        // 验证返回结果
        Assert.assertEquals(ColocationGroupProcNode.TITLE_NAMES, result.getNames());
        Assert.assertEquals(1, result.getRows().size());
        
        List<String> row = result.getRows().get(0);
        Assert.assertEquals(String.valueOf(testGroupId), row.get(0));
        Assert.assertEquals("test_cg", row.get(1));
        Assert.assertEquals("100", row.get(2));

        // 测试不存在的Group ID
        try {
            ColocationGroupProcNode invalidProcNode = new ColocationGroupProcNode(9999L);
            invalidProcNode.fetchResult();
            Assert.fail("Should throw AnalysisException for invalid group id");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("not found"));
        }
    }
}
