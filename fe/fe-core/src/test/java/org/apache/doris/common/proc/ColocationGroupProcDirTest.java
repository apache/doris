package org.apache.doris.common.proc;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ColocationGroup;
import org.apache.doris.common.GroupId;
import org.apache.doris.datasource.CloudReplica;
import org.apache.doris.persist.ColocateTableIndex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// 使用PowerMockRunner是因为需要Mock静态方法 CloudReplica.isCloudEnv()
@RunWith(PowerMockRunner.class)
// 告诉PowerMock需要处理CloudReplica和Env这两个类
@PrepareForTest({CloudReplica.class, Env.class})
public class ColocationGroupProcDirTest {

    private long dbId = 10001L;
    private long grpId = 20001L;
    private ColocationGroupProcDir procDir;

    @Before
    public void setUp() {
        // 每个测试方法运行前都会执行这里
        // 实例化要测试的类
        procDir = new ColocationGroupProcDir(dbId, grpId);
    }

    /**
     * 测试场景：云环境下，测试是否正确构建了BE序列
     * 对应PR中新增的 buildCloudBeSeqs 逻辑
     */
    @Test
    public void testLookupInCloudEnv() throws Exception {
        // 1. 准备模拟数据
        // 假设云环境返回了3个BE ID: 101, 102, 103
        List<Long> mockCloudBeIds = new ArrayList<>();
        mockCloudBeIds.add(101L);
        mockCloudBeIds.add(102L);
        mockCloudBeIds.add(103L);

        // 2. Mock静态类
        PowerMockito.mockStatic(CloudReplica.class);
        
        // 3. 设定行为：当调用 CloudReplica.isCloudEnv() 时，返回 true (模拟是云环境)
        when(CloudReplica.isCloudEnv()).thenReturn(true);
        
        // 4. 设定行为：当调用 CloudReplica.getColocatedBeId() 时，返回我们准备的数据
        when(CloudReplica.getColocatedBeId(dbId, grpId)).thenReturn(mockCloudBeIds);

        // 5. 执行测试：调用 lookup 方法（传入 "dbId.grpId" 格式的字符串）
        String groupIdStr = dbId + "." + grpId;
        ProcNodeInterface result = procDir.lookup(groupIdStr);

        // 6. 验证结果
        // 期望结果不为空
        Assert.assertNotNull(result);
        // 期望结果是 ColocationGroupBackendSeqsProcNode 类型
        Assert.assertTrue(result instanceof ColocationGroupBackendSeqsProcNode);

    }

    /**
     * 测试场景：非云环境下，是否保持原有的逻辑
     */
    @Test
    public void testLookupInNonCloudEnv() throws Exception {
        // 1. Mock ColocateTableIndex (原有逻辑依赖这个类)
        ColocateTableIndex mockIndex = mock(ColocateTableIndex.class);
        
        // 2. Mock静态类
        PowerMockito.mockStatic(CloudReplica.class);
        PowerMockito.mockStatic(Env.class);

        // 3. 设定行为：是非云环境
        when(CloudReplica.isCloudEnv()).thenReturn(false);
        
        // 4. 设定行为：Env.getCurrentColocateIndex() 返回我们的mock对象
        when(Env.getCurrentColocateIndex()).thenReturn(mockIndex);

        // 5. 执行测试
        String groupIdStr = dbId + "." + grpId;
        procDir.lookup(groupIdStr);

   
    }
}
