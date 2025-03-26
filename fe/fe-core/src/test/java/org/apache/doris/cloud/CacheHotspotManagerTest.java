package org.apache.doris.cloud;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class CacheHotspotManagerTest {

    private CloudSystemInfoService cloudSystemInfoService;
    private CloudEnv cloudEnv;
    private Env env;
    private CacheHotspotManager cacheHotspotManager;

    @Before
    public void setUp() {
        SystemInfoService clusterInfo = Env.getClusterInfo();
    }

    @Test
    public void testBasicEnvironmentSetup() {
        // 验证基础环境是否正确设置
        assertNotNull(env);
        assertNotNull(cloudEnv);
        assertNotNull(cacheHotspotManager);
    }
}