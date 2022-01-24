package org.apache.doris.utframe;

import org.apache.doris.common.Config;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UtFrameUtilsTest {

    @Test
    public void testCreateDorisCluster() throws Exception {
        String runningDir = UtFrameUtils.generateRandomFeRunningDir(UtFrameUtilsTest.class);
        assertTrue(runningDir.startsWith("fe/mocked/UtFrameUtilsTest/"));
        Map<String, String> feConfMap = new HashMap<>();
        feConfMap.put("cluster_name", "Doris Test");
        feConfMap.put("enable_complex_type_support", "true");
        UtFrameUtils.createDorisCluster(runningDir, feConfMap);
        assertEquals("Doris Test", Config.cluster_name);
        assertTrue(Config.enable_complex_type_support);
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }
}
