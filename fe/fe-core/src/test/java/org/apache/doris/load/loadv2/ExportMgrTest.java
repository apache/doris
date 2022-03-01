package org.apache.doris.load.loadv2;

import com.google.common.collect.Maps;
import mockit.Mocked;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.ExportJob;
import org.apache.doris.load.ExportMgr;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExportMgrTest {
    private final ExportMgr exportMgr = new ExportMgr();

    @Mocked
    private PaloAuth auth;

    @Before
    public void setUp() {
        MockedAuth.mockedAuth(auth);
    }

    @Test
    public void testShowExport() throws Exception {

        ExportJob job1 = makeExportJob(1, "aabbcc");
        ExportJob job2 = makeExportJob(2, "aabbdd");
        ExportJob job3 = makeExportJob(3, "eebbcc");

        exportMgr.unprotectAddJob(job1);
        exportMgr.unprotectAddJob(job2);
        exportMgr.unprotectAddJob(job3);

        List<List<String>> r1 = exportMgr.getExportJobInfosByIdOrState(-1, 3, "", true, null, null, -1);
        Assert.assertEquals(r1.size(), 1);

        List<List<String>> r2 = exportMgr.getExportJobInfosByIdOrState(-1, 0, "", false, null, null, -1);
        Assert.assertEquals(r2.size(), 3);

        List<List<String>> r3 = exportMgr.getExportJobInfosByIdOrState(-1, 0, "aabbcc", false, null, null, -1);
        Assert.assertEquals(r3.size(), 1);

        List<List<String>> r4 = exportMgr.getExportJobInfosByIdOrState(-1, 0, "%bb%", true, null, null, -1);
        Assert.assertEquals(r4.size(), 3);

        List<List<String>> r5 = exportMgr.getExportJobInfosByIdOrState(-1, 0, "aabb%", true, null, null, -1);
        Assert.assertEquals(r5.size(), 2);

        List<List<String>> r6 = exportMgr.getExportJobInfosByIdOrState(-1, 0, "%dd", true, null, null, -1);
        Assert.assertEquals(r6.size(), 1);

    }

    private ExportJob makeExportJob(long id, String label) {
        ExportJob job1 = new ExportJob(id);
        Deencapsulation.setField(job1, "label", label);

        TableName tbl1 = new TableName("testCluster", "testDb");
        Deencapsulation.setField(job1, "tableName", tbl1);

        BrokerDesc bd = new BrokerDesc("broker", new HashMap<>());
        Deencapsulation.setField(job1, "brokerDesc", bd);

        Map<String, String> properties = Maps.newHashMap();
        properties.put(LoadStmt.EXEC_MEM_LIMIT, "-1");
        properties.put(LoadStmt.TIMEOUT_PROPERTY, "-1");
        Deencapsulation.setField(job1, "properties", properties);


        return job1;
    }

}
