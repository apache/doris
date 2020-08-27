package org.apache.doris.load.loadv2;

import org.apache.doris.common.LoadException;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.junit.Assert;
import org.junit.Test;

public class YarnApplicationReportTest {
    private final String runningReport = "Application Report :\n" +
            "Application-Id : application_15888888888_0088\n" +
            "Application-Name : label0\n" +
            "Application-Type : SPARK-2.4.1\n" +
            "User : test\n" +
            "Queue : test-queue\n" +
            "Start-Time : 1597654469958\n" +
            "Finish-Time : 0\n" +
            "Progress : 50%\n" +
            "State : RUNNING\n" +
            "Final-State : UNDEFINED\n" +
            "Tracking-URL : http://127.0.0.1:8080/proxy/application_1586619723848_0088/\n" +
            "RPC Port : 40236\n" +
            "AM Host : host-name";

    @Test
    public void testParseToReport() {
        try {
            YarnApplicationReport yarnReport = new YarnApplicationReport(runningReport);
            ApplicationReport report = yarnReport.getReport();
            Assert.assertEquals("application_15888888888_0088", report.getApplicationId().toString());
            Assert.assertEquals("label0", report.getName());
            Assert.assertEquals("test", report.getUser());
            Assert.assertEquals("test-queue", report.getQueue());
            Assert.assertEquals(1597654469958L, report.getStartTime());
            Assert.assertEquals(0L, report.getFinishTime());
            Assert.assertTrue(report.getProgress() == 0.5f);
            Assert.assertEquals(YarnApplicationState.RUNNING, report.getYarnApplicationState());
            Assert.assertEquals(FinalApplicationStatus.UNDEFINED, report.getFinalApplicationStatus());
            Assert.assertEquals("http://127.0.0.1:8080/proxy/application_1586619723848_0088/", report.getTrackingUrl());
            Assert.assertEquals(40236, report.getRpcPort());
            Assert.assertEquals("host-name", report.getHost());

        } catch (LoadException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
