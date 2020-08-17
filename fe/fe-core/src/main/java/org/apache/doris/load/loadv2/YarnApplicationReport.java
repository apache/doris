package org.apache.doris.load.loadv2;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationReportPBImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.util.List;
import java.util.Map;

/**
 * Covert output string of command `yarn application -status` to application report.
 * Input sample:
 * -------------------
 * Application Report :
 * 	Application-Id : application_1573630236805_6763648
 * 	Application-Name : doris_label_test
 * 	Application-Type : SPARK-2.4.1
 * 	User : test
 * 	Queue : test-queue
 * 	Start-Time : 1597654469958
 * 	Finish-Time : 1597654801939
 * 	Progress : 100%
 * 	State : FINISHED
 * 	Final-State : SUCCEEDED
 * 	Tracking-URL : 127.0.0.1:8004/history/application_1573630236805_6763648/1
 * 	RPC Port : 40236
 * 	AM Host : host-name
 * 	------------------
 *
 * 	Output:
 * 	ApplicationReport
 */
public class YarnApplicationReport {
    private ApplicationReport report;
    private static final String APPLICATION_ID = "Application-Id";
    private static final String APPLICATION_TYPE = "Application-Type";
    private static final String APPLICATION_NAME = "Application-Name";
    private static final String USER = "User";
    private static final String QUEUE = "Queue";
    private static final String START_TIME = "Start-Time";
    private static final String FINISH_TIME = "Finish-Time";
    private static final String PROGRESS = "Progress";
    private static final String STATE = "State";
    private static final String FINAL_STATE = "Final-State";
    private static final String TRACKING_URL = "Tracking-URL";
    private static final String RPC_PORT = "RPC Port";
    private static final String AM_HOST = "AM Host";

    public YarnApplicationReport(String output) {
        this.report = new ApplicationReportPBImpl();
        getReportFromOutput(output);
    }

    public ApplicationReport getReport() {
        return report;
    }

    private void getReportFromOutput(String output) {
        Map<String, String> reportMap = Maps.newHashMap();
        List<String> lines = Splitter.onPattern(",").trimResults().splitToList(output);
        for (String line : lines) {
            List<String> entry = Splitter.onPattern(":").trimResults().splitToList(line);
            if (entry.size() != 2) {
                continue;
            }
            reportMap.put(entry.get(0), entry.get(1));
        }

        report.setApplicationId(ConverterUtils.toApplicationId(reportMap.get(APPLICATION_ID)));
        report.setName(reportMap.get(APPLICATION_NAME));
        report.setApplicationType(reportMap.get(APPLICATION_TYPE));
        report.setUser(reportMap.get(USER));
        report.setQueue(reportMap.get(QUEUE));
        report.setStartTime(Long.parseLong(reportMap.get(START_TIME)));
        report.setFinishTime(Long.parseLong(reportMap.get(FINISH_TIME)));
        report.setProgress(Float.parseFloat(reportMap.get(PROGRESS)));
        report.setYarnApplicationState(YarnApplicationState.valueOf(reportMap.get(STATE)));
        report.setFinalApplicationStatus(FinalApplicationStatus.valueOf(reportMap.get(FINAL_STATE)));
        report.setTrackingUrl(reportMap.get(TRACKING_URL));
        report.setRpcPort(Integer.parseInt(reportMap.get(RPC_PORT)));
        report.setHost(reportMap.get(AM_HOST));
    }
}
