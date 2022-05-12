// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.load.loadv2;

import org.apache.doris.common.LoadException;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationReportPBImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

/**
 * Covert output string of command `yarn application -status` to application report.
 * Input sample:
 * -------------------
 * Application Report :
 *   Application-Id : application_1573630236805_6763648
 *   Application-Name : doris_label_test
 *   Application-Type : SPARK-2.4.1
 *   User : test
 *   Queue : test-queue
 *   Start-Time : 1597654469958
 *   Finish-Time : 1597654801939
 *   Progress : 100%
 *   State : FINISHED
 *   Final-State : SUCCEEDED
 *   Tracking-URL : 127.0.0.1:8004/history/application_1573630236805_6763648/1
 *   RPC Port : 40236
 *   AM Host : host-name
 * ------------------
 *
 * Output:
 *   ApplicationReport
 */
public class YarnApplicationReport {
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
    private static final String DIAGNOSTICS = "Diagnostics";

    private ApplicationReport report;

    public YarnApplicationReport(String output) throws LoadException {
        this.report = new ApplicationReportPBImpl();
        parseFromOutput(output);
    }

    public ApplicationReport getReport() {
        return report;
    }

    private void parseFromOutput(String output) throws LoadException {
        Map<String, String> reportMap = Maps.newHashMap();
        List<String> lines = Splitter.onPattern("\n").trimResults().splitToList(output);
        // Application-Id : application_1573630236805_6763648 ==> (Application-Id, application_1573630236805_6763648)
        for (String line : lines) {
            List<String> entry = Splitter.onPattern(":").limit(2).trimResults().splitToList(line);
            Preconditions.checkState(entry.size() <= 2, line);
            if (entry.size() > 1) {
                reportMap.put(entry.get(0), entry.get(1));
            } else {
                reportMap.put(entry.get(0), "");
            }
        }

        try {
            report.setApplicationId(ConverterUtils.toApplicationId(reportMap.get(APPLICATION_ID)));
            report.setName(reportMap.get(APPLICATION_NAME));
            report.setApplicationType(reportMap.get(APPLICATION_TYPE));
            report.setUser(reportMap.get(USER));
            report.setQueue(reportMap.get(QUEUE));
            report.setStartTime(Long.parseLong(reportMap.get(START_TIME)));
            report.setFinishTime(Long.parseLong(reportMap.get(FINISH_TIME)));
            report.setProgress(NumberFormat.getPercentInstance().parse(reportMap.get(PROGRESS)).floatValue());
            report.setYarnApplicationState(YarnApplicationState.valueOf(reportMap.get(STATE)));
            report.setFinalApplicationStatus(FinalApplicationStatus.valueOf(reportMap.get(FINAL_STATE)));
            report.setTrackingUrl(reportMap.get(TRACKING_URL));
            report.setRpcPort(Integer.parseInt(reportMap.get(RPC_PORT)));
            report.setHost(reportMap.get(AM_HOST));
            report.setDiagnostics(reportMap.get(DIAGNOSTICS));
        } catch (NumberFormatException | ParseException e) {
            throw new LoadException(e.getMessage());
        } catch (Exception e) {
            throw new LoadException(e.getMessage());
        }
    }
}
