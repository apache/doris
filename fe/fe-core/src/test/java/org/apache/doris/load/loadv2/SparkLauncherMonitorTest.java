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

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class SparkLauncherMonitorTest {
    private String appId;
    private SparkLoadAppHandle.State state;
    private String queue;
    private long startTime;
    private FinalApplicationStatus finalApplicationStatus;
    private String trackingUrl;
    private String user;
    private String logPath;

    @Before
    public void setUp() {
        appId = "application_1573630236805_6864759";
        state = SparkLoadAppHandle.State.RUNNING;
        queue = "spark-queue";
        startTime = 1597916263384L;
        finalApplicationStatus = FinalApplicationStatus.UNDEFINED;
        trackingUrl = "http://myhost:8388/proxy/application_1573630236805_6864759/";
        user = "testugi";
        logPath = "./spark-launcher.log";
    }

    @Test
    public void testLogMonitorNormal() {
        URL log = getClass().getClassLoader().getResource("spark_launcher_monitor.log");
        String cmd = "cat " + log.getPath();
        SparkLoadAppHandle handle = null;
        try {
            Process process = Runtime.getRuntime().exec(cmd);
            handle = new SparkLoadAppHandle(process);
            SparkLauncherMonitor.LogMonitor logMonitor = SparkLauncherMonitor.createLogMonitor(handle);
            logMonitor.setRedirectLogPath(logPath);
            logMonitor.start();
            try {
                logMonitor.join();
            } catch (InterruptedException e) {
            }
        } catch (IOException e) {
            Assert.fail();
        }

        // check values
        Assert.assertEquals(appId, handle.getAppId());
        Assert.assertEquals(state, handle.getState());
        Assert.assertEquals(queue, handle.getQueue());
        Assert.assertEquals(startTime, handle.getStartTime());
        Assert.assertEquals(finalApplicationStatus, handle.getFinalStatus());
        Assert.assertEquals(trackingUrl, handle.getUrl());
        Assert.assertEquals(user, handle.getUser());

        // check log
        File file = new File(logPath);
        Assert.assertTrue(file.exists());
    }

    @After
    public void clear() {
        File file = new File(logPath);
        if (file.exists()) {
            file.delete();
        }
    }
}
