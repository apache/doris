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

package org.apache.doris.trinoconnector;

import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.LinkedList;
import java.util.List;

/**
 * Utils for handling processes
 */
public class ProcessUtils {
    public static long getCurrentProcId() {
        try {
            return ManagementFactory.getRuntimeMXBean().getPid();
        } catch (Exception e) {
            throw new RuntimeException("Couldn't find PID of current JVM process.", e);
        }
    }

    public static List<Long> getChildProcessIds(long pid) {
        try {
            Process pgrep = (new ProcessBuilder("pgrep", "-P", String.valueOf(pid))).start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(pgrep.getInputStream()));
            List<Long> result = new LinkedList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                result.add(Long.valueOf(line.trim()));
            }
            pgrep.waitFor();
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Couldn't get child processes of PID " + pid, e);
        }
    }

    public static String getCommandLine(long pid) {
        try {
            return FileUtils.readFileToString(new File(String.format("/proc/%d/cmdline", pid))).trim();
        } catch (IOException e) {
            return null;
        }
    }

    public static void killProcess(long pid) {
        try {
            Process kill = (new ProcessBuilder("kill", "-9", String.valueOf(pid))).start();
            kill.waitFor();
        } catch (Exception e) {
            throw new RuntimeException("Couldn't kill process PID " + pid, e);
        }
    }
}
