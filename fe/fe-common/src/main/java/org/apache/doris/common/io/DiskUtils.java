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

package org.apache.doris.common.io;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

@Slf4j
public class DiskUtils {

    public static class Df {
        public String fileSystem = "";
        public long blocks = 0L;
        public long used = 0L;
        public long available = 0L;
        public int useRate = 0;
        public String mountedOn = "";
    }

    public static Df df(String dir) {
        String os = System.getProperty("os.name");
        if (os.startsWith("Windows")) {
            Df df = new Df();
            df.available = Long.MAX_VALUE / 1024;
            return df;
        }


        Process process;
        try {
            process = Runtime.getRuntime().exec("df -k " + dir);
            InputStream inputStream = process.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            Df df = new Df();
            // Filesystem      1K-blocks       Used Available Use% Mounted on
            // /dev/sdc       5814186096 5814169712         0 100% /home/spy-sd/sdc
            String titleLine = reader.readLine();
            String dataLine = reader.readLine();
            if (titleLine == null || dataLine == null) {
                return df;
            }
            String[] dfValues = dataLine.split("\\s+");
            if (os.startsWith("Mac")) {
                parseMacOSDiskInfo(dfValues, df);
            } else {
                parseLinuxDiskInfo(dfValues, df);
            }
            return df;
        } catch (IOException e) {
            log.info("failed to obtain disk information", e);
            return new Df();
        }
    }

    /**
     * Linux df -k output
     * Filesystem     1K-blocks     Used Available Use% Mounted on
     * /dev/sda1       8256952  2094712   5742232  27% /
     */
    private static void parseLinuxDiskInfo(String[] dfValues, Df df) {
        if (dfValues.length != 6) {
            return;
        }
        df.fileSystem = dfValues[0];
        df.blocks = parseLongValue(dfValues[1]);
        df.used = parseLongValue(dfValues[2]);
        df.available = parseLongValue(dfValues[3]);
        df.useRate = parseIntegerValue(dfValues[4].replace("%", ""));
        df.mountedOn = dfValues[5];
    }

    /**
     * MacOS df -k output
     * Filesystem    1024-blocks      Used Available Capacity iused      ifree %iused  Mounted on
     * /dev/disk1s1   488555536  97511104  390044432    20%  121655 4884849711    0%   /
     */
    private static void parseMacOSDiskInfo(String[] dfValues, Df df) {
        if (dfValues.length != 9) {
            return;
        }
        df.fileSystem = dfValues[0];
        df.blocks = Long.parseLong(dfValues[1]);
        df.used = parseLongValue(dfValues[2]);
        df.available = parseLongValue(dfValues[3]);
        df.useRate = parseIntegerValue(dfValues[4].replace("%", ""));
        df.mountedOn = dfValues[8];
    }

    private static long parseLongValue(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            log.info("failed to parse long value", e);
            return 0L;
        }
    }

    private static int parseIntegerValue(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.info("failed to parse integer value", e);
            return 0;
        }
    }

    private static String[] units = new String[]{"", "K", "M", "G", "T", "P"};

    public static String sizeFormat(long size) {
        int unitPos = 0;
        while (size >= 1024 && unitPos < units.length - 1) {
            unitPos++;
            size /= 1024;
        }
        return size + units[unitPos];
    }
}
