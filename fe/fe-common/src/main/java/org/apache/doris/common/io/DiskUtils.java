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
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class DiskUtils {

    public static class Df {
        public String fileSystem = "";
        public long blocks;
        public long used;
        public long available;
        public int useRate;
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
            process = Runtime.getRuntime().exec("df " + dir);
            InputStream inputStream = process.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            // Filesystem      1K-blocks       Used Available Use% Mounted on
            // /dev/sdc       5814186096 5814169712         0 100% /home/spy-sd/sdc
            String titleLine = reader.readLine();
            String dataLine = reader.readLine();
            if (titleLine == null || dataLine == null) {
                return null;
            }
            String[] values = dataLine.split("\\s+");
            if (values.length != 6) {
                return null;
            }

            Df df = new Df();
            df.fileSystem = values[0];
            df.blocks = Long.parseLong(values[1]);
            df.used = Long.parseLong(values[2]);
            df.available = Long.parseLong(values[3]);
            df.useRate = Integer.parseInt(values[4].replace("%", ""));
            df.mountedOn = values[5];
            return df;
        } catch (IOException e) {
            log.info("failed to obtain disk information", e);
            return null;
        }
    }

    private static List<String> getTitles(String titlesLine) {
        List<String> titles = new ArrayList<>();
        String[] titleArray = titlesLine.split("\\s+");
        for (String title : titleArray) {
            if (title.equalsIgnoreCase("on")) {
                if (!titles.isEmpty()) {
                    int lastIdx = titles.size() - 1;
                    titles.set(lastIdx, titles.get(lastIdx) + "On");
                }
            } else {
                titles.add(title);
            }
        }
        return titles;
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
