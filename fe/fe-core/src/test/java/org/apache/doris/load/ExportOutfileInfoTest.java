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

package org.apache.doris.load;

import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ExportOutfileInfoTest {

    @Test
    public void testAttachedFileUrls() {
        String prefix = "s3://bucket/export/fragment_";
        Map<String, String> singleFile = new HashMap<>();
        singleFile.put(OutFileClause.FILE_NUMBER, "1");
        singleFile.put(OutFileClause.TOTAL_ROWS, "10");
        singleFile.put(OutFileClause.FILE_SIZE, "100");
        singleFile.put(OutFileClause.URL, prefix + "*");
        singleFile.put(OutFileClause.SINGLE_FILE_URL, prefix + "0.csv.gz");
        singleFile.put(OutFileClause.WRITE_TIME_SEC, "0.001");
        singleFile.put(OutFileClause.WRITE_SPEED_KB, "97.66");

        Map<String, String> oldBackend = new HashMap<>(singleFile);
        oldBackend.remove(OutFileClause.SINGLE_FILE_URL);
        Map<String, String> multipleFiles = new HashMap<>(oldBackend);
        multipleFiles.put(OutFileClause.FILE_NUMBER, "2");
        Map<String, String> noFiles = new HashMap<>(oldBackend);
        noFiles.put(OutFileClause.FILE_NUMBER, "0");

        ExportTaskExecutor executor = new ExportTaskExecutor(Optional.empty(), null);
        List<OutfileInfo> infos = Deencapsulation.invoke(executor, "getOutFileInfo",
                Lists.newArrayList(singleFile, oldBackend, multipleFiles, noFiles));
        Assertions.assertEquals(4, infos.size());
        Assertions.assertEquals(prefix + "0.csv.gz", infos.get(0).getUrl());
        for (int i = 1; i < infos.size(); i++) {
            Assertions.assertEquals(prefix + "*", infos.get(i).getUrl());
        }
        Assertions.assertEquals("1", infos.get(0).getFileNumber());
        Assertions.assertEquals("10", infos.get(0).getTotalRows());
        Assertions.assertEquals("100", infos.get(0).getFileSize());
        Assertions.assertEquals("0.001", infos.get(0).getWriteTime());
        Assertions.assertEquals("97.66", infos.get(0).getWriteSpeed());
        Assertions.assertEquals(prefix + "*", singleFile.get(OutFileClause.URL));
        Assertions.assertEquals(6, OutFileClause.RESULT_COL_NAMES.size());
    }

    @Test
    public void testOutfileInfo() throws Exception {
        // outfileInfoList1
        OutfileInfo outfileInfo1 = new OutfileInfo();
        outfileInfo1.setFileNumber("2");
        outfileInfo1.setTotalRows("1234");
        outfileInfo1.setFileSize("10240");
        outfileInfo1.setUrl("file:///172.20.32.136/path/to/result2_c6df5f01bd664dde-a2168b019b6c2b3f_*");

        OutfileInfo outfileInfo2 = new OutfileInfo();
        outfileInfo2.setFileNumber("2");
        outfileInfo2.setTotalRows("1235");
        outfileInfo2.setFileSize("10250");
        outfileInfo2.setUrl("file:///172.20.32.136/path/to/result2_c6df5f01bd664dde-a2168b019b6c2b3f_*");

        List<OutfileInfo> outfileInfoList1 = Lists.newArrayList();
        outfileInfoList1.add(outfileInfo1);
        outfileInfoList1.add(outfileInfo2);

        // outfileInfoList2
        OutfileInfo outfileInfo3 = new OutfileInfo();
        outfileInfo3.setFileNumber("3");
        outfileInfo3.setTotalRows("2345");
        outfileInfo3.setFileSize("20260");
        outfileInfo3.setUrl("file:///172.20.32.137/path/to/result2_c6df5f01bd664dde-a2168b019b6c2b3f_*");

        OutfileInfo outfileInfo4 = new OutfileInfo();
        outfileInfo4.setFileNumber("3");
        outfileInfo4.setTotalRows("2346");
        outfileInfo4.setFileSize("20270");
        outfileInfo4.setUrl("file:///172.20.32.137/path/to/result2_c6df5f01bd664dde-a2168b019b6c2b3f_*");

        List<OutfileInfo> outfileInfoList2 = Lists.newArrayList();
        outfileInfoList2.add(outfileInfo3);
        outfileInfoList2.add(outfileInfo4);

        List<List<OutfileInfo>> allOutfileInfo = Lists.newArrayList();
        allOutfileInfo.add(outfileInfoList1);
        allOutfileInfo.add(outfileInfoList2);

        String showInfo = GsonUtils.GSON.toJson(allOutfileInfo);
        System.out.println(showInfo);
        Assertions.assertEquals(
                "[[{\"fileNumber\":\"2\",\"totalRows\":\"1234\",\"fileSize\":\"10240\","
                        + "\"url\":\"file:///172.20.32.136/path/to/result2_c6df5f01bd664dde-a2168b019b6c2b3f_*\"},"
                        + "{\"fileNumber\":\"2\",\"totalRows\":\"1235\",\"fileSize\":\"10250\","
                        + "\"url\":\"file:///172.20.32.136/path/to/result2_c6df5f01bd664dde-a2168b019b6c2b3f_*\"}],"
                        + "[{\"fileNumber\":\"3\",\"totalRows\":\"2345\",\"fileSize\":\"20260\","
                        + "\"url\":\"file:///172.20.32.137/path/to/result2_c6df5f01bd664dde-a2168b019b6c2b3f_*\"},"
                        + "{\"fileNumber\":\"3\",\"totalRows\":\"2346\",\"fileSize\":\"20270\","
                        + "\"url\":\"file:///172.20.32.137/path/to/result2_c6df5f01bd664dde-a2168b019b6c2b3f_*\"}]]",
                showInfo);
    }

}
