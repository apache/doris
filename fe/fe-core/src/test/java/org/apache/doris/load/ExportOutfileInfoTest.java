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

import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ExportOutfileInfoTest {

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
        Assert.assertEquals(
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
