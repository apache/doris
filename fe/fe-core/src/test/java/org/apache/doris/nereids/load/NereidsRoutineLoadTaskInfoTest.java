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

package org.apache.doris.nereids.load;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.property.fileformat.JsonFileFormatProperties;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class NereidsRoutineLoadTaskInfoTest {

    private NereidsRoutineLoadTaskInfo buildTaskInfo(Map<String, String> jobProperties) {
        return new NereidsRoutineLoadTaskInfo(0L, jobProperties, 10L, null,
                LoadTask.MergeType.APPEND, null, null, 0.0,
                new NereidsLoadTaskInfo.NereidsImportColumnDescs(), null, null, null,
                null, (byte) 0, (byte) 0, 1, false, TUniqueKeyUpdateMode.UPSERT,
                TPartialUpdateNewRowPolicy.APPEND, false);
    }

    @Test
    public void testIsFillMissingColumnsTrue() {
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS, "true");
        NereidsRoutineLoadTaskInfo taskInfo = buildTaskInfo(jobProperties);
        Assertions.assertTrue(taskInfo.isFillMissingColumns());
    }

    @Test
    public void testIsFillMissingColumnsFalse() {
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS, "false");
        NereidsRoutineLoadTaskInfo taskInfo = buildTaskInfo(jobProperties);
        Assertions.assertFalse(taskInfo.isFillMissingColumns());
    }

    @Test
    public void testIsFillMissingColumnsDefault() {
        NereidsRoutineLoadTaskInfo taskInfo = buildTaskInfo(new HashMap<>());
        Assertions.assertFalse(taskInfo.isFillMissingColumns());
    }

    @Test
    public void testFillMissingColumnsPropagatedToDataDescriptionAnalysisMap() {
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS, "true");
        NereidsRoutineLoadTaskInfo taskInfo = buildTaskInfo(jobProperties);

        NereidsDataDescription desc = new NereidsDataDescription("tbl", taskInfo);
        Map<String, String> analysisMap = Deencapsulation.getField(desc, "analysisMap");
        Assertions.assertEquals("true",
                analysisMap.get(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS));
    }

    @Test
    public void testFillMissingColumnsDefaultFalseInDataDescriptionAnalysisMap() {
        NereidsRoutineLoadTaskInfo taskInfo = buildTaskInfo(new HashMap<>());
        NereidsDataDescription desc = new NereidsDataDescription("tbl", taskInfo);
        Map<String, String> analysisMap = Deencapsulation.getField(desc, "analysisMap");
        Assertions.assertEquals("false",
                analysisMap.get(JsonFileFormatProperties.PROP_FILL_MISSING_COLUMNS));
    }
}
