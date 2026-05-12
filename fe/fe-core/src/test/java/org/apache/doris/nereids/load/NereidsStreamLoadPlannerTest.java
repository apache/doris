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

import org.apache.doris.common.UserException;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TMergeType;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class NereidsStreamLoadPlannerTest {
    @Test
    public void testValidateFlexiblePartialUpdateStreamLoadOptions() throws Exception {
        NereidsStreamLoadPlanner.validateLoadTaskForFlexiblePartialUpdate(newFlexibleTaskInfo());

        NereidsLoadTaskInfo csvTaskInfo = newFlexibleTaskInfo();
        Mockito.when(csvTaskInfo.getFormatType()).thenReturn(TFileFormatType.FORMAT_CSV_PLAIN);
        assertRejected(csvTaskInfo, "flexible partial update only support json format");

        NereidsLoadTaskInfo fuzzyParseTaskInfo = newFlexibleTaskInfo();
        Mockito.when(fuzzyParseTaskInfo.isFuzzyParse()).thenReturn(true);
        assertRejected(fuzzyParseTaskInfo, "Don't support flexible partial update when 'fuzzy_parse' is enabled");

        NereidsLoadTaskInfo columnsTaskInfo = newFlexibleTaskInfo();
        NereidsLoadTaskInfo.NereidsImportColumnDescs columnDescs =
                new NereidsLoadTaskInfo.NereidsImportColumnDescs();
        columnDescs.descs.add(new NereidsImportColumnDesc("k"));
        Mockito.when(columnsTaskInfo.getColumnExprDescs()).thenReturn(columnDescs);
        assertRejected(columnsTaskInfo, "Don't support flexible partial update when 'columns' is specified");

        NereidsLoadTaskInfo jsonPathsTaskInfo = newFlexibleTaskInfo();
        Mockito.when(jsonPathsTaskInfo.getJsonPaths()).thenReturn("[\"$.k\",\"$.v\"]");
        assertRejected(jsonPathsTaskInfo, "Don't support flexible partial update when 'jsonpaths' is specified");

        NereidsLoadTaskInfo hiddenColumnsTaskInfo = newFlexibleTaskInfo();
        Mockito.when(hiddenColumnsTaskInfo.getHiddenColumns())
                .thenReturn(Collections.singletonList("__DORIS_DELETE_SIGN__"));
        assertRejected(hiddenColumnsTaskInfo,
                "Don't support flexible partial update when 'hidden_columns' is specified");

        NereidsLoadTaskInfo sequenceColTaskInfo = newFlexibleTaskInfo();
        Mockito.when(sequenceColTaskInfo.hasSequenceCol()).thenReturn(true);
        assertRejected(sequenceColTaskInfo,
                "Don't support flexible partial update when 'function_column.sequence_col' is specified");

        NereidsLoadTaskInfo mergeTaskInfo = newFlexibleTaskInfo();
        Mockito.when(mergeTaskInfo.getMergeType()).thenReturn(LoadTask.MergeType.MERGE);
        assertRejected(mergeTaskInfo, "Don't support flexible partial update when 'merge_type' is specified");

        NereidsLoadTaskInfo explicitAppendTaskInfo = newFlexibleTaskInfo();
        Mockito.when(explicitAppendTaskInfo.isMergeTypeSpecified()).thenReturn(true);
        assertRejected(explicitAppendTaskInfo,
                "Don't support flexible partial update when 'merge_type' is specified");

        NereidsLoadTaskInfo whereTaskInfo = newFlexibleTaskInfo();
        Mockito.when(whereTaskInfo.getWhereExpr()).thenReturn(new UnboundSlot("v"));
        assertRejected(whereTaskInfo, "Don't support flexible partial update when 'where' is specified");

        NereidsLoadTaskInfo deleteTaskInfo = newFlexibleTaskInfo();
        Mockito.when(deleteTaskInfo.getDeleteCondition()).thenReturn(new UnboundSlot("v"));
        assertRejected(deleteTaskInfo, "Don't support flexible partial update when 'delete' is specified");
    }

    @Test
    public void testValidateFlexiblePartialUpdateRoutineLoadOptions() throws Exception {
        NereidsStreamLoadPlanner.validateLoadTaskForFlexiblePartialUpdate(
                newFlexibleRoutineTaskInfo(LoadTask.MergeType.APPEND, null, null));

        assertRejected(newFlexibleRoutineTaskInfo(LoadTask.MergeType.MERGE, null, null),
                "Don't support flexible partial update when 'merge_type' is specified");
        assertRejected(newFlexibleRoutineTaskInfo(LoadTask.MergeType.APPEND, new UnboundSlot("v"), null),
                "Don't support flexible partial update when 'where' is specified");
        assertRejected(newFlexibleRoutineTaskInfo(LoadTask.MergeType.APPEND, null, "seq"),
                "Don't support flexible partial update when 'function_column.sequence_col' is specified");
    }

    @Test
    public void testValidateFlexiblePartialUpdateStreamLoadDefaultMergeType() throws Exception {
        TStreamLoadPutRequest request = newFlexibleStreamLoadRequest();
        request.setMergeType(TMergeType.APPEND);
        NereidsStreamLoadTask defaultAppendTask = NereidsStreamLoadTask.fromTStreamLoadPutRequest(request);
        Assertions.assertFalse(defaultAppendTask.isMergeTypeSpecified());
        NereidsStreamLoadPlanner.validateLoadTaskForFlexiblePartialUpdate(defaultAppendTask);

        request.setMergeTypeSpecified(true);
        NereidsStreamLoadTask explicitAppendTask = NereidsStreamLoadTask.fromTStreamLoadPutRequest(request);
        Assertions.assertTrue(explicitAppendTask.isMergeTypeSpecified());
        assertRejected(explicitAppendTask,
                "Don't support flexible partial update when 'merge_type' is specified");

        TStreamLoadPutRequest deleteRequest = newFlexibleStreamLoadRequest();
        deleteRequest.setMergeType(TMergeType.APPEND);
        deleteRequest.setDeleteCondition("v = 1");
        NereidsStreamLoadTask deleteTask = NereidsStreamLoadTask.fromTStreamLoadPutRequest(deleteRequest);
        Assertions.assertFalse(deleteTask.isMergeTypeSpecified());
        assertRejected(deleteTask, "Don't support flexible partial update when 'delete' is specified");
    }

    private NereidsLoadTaskInfo newFlexibleTaskInfo() {
        NereidsLoadTaskInfo taskInfo = Mockito.mock(NereidsLoadTaskInfo.class);
        Mockito.when(taskInfo.getFormatType()).thenReturn(TFileFormatType.FORMAT_JSON);
        Mockito.when(taskInfo.getColumnExprDescs()).thenReturn(new NereidsLoadTaskInfo.NereidsImportColumnDescs());
        Mockito.when(taskInfo.getJsonPaths()).thenReturn("");
        Mockito.when(taskInfo.getMergeType()).thenReturn(LoadTask.MergeType.APPEND);
        return taskInfo;
    }

    private TStreamLoadPutRequest newFlexibleStreamLoadRequest() {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setLoadId(new TUniqueId(1, 2));
        request.setTxnId(1);
        request.setFileType(TFileType.FILE_STREAM);
        request.setFormatType(TFileFormatType.FORMAT_JSON);
        request.setCompressType(TFileCompressType.UNKNOWN);
        request.setReadJsonByLine(true);
        return request;
    }

    private NereidsRoutineLoadTaskInfo newFlexibleRoutineTaskInfo(
            LoadTask.MergeType mergeType, UnboundSlot whereExpr, String sequenceCol) {
        return new NereidsRoutineLoadTaskInfo(
                1024L, Collections.singletonMap("format", "json"), 10L, null, mergeType, null, sequenceCol, 1.0,
                new NereidsLoadTaskInfo.NereidsImportColumnDescs(), null, whereExpr, null, null,
                (byte) 0, (byte) 0, 1, false, TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS,
                TPartialUpdateNewRowPolicy.APPEND, false);
    }

    private void assertRejected(NereidsLoadTaskInfo taskInfo, String expectedMessage) {
        UserException exception = Assertions.assertThrows(UserException.class,
                () -> NereidsStreamLoadPlanner.validateLoadTaskForFlexiblePartialUpdate(taskInfo));
        Assertions.assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
    }
}
