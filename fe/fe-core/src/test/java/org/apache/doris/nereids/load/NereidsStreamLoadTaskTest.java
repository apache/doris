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

import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class NereidsStreamLoadTaskTest {
    @Test
    public void testMultiTableBaseTaskCopiesJsonProperties() throws Exception {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setLoadId(new TUniqueId(1, 2));
        request.setTxnId(3);
        request.setFileType(TFileType.FILE_STREAM);
        request.setFormatType(TFileFormatType.FORMAT_JSON);
        request.setCompressType(TFileCompressType.PLAIN);

        NereidsStreamLoadTask streamLoadTask = NereidsStreamLoadTask.fromTStreamLoadPutRequest(request);
        LoadTaskInfo routineLoadTask = Mockito.mock(LoadTaskInfo.class);
        Mockito.when(routineLoadTask.getFormatType()).thenReturn(TFileFormatType.FORMAT_JSON);
        Mockito.when(routineLoadTask.getJsonPaths()).thenReturn(
                "[\"$.meta.id\", \"$.meta.ts\", \"$.value.score\", \"$.value.region\"]");
        Mockito.when(routineLoadTask.getJsonRoot()).thenReturn("$.payload.items");
        Mockito.when(routineLoadTask.isStripOuterArray()).thenReturn(true);
        Mockito.when(routineLoadTask.isNumAsString()).thenReturn(true);

        streamLoadTask.setMultiTableBaseTaskInfo(routineLoadTask);

        Assertions.assertEquals(TFileFormatType.FORMAT_JSON, streamLoadTask.getFormatType());
        Assertions.assertEquals(
                "[\"$.meta.id\", \"$.meta.ts\", \"$.value.score\", \"$.value.region\"]",
                streamLoadTask.getJsonPaths());
        Assertions.assertEquals("$.payload.items", streamLoadTask.getJsonRoot());
        Assertions.assertTrue(streamLoadTask.isStripOuterArray());
        Assertions.assertTrue(streamLoadTask.isNumAsString());
    }
}
