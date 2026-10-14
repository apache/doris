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
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NereidsStreamLoadTaskTest {
    @Test
    public void testDefaultSendBatchParallelism() throws UserException {
        Assertions.assertEquals(1,
                NereidsStreamLoadTask.fromTStreamLoadPutRequest(newRequest()).getSendBatchParallelism());
    }

    @Test
    public void testSendBatchParallelismBoundary() throws UserException {
        for (int parallelism : new int[] {Integer.MIN_VALUE, -1, 0, 1, 256}) {
            TStreamLoadPutRequest request = newRequest();
            request.setSendBatchParallelism(parallelism);
            Assertions.assertEquals(parallelism,
                    NereidsStreamLoadTask.fromTStreamLoadPutRequest(request).getSendBatchParallelism());
        }
    }

    @Test
    public void testRejectExcessiveSendBatchParallelism() {
        for (int parallelism : new int[] {257, Integer.MAX_VALUE}) {
            TStreamLoadPutRequest request = newRequest();
            request.setSendBatchParallelism(parallelism);
            UserException exception = Assertions.assertThrows(UserException.class,
                    () -> NereidsStreamLoadTask.fromTStreamLoadPutRequest(request));
            Assertions.assertTrue(exception.getMessage().contains(
                    "send_batch_parallelism value should less than or equal 256, you set value is: " + parallelism));
        }
    }

    private TStreamLoadPutRequest newRequest() {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setLoadId(new TUniqueId(1, 2));
        request.setTxnId(3);
        request.setFileType(TFileType.FILE_STREAM);
        request.setFormatType(TFileFormatType.FORMAT_CSV_PLAIN);
        request.setCompressType(TFileCompressType.UNKNOWN);
        return request;
    }
}
