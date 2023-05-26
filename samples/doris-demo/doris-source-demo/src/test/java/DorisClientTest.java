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

import org.apache.doris.sdk.DorisClient;
import org.apache.doris.sdk.serialization.RowBatch;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;

import org.apache.thrift.TException;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class DorisClientTest {
    private final AtomicBoolean eos = new AtomicBoolean(false);
    private int offset = 0;

    @Test
    public void testClient() throws TException, IOException {
        DorisClient dorisClient = new DorisClient(buildRequiredParams());
        dorisClient.openScanner();
        TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
        nextBatchParams.setContextId(dorisClient.getContextId());
        while (!eos.get()) {
            nextBatchParams.setOffset(offset);
            TScanBatchResult nextResult = dorisClient.getNext(nextBatchParams);
            eos.set(nextResult.isEos());
            if (!eos.get()) {
                RowBatch rowBatch = new RowBatch(nextResult, dorisClient.getSchema()).readArrow();
                offset += rowBatch.getReadRowCount();
                List<Object> nextObject = rowBatch.next();
                System.out.println(nextObject);
                rowBatch.close();
            }
        }
        dorisClient.close();
    }

    private Map<String, String> buildRequiredParams() {
        Map<String, String> requiredParams = new HashMap<>();
        requiredParams.put("fe_host", "127.0.0.1");
        requiredParams.put("fe_http_port", "8030");
        requiredParams.put("database", "test");
        requiredParams.put("table", "test_source");
        requiredParams.put("username", "root");
        requiredParams.put("password", "");
        requiredParams.put("read_field", "id, name");
        requiredParams.put("filter_query", "id=1");
        return requiredParams;
    }

}
