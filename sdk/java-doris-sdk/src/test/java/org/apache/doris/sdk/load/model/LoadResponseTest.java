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

package org.apache.doris.sdk.load.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class LoadResponseTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String SUCCESS_JSON = "{"
            + "\"TxnId\":1001,"
            + "\"Label\":\"load_test_20260319_abc123\","
            + "\"Status\":\"Success\","
            + "\"TwoPhaseCommit\":\"false\","
            + "\"Message\":\"\","
            + "\"NumberTotalRows\":100,"
            + "\"NumberLoadedRows\":100,"
            + "\"NumberFilteredRows\":0,"
            + "\"NumberUnselectedRows\":0,"
            + "\"LoadBytes\":2048,"
            + "\"LoadTimeMs\":350,"
            + "\"BeginTxnTimeMs\":5,"
            + "\"StreamLoadPutTimeMs\":10,"
            + "\"ReadDataTimeMs\":200,"
            + "\"WriteDataTimeMs\":130,"
            + "\"CommitAndPublishTimeMs\":5,"
            + "\"ErrorURL\":\"\""
            + "}";

    @Test
    public void testDeserializeSuccess() throws Exception {
        RespContent resp = mapper.readValue(SUCCESS_JSON, RespContent.class);
        assertEquals(1001L, resp.getTxnId());
        assertEquals("load_test_20260319_abc123", resp.getLabel());
        assertEquals("Success", resp.getStatus());
        assertEquals(100L, resp.getNumberLoadedRows());
        assertEquals(2048L, resp.getLoadBytes());
        assertEquals(350, resp.getLoadTimeMs());
    }

    @Test
    public void testLoadResponseSuccess() {
        RespContent resp = new RespContent();
        resp.setStatus("Success");
        LoadResponse response = LoadResponse.success(resp);
        assertEquals(LoadResponse.Status.SUCCESS, response.getStatus());
        assertNull(response.getErrorMessage());
        assertNotNull(response.getRespContent());
    }

    @Test
    public void testLoadResponseFailure() {
        RespContent resp = new RespContent();
        resp.setStatus("Fail");
        resp.setMessage("table not found");
        LoadResponse response = LoadResponse.failure(resp, "load failed. cause by: table not found");
        assertEquals(LoadResponse.Status.FAILURE, response.getStatus());
        assertEquals("load failed. cause by: table not found", response.getErrorMessage());
    }
}
