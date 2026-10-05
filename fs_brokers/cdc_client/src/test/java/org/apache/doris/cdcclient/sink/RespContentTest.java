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

package org.apache.doris.cdcclient.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

class RespContentTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void parsesFirstErrorMsgFromFailedLoadResponse() throws Exception {
        String json =
                "{\"Status\":\"Fail\","
                        + "\"Message\":\"[DATA_QUALITY_ERROR]Encountered unqualified data.\","
                        + "\"ErrorURL\":\"https://host/error_log/abc\","
                        + "\"FirstErrorMsg\":\"column(event_qty) values is null while columns is"
                        + " not nullable.\"}";

        RespContent resp = MAPPER.readValue(json, RespContent.class);

        assertEquals("Fail", resp.getStatus());
        assertEquals(
                "column(event_qty) values is null while columns is not nullable.",
                resp.getFirstErrorMsg());
    }

    @Test
    void firstErrorMsgIsNullWhenAbsent() throws Exception {
        // Success responses omit FirstErrorMsg; must not blow up deserialization.
        RespContent resp =
                MAPPER.readValue("{\"Status\":\"Success\"}", RespContent.class);
        assertNull(resp.getFirstErrorMsg());
    }
}
