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

package org.apache.doris.sdk.load.internal;

import org.apache.doris.sdk.load.config.*;
import org.apache.http.client.methods.HttpPut;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

public class RequestBuilderTest {

    private DorisConfig buildConfig(GroupCommitMode gcm) {
        return DorisConfig.builder()
                .endpoints(Arrays.asList("http://127.0.0.1:8030"))
                .user("root").password("secret")
                .database("testdb").table("users")
                .format(new JsonFormat(JsonFormat.Type.OBJECT_LINE))
                .groupCommit(gcm)
                .build();
    }

    @Test
    public void testBasicAuthHeader() throws Exception {
        DorisConfig config = buildConfig(GroupCommitMode.OFF);
        HttpPut req = RequestBuilder.build(config, "{\"id\":1}".getBytes(), 0);
        String auth = req.getFirstHeader("Authorization").getValue();
        assertTrue(auth.startsWith("Basic "));
        // Base64("root:secret") = "cm9vdDpzZWNyZXQ="
        assertTrue(auth.contains("cm9vdDpzZWNyZXQ="));
    }

    @Test
    public void testUrlPattern() throws Exception {
        DorisConfig config = buildConfig(GroupCommitMode.OFF);
        HttpPut req = RequestBuilder.build(config, "test".getBytes(), 0);
        String url = req.getURI().toString();
        assertTrue(url.contains("/api/testdb/users/_stream_load"));
    }

    @Test
    public void testJsonFormatHeaders() throws Exception {
        DorisConfig config = buildConfig(GroupCommitMode.OFF);
        HttpPut req = RequestBuilder.build(config, "{}".getBytes(), 0);
        assertEquals("json", req.getFirstHeader("format").getValue());
        assertEquals("true", req.getFirstHeader("read_json_by_line").getValue());
    }

    @Test
    public void testLabelSetWhenGroupCommitOff() throws Exception {
        DorisConfig config = buildConfig(GroupCommitMode.OFF);
        HttpPut req = RequestBuilder.build(config, "test".getBytes(), 0);
        assertNotNull(req.getFirstHeader("label"));
    }

    @Test
    public void testLabelNotSetWhenGroupCommitAsync() throws Exception {
        DorisConfig config = buildConfig(GroupCommitMode.ASYNC);
        HttpPut req = RequestBuilder.build(config, "test".getBytes(), 0);
        assertNull(req.getFirstHeader("label"));
        assertEquals("async_mode", req.getFirstHeader("group_commit").getValue());
    }

    @Test
    public void testGzipHeader() throws Exception {
        DorisConfig config = DorisConfig.builder()
                .endpoints(Arrays.asList("http://127.0.0.1:8030"))
                .user("root").password("").database("db").table("t")
                .format(new CsvFormat(",", "\\n"))
                .enableGzip(true)
                .build();
        HttpPut req = RequestBuilder.build(config, "1,a".getBytes(), 0);
        assertEquals("gz", req.getFirstHeader("compress_type").getValue());
    }

    @Test
    public void testRetryLabelHasSuffix() throws Exception {
        DorisConfig config = buildConfig(GroupCommitMode.OFF);
        HttpPut req0 = RequestBuilder.build(config, "test".getBytes(), 0);
        HttpPut req1 = RequestBuilder.build(config, "test".getBytes(), 1);
        String label0 = req0.getFirstHeader("label").getValue();
        String label1 = req1.getFirstHeader("label").getValue();
        assertTrue(label1.contains("retry"), "retry label must contain 'retry'");
        assertNotEquals(label0, label1);
    }

    @Test
    public void testCustomLabelUsedOnFirstAttempt() throws Exception {
        DorisConfig config = DorisConfig.builder()
                .endpoints(Arrays.asList("http://127.0.0.1:8030"))
                .user("root").password("").database("db").table("t")
                .format(new JsonFormat(JsonFormat.Type.OBJECT_LINE))
                .label("my_custom_label")
                .build();
        HttpPut req = RequestBuilder.build(config, "test".getBytes(), 0);
        assertEquals("my_custom_label", req.getFirstHeader("label").getValue());
    }
}
