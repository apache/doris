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

package org.apache.doris.sdk.load.config;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

public class DorisConfigTest {

    private DorisConfig.Builder validBuilder() {
        return DorisConfig.builder()
                .endpoints(Arrays.asList("http://127.0.0.1:8030"))
                .user("root")
                .password("password")
                .database("test_db")
                .table("users")
                .format(new JsonFormat(JsonFormat.Type.OBJECT_LINE));
    }

    @Test
    public void testValidConfigBuilds() {
        DorisConfig config = validBuilder().build();
        assertNotNull(config);
        assertEquals("root", config.getUser());
        assertEquals("test_db", config.getDatabase());
        assertEquals("users", config.getTable());
    }

    @Test
    public void testMissingUserThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            DorisConfig.builder()
                    .endpoints(Arrays.asList("http://127.0.0.1:8030"))
                    .database("db").table("t")
                    .format(new JsonFormat(JsonFormat.Type.OBJECT_LINE))
                    .build()
        );
    }

    @Test
    public void testMissingDatabaseThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            DorisConfig.builder()
                    .endpoints(Arrays.asList("http://127.0.0.1:8030"))
                    .user("root").table("t")
                    .format(new JsonFormat(JsonFormat.Type.OBJECT_LINE))
                    .build()
        );
    }

    @Test
    public void testMissingTableThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            DorisConfig.builder()
                    .endpoints(Arrays.asList("http://127.0.0.1:8030"))
                    .user("root").database("db")
                    .format(new JsonFormat(JsonFormat.Type.OBJECT_LINE))
                    .build()
        );
    }

    @Test
    public void testEmptyEndpointsThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            DorisConfig.builder()
                    .user("root").database("db").table("t")
                    .format(new JsonFormat(JsonFormat.Type.OBJECT_LINE))
                    .build()
        );
    }

    @Test
    public void testNullFormatThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            DorisConfig.builder()
                    .endpoints(Arrays.asList("http://127.0.0.1:8030"))
                    .user("root").database("db").table("t")
                    .build()
        );
    }

    @Test
    public void testDefaultRetryValues() {
        RetryConfig retry = RetryConfig.defaultRetry();
        assertEquals(6, retry.getMaxRetryTimes());
        assertEquals(1000L, retry.getBaseIntervalMs());
        assertEquals(60000L, retry.getMaxTotalTimeMs());
    }

    @Test
    public void testNegativeRetryTimesThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            RetryConfig.builder().maxRetryTimes(-1).baseIntervalMs(1000).maxTotalTimeMs(60000).build()
        );
    }
}
