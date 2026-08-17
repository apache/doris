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

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Unit tests for {@link LoadStatistic}. */
class LoadStatisticTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private RespContent resp(int filtered, long loaded, long bytes) throws Exception {
        String json =
                String.format(
                        "{\"NumberFilteredRows\":%d,\"NumberLoadedRows\":%d,\"LoadBytes\":%d}",
                        filtered, loaded, bytes);
        return MAPPER.readValue(json, RespContent.class);
    }

    @Test
    void add_accumulatesAcrossResponses() throws Exception {
        LoadStatistic stat = new LoadStatistic();
        stat.add(resp(2, 10, 100));
        stat.add(resp(3, 20, 200));

        assertEquals(5, stat.getFilteredRows());
        assertEquals(30, stat.getLoadedRows());
        assertEquals(300, stat.getLoadBytes());
    }

    @Test
    void clear_resetsToZero() throws Exception {
        LoadStatistic stat = new LoadStatistic();
        stat.add(resp(1, 1, 1));

        stat.clear();

        assertEquals(0, stat.getFilteredRows());
        assertEquals(0, stat.getLoadedRows());
        assertEquals(0, stat.getLoadBytes());
    }
}
