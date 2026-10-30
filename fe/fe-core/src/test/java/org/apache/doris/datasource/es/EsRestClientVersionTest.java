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

package org.apache.doris.datasource.es;

import com.google.common.collect.ImmutableSet;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;

class EsRestClientVersionTest {
    @Test
    void mixedEightAndNineNodesAreDiscoveredDuringUpgrade() throws Exception {
        String nodes = "{\"nodes\":{"
                + "\"eight\":{\"version\":\"8.19.0\",\"name\":\"eight\",\"roles\":[\"data\",\"ingest\"],"
                + "\"http\":{\"publish_address\":\"127.0.0.1:9200\"}},"
                + "\"nine\":{\"version\":\"9.0.0\",\"name\":\"nine\",\"roles\":[\"data\"],"
                + "\"http\":{\"publish_address\":\"node/127.0.0.2:9201\"}},"
                + "\"no-http\":{\"version\":\"9.0.0\",\"name\":\"master\",\"roles\":[\"master\"]}}}";
        byte[] body = nodes.getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/_nodes/http", exchange -> {
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            EsRestClient client = new EsRestClient(
                    new String[]{"http://127.0.0.1:" + server.getAddress().getPort()}, null, null, false);
            Map<String, EsNodeInfo> discovered = client.getHttpNodes();
            Assertions.assertEquals(ImmutableSet.of("eight", "nine"), discovered.keySet());
            Assertions.assertEquals("127.0.0.1", discovered.get("eight").getPublishAddress().getHostname());
            Assertions.assertEquals(9200, discovered.get("eight").getPublishAddress().getPort());
            Assertions.assertEquals("127.0.0.2", discovered.get("nine").getPublishAddress().getHostname());
            Assertions.assertEquals(9201, discovered.get("nine").getPublishAddress().getPort());
            Assertions.assertTrue(discovered.get("nine").isData());
        } finally {
            server.stop(0);
        }
    }

}
