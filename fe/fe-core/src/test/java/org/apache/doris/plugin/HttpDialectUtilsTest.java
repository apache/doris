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

package org.apache.doris.plugin;

import org.apache.doris.plugin.dialect.HttpDialectUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.SocketException;

public class HttpDialectUtilsTest {

    private int port;
    private SimpleHttpServer server;

    @Before
    public void setUp() throws Exception {
        port = findValidPort();
        server = new SimpleHttpServer(port);
        server.start("/api/v1/convert");
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testSqlConvert() {
        String originSql = "select * from t1 where \"k1\" = 1";
        String expectedSql = "select * from t1 where `k1` = 1";

        String targetURL = "http://127.0.0.1:" + port + "/api/v1/convert";
        String res = HttpDialectUtils.convertSql(targetURL, originSql, "presto");
        Assert.assertEquals(originSql, res);
        // test presto
        server.setResponse("{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
        res = HttpDialectUtils.convertSql(targetURL, originSql, "presto");
        Assert.assertEquals(expectedSql, res);
        // test response version error
        server.setResponse("{\"version\": \"v2\", \"data\": \"" + expectedSql + "\", \"code\": 0, \"message\": \"\"}");
        res = HttpDialectUtils.convertSql(targetURL, originSql, "presto");
        Assert.assertEquals(originSql, res);
        // 7. test response code error
        server.setResponse(
                "{\"version\": \"v1\", \"data\": \"" + expectedSql + "\", \"code\": 400, \"message\": \"\"}");
        res = HttpDialectUtils.convertSql(targetURL, originSql, "presto");
        Assert.assertEquals(originSql, res);
    }

    private static int findValidPort() {
        int port;
        while (true) {
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(true);
                port = socket.getLocalPort();
                try (DatagramSocket datagramSocket = new DatagramSocket(port)) {
                    datagramSocket.setReuseAddress(true);
                    break;
                } catch (SocketException e) {
                    System.out.println("The port " + port + " is invalid and try another port.");
                }
            } catch (IOException e) {
                throw new IllegalStateException("Could not find a free TCP/IP port to start HTTP Server on");
            }
        }
        return port;
    }
}
