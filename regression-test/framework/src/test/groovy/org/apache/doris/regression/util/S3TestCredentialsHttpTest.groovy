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

package org.apache.doris.regression.util

import com.amazonaws.ClientConfiguration
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.ObjectMetadata
import com.sun.net.httpserver.HttpServer
import java.nio.charset.StandardCharsets
import org.junit.jupiter.api.Test
import static org.junit.jupiter.api.Assertions.*

class S3TestCredentialsHttpTest {
    @Test
    void signedRequestsCarryTheTokenAndPropagateStorageFailures() {
        Map config = new Properties()
        config.putAll([ak: "httpAccessKey0001", sk: "httpSecretKey0001", sessionToken: "httpSessionToken0001"])
        def requests = Collections.synchronizedList([])
        byte[] object = "fixture-content".getBytes(StandardCharsets.UTF_8)
        def server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/") { exchange ->
            requests.add([method: exchange.requestMethod, token: exchange.requestHeaders.getFirst("x-amz-security-token")])
            byte[] response
            int status
            if (exchange.requestURI.path.endsWith("/denied")) {
                status = 403
                response = "<Error><Code>AccessDenied</Code><Message>read denied</Message><RequestId>test-request</RequestId></Error>".getBytes(StandardCharsets.UTF_8)
            } else if (exchange.requestMethod == "PUT") {
                assertArrayEquals(object, exchange.requestBody.bytes)
                status = 200
                exchange.responseHeaders.add("ETag", '"fixture-etag"')
                response = new byte[0]
            } else {
                status = 200
                response = object
            }
            exchange.sendResponseHeaders(status, response.length == 0 ? -1 : response.length)
            if (response.length > 0) exchange.responseBody.write(response)
            exchange.close()
        }
        server.start()
        def client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://127.0.0.1:" + server.address.port, "us-east-1"))
                .withCredentials(new S3TestCredentials(config))
                .withPathStyleAccessEnabled(true)
                .withChunkedEncodingDisabled(true)
                .withClientConfiguration(new ClientConfiguration().withMaxErrorRetry(0).withConnectionTimeout(5000).withSocketTimeout(5000))
                .build()
        try {
            def metadata = new ObjectMetadata()
            metadata.contentLength = object.length
            client.putObject("private-results", "runs/123/fixture", new ByteArrayInputStream(object), metadata)
            assertEquals("httpSessionToken0001", requests.last().token)
            config.putAll([ak: "httpAccessKey0002", sk: "httpSecretKey0002", sessionToken: "httpSessionToken0002"])
            def result = client.getObject("private-results", "runs/123/fixture")
            try { assertArrayEquals(object, result.objectContent.bytes) } finally { result.close() }
            assertEquals("httpSessionToken0002", requests.last().token)
            def failure = assertThrows(AmazonS3Exception) { client.getObject("public-fixtures", "denied") }
            assertEquals(403, failure.statusCode)
            assertEquals("AccessDenied", failure.errorCode)
            config.remove("sessionToken")
            result = client.getObject("public-fixtures", "fixture")
            try { assertArrayEquals(object, result.objectContent.bytes) } finally { result.close() }
            assertNull(requests.last().token)
        } finally {
            client.shutdown()
            server.stop(0)
        }
    }
}
