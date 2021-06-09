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

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
/**
 * This class is a java demo for doris stream load
 *
 * The pom.xml dependency:
 *
 *         <dependency>
 *             <groupId>org.apache.httpcomponents</groupId>
 *             <artifactId>httpclient</artifactId>
 *             <version>4.5.3</version>
 *         </dependency>
 *
 * How to use:
 *
 * 1 create a table in doris with any mysql client
 *
 * CREATE TABLE `stream_test` (
 *   `id` bigint(20) COMMENT "",
 *   `id2` bigint(20) COMMENT "",
 *   `username` varchar(32) COMMENT ""
 * ) ENGINE=OLAP
 * DUPLICATE KEY(`id`)
 * DISTRIBUTED BY HASH(`id`) BUCKETS 20;
 *
 *
 * 2 change the Doris cluster, db, user config in this class
 *
 * 3 run this class, you should see the following output:
 *
 * {
 *     "TxnId": 27,
 *     "Label": "39c25a5c-7000-496e-a98e-348a264c81de",
 *     "Status": "Success",
 *     "Message": "OK",
 *     "NumberTotalRows": 10,
 *     "NumberLoadedRows": 10,
 *     "NumberFilteredRows": 0,
 *     "NumberUnselectedRows": 0,
 *     "LoadBytes": 50,
 *     "LoadTimeMs": 151
 * }
 *
 * Attention:
 *
 * 1 wrong dependency version(such as 4.4) of httpclient may cause shaded.org.apache.http.ProtocolException
 *   Caused by: shaded.org.apache.http.ProtocolException: Content-Length header already present
 *     at shaded.org.apache.http.protocol.RequestContent.process(RequestContent.java:96)
 *     at shaded.org.apache.http.protocol.ImmutableHttpProcessor.process(ImmutableHttpProcessor.java:132)
 *     at shaded.org.apache.http.impl.execchain.ProtocolExec.execute(ProtocolExec.java:182)
 *     at shaded.org.apache.http.impl.execchain.RetryExec.execute(RetryExec.java:88)
 *     at shaded.org.apache.http.impl.execchain.RedirectExec.execute(RedirectExec.java:110)
 *     at shaded.org.apache.http.impl.client.InternalHttpClient.doExecute(InternalHttpClient.java:184)
 *
 *2 run this class more than once, the status code for http response is still ok, and you will see
 *  the following output:
 *
 * {
 *     "TxnId": -1,
 *     "Label": "39c25a5c-7000-496e-a98e-348a264c81de",
 *     "Status": "Label Already Exists",
 *     "ExistingJobStatus": "FINISHED",
 *     "Message": "Label [39c25a5c-7000-496e-a98e-348a264c81de"] has already been used.",
 *     "NumberTotalRows": 0,
 *     "NumberLoadedRows": 0,
 *     "NumberFilteredRows": 0,
 *     "NumberUnselectedRows": 0,
 *     "LoadBytes": 0,
 *     "LoadTimeMs": 0
 * }
 * 3 when the response statusCode is 200, that doesn't mean your stream load is ok, there may be still
 *   some stream problem unless you see the output with 'ok' message
 */
public class DorisStreamLoad {
    private final static String DORIS_HOST = "xxx.com";
    private final static String DORIS_DB = "test";
    private final static String DORIS_TABLE = "stream_test";
    private final static String DORIS_USER = "root";
    private final static String DORIS_PASSWORD = "xxx";
    private final static int DORIS_HTTP_PORT = 8410;

    private void sendData(String content) throws Exception {
        final String loadUrl = String.format("http://%s:%s/api/%s/%s/_stream_load",
                DORIS_HOST,
                DORIS_HTTP_PORT,
                DORIS_DB,
                DORIS_TABLE);

        final HttpClientBuilder httpClientBuilder = HttpClients
                .custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });

        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = new HttpPut(loadUrl);
            StringEntity entity = new StringEntity(content, "UTF-8");
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(DORIS_USER, DORIS_PASSWORD));
            // the label header is optional, not necessary
            // use label header can ensure at most once semantics
            put.setHeader("label", "39c25a5c-7000-496e-a98e-348a264c81de");
            put.setEntity(entity);

            try (CloseableHttpResponse response = client.execute(put)) {
                String loadResult = "";
                if (response.getEntity() != null) {
                    loadResult = EntityUtils.toString(response.getEntity());
                }
                final int statusCode = response.getStatusLine().getStatusCode();
                // statusCode 200 just indicates that doris be service is ok, not stream load
                // you should see the output content to find whether stream load is success
                if (statusCode != 200) {
                    throw new IOException(
                            String.format("Stream load failed, statusCode=%s load result=%s", statusCode, loadResult));
                }

                System.out.println(loadResult);
            }
        }
    }

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    public static void main(String[] args) throws Exception {
        int id1 = 1;
        int id2 = 10;
        String id3 = "张三";
        int rowNumber = 10;
        String oneRow = id1 + "\t" + id2 + "\t" + id3 + "\n";

        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < rowNumber; i++) {
            stringBuilder.append(oneRow);
        }

        //in doris 0.9 version, you need to comment this line
        //refer to https://github.com/apache/incubator-doris/issues/783
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);

        String loadData = stringBuilder.toString();
        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad();
        dorisStreamLoad.sendData(loadData);
    }
}
