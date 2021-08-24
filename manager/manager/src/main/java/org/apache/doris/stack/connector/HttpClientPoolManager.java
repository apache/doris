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

package org.apache.doris.stack.connector;

import com.alibaba.fastjson.JSON;
import org.apache.doris.stack.model.palo.PaloResponseEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class HttpClientPoolManager implements InitializingBean, DisposableBean {

    private HttpClient client;

    private ExecutorService executorService;

    /**
     * initialization
     *
     * @throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        PoolingHttpClientConnectionManager pool = initPool();
        client = HttpClients.custom()
                .setConnectionManager(pool)
                .setKeepAliveStrategy(initKeepAliveStrategy())
                .build();
        executorService = Executors.newFixedThreadPool(1);
        executorService.submit(new IdleConnectionClearThread(pool));
    }

    /**
     * destroy
     *
     * @throws Exception
     */
    @Override
    public void destroy() throws Exception {
        //TODO:close?
//        client.close();
        executorService.shutdown();
    }

    /**
     * get
     *
     * @param url
     * @param headers
     * @return
     * @throws Exception
     */
    public PaloResponseEntity doGet(String url, Map<String, String> headers) throws Exception {
        // create get object
        HttpGet httpGet = new HttpGet(url);
        // set header
        if (null != headers) {
            for (String key : headers.keySet()) {
                httpGet.setHeader(key, headers.get(key));
            }
        }
        setRequestConfig(httpGet);
        return executeRequest(httpGet);
    }

    /**
     * get(with body)
     *
     * @param url
     * @param headers
     * @param body
     * @return
     * @throws Exception
     */
    public PaloResponseEntity doGet(String url, Map<String, String> headers, Object body) throws Exception {
        HttpGetWithEntity httpGet = new HttpGetWithEntity(url);
        if (null != headers) {
            for (String key : headers.keySet()) {
                httpGet.setHeader(key, headers.get(key));
            }
        }
        String jsonString = JSON.toJSONString(body);
        StringEntity stringEntity = new StringEntity(jsonString, "UTF-8");
        httpGet.setEntity(stringEntity);
        setRequestConfig(httpGet);
        return executeRequest(httpGet);
    }

    /**
     * post
     *
     * @param url
     * @param headers
     * @param body
     * @return
     * @throws Exception
     */
    public PaloResponseEntity doPost(String url, Map<String, String> headers, Object body) throws Exception {
        HttpPost httpPost = new HttpPost(url);
        if (null != headers) {
            for (String key : headers.keySet()) {
                httpPost.setHeader(key, headers.get(key));
            }
        }
        String jsonString = JSON.toJSONString(body);
        StringEntity stringEntity = new StringEntity(jsonString, "UTF-8");
        httpPost.setEntity(stringEntity);
        setRequestConfig(httpPost);
        return executeRequest(httpPost);
    }

    public PaloResponseEntity forwardPost(String url, Map<String, String> headers, String body) throws Exception {
        HttpPost httpPost = new HttpPost(url);
        if (null != headers) {
            for (String key : headers.keySet()) {
                httpPost.setHeader(key, headers.get(key));
            }
        }
        StringEntity stringEntity = new StringEntity(body, "UTF-8");
        httpPost.setEntity(stringEntity);
        setRequestConfig(httpPost);
        return executeRequest(httpPost);
    }

    public PaloResponseEntity doPut(String url, Map<String, String> headers) throws Exception {
        HttpPut httpPut = new HttpPut(url);
        if (null != headers) {
            for (String key : headers.keySet()) {
                httpPut.setHeader(key, headers.get(key));
            }
        }
        setRequestConfig(httpPut);
        return executeRequest(httpPut);

    }

    public PaloResponseEntity uploadFile(String url, MultipartFile file, Map<String, String> headers,
                                         Map<String, String> otherParams, String boundary) throws Exception {
        HttpPost httpPost = new HttpPost(url);
        // add header
        if (null != headers) {
            for (String key : headers.keySet()) {
                httpPost.setHeader(key, headers.get(key));
            }
        }

        // upload file
        String fileName = file.getOriginalFilename();
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.setBoundary(boundary);
        builder.setCharset(Charset.forName("utf-8"));
        // Add this line of code to solve the problem of returning Chinese garbled code
        builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
        // File stream
        builder.addBinaryBody("file", file.getInputStream(), ContentType.MULTIPART_FORM_DATA, fileName);
        for (Map.Entry<String, String> e : otherParams.entrySet()) {
            // Similar to browser form submission, corresponding to the name and value of input
            builder.addTextBody(e.getKey(), e.getValue());
        }
        HttpEntity entity = builder.build();
        httpPost.setEntity(entity);

        setRequestConfig(httpPost);
        return executeRequest(httpPost);

    }

    public PaloResponseEntity doDelete(String url, Map<String, String> headers) throws Exception {
        HttpDelete httpDelete = new HttpDelete(url);
        if (null != headers) {
            for (String key : headers.keySet()) {
                httpDelete.setHeader(key, headers.get(key));
            }
        }

        setRequestConfig(httpDelete);
        return executeRequest(httpDelete);
    }

    private void setRequestConfig(HttpRequestBase request) {
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(35000)
                .setConnectionRequestTimeout(35000)
                .setSocketTimeout(60000)
                .build();
        request.setConfig(config);
    }

    private PaloResponseEntity executeRequest(HttpRequestBase request) throws Exception {
        String result = client.execute(request, new ResponseHandler<String>() {
            @Override
            public String handleResponse(HttpResponse httpResponse) throws ClientProtocolException, IOException {
                return EntityUtils.toString(httpResponse.getEntity());
            }
        });
        log.debug("execute request result:" + result);

        PaloResponseEntity responseEntity = JSON.parseObject(result, PaloResponseEntity.class);
        return responseEntity;
    }

    /**
     * Get connection keep alive policy
     *
     * @return
     */
    private ConnectionKeepAliveStrategy initKeepAliveStrategy() {
        ConnectionKeepAliveStrategy strategy = new ConnectionKeepAliveStrategy() {
            @Override
            public long getKeepAliveDuration(HttpResponse httpResponse, HttpContext httpContext) {
                // Set the maximum idle time for the connection to survive
                // If the connection is idle after this time, the connection will be closed. Here, it is set to 600 seconds
                return 600 * 1000;
            }
        };
        return strategy;
    }

    /**
     * Initialize connection pool
     *
     * @return
     */
    private PoolingHttpClientConnectionManager initPool() {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        // Configure connection pool properties
        connectionManager.setMaxTotal(500);
        connectionManager.setDefaultMaxPerRoute(50);
        return connectionManager;
    }

    public static class IdleConnectionClearThread extends Thread {

        private final HttpClientConnectionManager connectionManager;

        private volatile boolean shutdown;

        public IdleConnectionClearThread(HttpClientConnectionManager connectionManager) {
            super();
            this.connectionManager = connectionManager;
        }

        @Override
        public void run() {
            try {
                while (!shutdown) {
                    synchronized (this) {
                        wait(5000);
                        connectionManager.closeExpiredConnections();
                        connectionManager.closeIdleConnections(30, TimeUnit.SECONDS);
                    }
                }
            } catch (InterruptedException exception) {
                exception.printStackTrace();
            }
        }

        public void shutdown() {
            this.shutdown = true;
            synchronized (this) {
                notifyAll();
            }
        }
    }

    private static class HttpGetWithEntity extends HttpEntityEnclosingRequestBase {

        public static final String METHOD_NAME = "GET";

        public HttpGetWithEntity() {
        }

        public HttpGetWithEntity(URI uri) {
            this.setURI(uri);
        }

        public HttpGetWithEntity(String uri) {
            this.setURI(URI.create(uri));
        }

        @Override
        public String getMethod() {
            return METHOD_NAME;
        }
    }
}
