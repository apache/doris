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

package org.apache.doris.httpv2.util;

import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.httpv2.rest.UploadAction;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class LoadSubmitter {
    private static final Logger LOG = LogManager.getLogger(LoadSubmitter.class);

    private ThreadPoolExecutor executor = ThreadPoolManager.newDaemonCacheThreadPool(2, "Load submitter", true);

    private static final String STREAM_LOAD_URL_PATTERN = "http://%s:%d/api/%s/%s/_stream_load";

    public Future<SubmitResult> submit(UploadAction.LoadContext loadContext) {
        LoadSubmitter.Worker worker = new LoadSubmitter.Worker(loadContext);
        return executor.submit(worker);
    }

    private static class Worker implements Callable<SubmitResult> {

        private UploadAction.LoadContext loadContext;

        public Worker(UploadAction.LoadContext loadContext) {
            this.loadContext = loadContext;
        }

        @Override
        public SubmitResult call() throws Exception {
            try {
                return load();
            } catch (Throwable e) {
                LOG.warn("failed to submit load. label: {}", loadContext.label, e);
                throw e;
            }
        }

        private SubmitResult sendData(String content) throws Exception {
            String loadUrlStr = String.format(STREAM_LOAD_URL_PATTERN, "127.0.0.1", Config.http_port, loadContext.db, loadContext.tbl);

            final HttpClientBuilder httpClientBuilder = HttpClients
                    .custom()
                    .setRedirectStrategy(new DefaultRedirectStrategy() {
                        @Override
                        protected boolean isRedirectable(String method) {
                            return true;
                        }
                    });

            try (CloseableHttpClient client = httpClientBuilder.build()) {
                HttpPut put = new HttpPut(loadUrlStr);
                put.setHeader(HttpHeaders.EXPECT, "100-continue");
                put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader());
                if (!Strings.isNullOrEmpty(loadContext.columns)) {
                    put.setHeader("columns", loadContext.columns);
                }
                if (!Strings.isNullOrEmpty(loadContext.columnSeparator)) {
                    put.setHeader("column_separator", loadContext.columnSeparator);
                }
                if (!Strings.isNullOrEmpty(loadContext.label)) {
                    put.setHeader("label", loadContext.label);
                }

                FileEntity entity = new FileEntity(checkAndGetFile(loadContext.file));
                put.setEntity(entity);

                try (CloseableHttpResponse response = client.execute(put)) {
                    String loadResult = "";
                    if (response.getEntity() != null) {
                        loadResult = EntityUtils.toString(response.getEntity());
                    }
                    final int statusCode = response.getStatusLine().getStatusCode();

                    if (statusCode != 200) {
                        throw new IOException(
                                String.format("Stream load failed, statusCode=%s load result=%s", statusCode, loadResult));
                    }

                    Type type = new TypeToken<SubmitResult>() {
                    }.getType();
                    SubmitResult result = new Gson().fromJson(loadResult, type);
                    return result;
                }
            }
        }

        private String basicAuthHeader() {
            String auth = String.format("%s:%s", ClusterNamespace.getNameFromFullName(loadContext.user), loadContext.passwd);
            return Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        }

        private SubmitResult load() throws Exception {
            String auth = String.format("%s:%s", ClusterNamespace.getNameFromFullName(loadContext.user), loadContext.passwd);
            String authEncoding = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));

            String loadUrlStr = String.format(STREAM_LOAD_URL_PATTERN, "127.0.0.1", Config.http_port, loadContext.db, loadContext.tbl);
            URL loadUrl = new URL(loadUrlStr);
            HttpURLConnection conn = (HttpURLConnection) loadUrl.openConnection();
            conn.setRequestMethod("PUT");
            conn.setRequestProperty("Authorization", "Basic " + authEncoding);
            conn.addRequestProperty("Expect", "100-continue");
            conn.addRequestProperty("Content-Type", "text/plain; charset=UTF-8");
            if (!Strings.isNullOrEmpty(loadContext.columns)) {
                conn.addRequestProperty("columns", loadContext.columns);
            }
            if (!Strings.isNullOrEmpty(loadContext.columnSeparator)) {
                conn.addRequestProperty("column_separator", loadContext.columnSeparator);
            }
            if (!Strings.isNullOrEmpty(loadContext.label)) {
                conn.addRequestProperty("label", loadContext.label);
            }
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setReadTimeout(0);
            conn.setConnectTimeout(5000);

            File loadFile = checkAndGetFile(loadContext.file);
            try (BufferedOutputStream bos = new BufferedOutputStream(conn.getOutputStream());
                 BufferedInputStream bis = new BufferedInputStream(new FileInputStream(loadFile));) {
                int i;
                while ((i = bis.read()) > 0) {
                    bos.write(i);
                }
            }

            int status = conn.getResponseCode();
            String respMsg = conn.getResponseMessage();

            LOG.info("get status: {}, response msg: {}", status, respMsg);

            InputStream stream = (InputStream) conn.getContent();
            BufferedReader br = new BufferedReader(new InputStreamReader(stream));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            Type type = new TypeToken<SubmitResult>() {
            }.getType();
            SubmitResult result = new Gson().fromJson(sb.toString(), type);
            return result;
        }

        private File checkAndGetFile(TmpFileMgr.TmpFile tmpFile) {
            File file = new File(tmpFile.absPath);
            return file;
        }
    }

    public static class SubmitResult {
        public String TxnId;
        public String Label;
        public String Status;
        public String ExistingJobStatus;
        public String Message;
        public String NumberTotalRows;
        public String NumberLoadedRows;
        public String NumberFilteredRows;
        public String NumberUnselectedRows;
        public String LoadBytes;
        public String LoadTimeMs;
        public String BeginTxnTimeMs;
        public String StreamLoadPutTimeMs;
        public String ReadDataTimeMs;
        public String WriteDataTimeMs;
        public String CommitAndPublishTimeMs;
        public String ErrorURL;
    }
}
