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

import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.httpv2.rest.UploadAction;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class LoadSubmitter {
    private static final Logger LOG = LogManager.getLogger(LoadSubmitter.class);

    private ThreadPoolExecutor executor = ThreadPoolManager.newDaemonCacheThreadPoolThrowException(
                        Config.http_load_submitter_max_worker_threads, "load-submitter", true);

    private static final String STREAM_LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";

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

        private SubmitResult load() throws Exception {
            // choose a backend to submit the stream load
            Backend be = selectOneBackend();

            String hostPort = NetUtils.getHostPortInAccessibleFormat(be.getHost(), be.getHttpPort());
            String loadUrlStr = String.format(STREAM_LOAD_URL_PATTERN, hostPort, loadContext.db, loadContext.tbl);
            URL loadUrl = new URL(loadUrlStr);
            HttpURLConnection conn = (HttpURLConnection) loadUrl.openConnection();
            conn.setRequestMethod("PUT");
            String auth = String.format("%s:%s", ClusterNamespace.getNameFromFullName(loadContext.user),
                    loadContext.passwd);
            String authEncoding = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
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

        private Backend selectOneBackend() throws LoadException {
            BeSelectionPolicy policy = new BeSelectionPolicy.Builder().needLoadAvailable().build();
            List<Long> backendIds = Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, 1);
            if (backendIds.isEmpty()) {
                throw new LoadException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
            }
            Backend backend = Env.getCurrentSystemInfo().getBackend(backendIds.get(0));
            if (backend == null) {
                throw new LoadException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
            }
            return backend;
        }
    }

    // CHECKSTYLE OFF: These name must match the name in json, case-sensitive.
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
    // CHECKSTYLE ON
}
