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

package org.apache.doris.plugin.audit;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InternalSchema;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.GlobalVariable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Calendar;
import java.util.stream.Collectors;

/**
 * Stream loader for uploading per-BE query metrics to
 * __internal_schema.query_be_metrics via HTTP PUT.
 *
 * Mirrors AuditStreamLoader but targets the query_be_metrics table.
 */
public class BeMetricsStreamLoader {

    private static final Logger LOG = LogManager.getLogger(BeMetricsStreamLoader.class);
    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load?";
    private static final int HTTP_TIMEOUT_MS = 10000;

    private final String hostPort;
    private final String loadUrlStr;
    private final String feIdentity;

    public BeMetricsStreamLoader() {
        this.hostPort = "127.0.0.1:" + Config.http_port;
        this.loadUrlStr = String.format(LOAD_URL_PATTERN, hostPort,
            FeConstants.INTERNAL_DB_NAME, BeMetricsLoader.BE_METRICS_TABLE);
        this.feIdentity = Env.getCurrentEnv().getSelfNode().getIdent()
            .replaceAll("\\.", "_").replaceAll(":", "_");
    }

    private HttpURLConnection getConnection(String urlStr, String label, String clusterToken)
        throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("token", clusterToken);
        conn.setRequestProperty("Authorization", "Basic YWRtaW46");
        conn.addRequestProperty("Expect", "100-continue");
        conn.addRequestProperty("Content-Type", "text/plain; charset=UTF-8");
        conn.addRequestProperty("label", label);
        conn.setConnectTimeout(HTTP_TIMEOUT_MS);
        conn.setReadTimeout(HTTP_TIMEOUT_MS);
        conn.setRequestProperty("timeout",
            String.valueOf(GlobalVariable.beMetricsPluginLoadTimeoutS));
        conn.addRequestProperty("max_filter_ratio", "1.0");
        conn.addRequestProperty("columns",
            InternalSchema.QUERY_BE_METRICS_SCHEMA.stream()
                .map(c -> c.getName())
                .collect(Collectors.joining(",")));
        conn.addRequestProperty("redirect-policy", "random-be");
        conn.addRequestProperty("column_separator",
            BeMetricsLoader.BE_METRICS_COL_SEPARATOR_STR);
        conn.addRequestProperty("line_delimiter",
            BeMetricsLoader.BE_METRICS_LINE_DELIMITER_STR);
        conn.setDoOutput(true);
        conn.setDoInput(true);
        return conn;
    }

    private String getContent(HttpURLConnection conn) {
        StringBuilder response = new StringBuilder();
        try {
            BufferedReader br;
            if (100 <= conn.getResponseCode() && conn.getResponseCode() <= 399) {
                br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            } else {
                br = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
            }
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
        } catch (IOException e) {
            LOG.warn("get content error,", e);
        }
        return response.toString();
    }

    public LoadResponse loadBatch(StringBuilder sb, String clusterToken) {
        String label = genLabel();
        HttpURLConnection feConn = null;
        HttpURLConnection beConn = null;
        try {
            label = "bemetrics" + label;
            feConn = getConnection(loadUrlStr, label, clusterToken);
            int status = feConn.getResponseCode();
            if (status != 307) {
                throw new Exception("status is not TEMPORARY_REDIRECT 307, status: "
                    + status + ", response: " + getContent(feConn));
            }
            String location = feConn.getHeaderField("Location");
            if (location == null) {
                throw new Exception("redirect location is null");
            }
            beConn = getConnection(location, label, clusterToken);
            try (BufferedOutputStream bos = new BufferedOutputStream(beConn.getOutputStream())) {
                bos.write(sb.toString().getBytes());
            }
            status = beConn.getResponseCode();
            String respMsg = beConn.getResponseMessage();
            String response = getContent(beConn);
            LOG.info("BeMetricsLoader plugin load with label: {}, response code: {},"
                + " msg: {}, content: {}", label, status, respMsg, response);
            return new LoadResponse(status, respMsg, response);
        } catch (Exception e) {
            String err = "failed to load be metrics via BeMetricsLoader plugin with label: "
                + label;
            LOG.warn(err, e);
            return new LoadResponse(-1, e.getMessage(), err);
        } finally {
            if (feConn != null) {
                feConn.disconnect();
            }
            if (beConn != null) {
                beConn.disconnect();
            }
        }
    }

    private String genLabel() {
        Calendar calendar = Calendar.getInstance();
        return String.format("_bemetrics_%s%02d%02d_%02d%02d%02d_%s_%s",
            calendar.get(Calendar.YEAR),
            calendar.get(Calendar.MONTH) + 1,
            calendar.get(Calendar.DAY_OF_MONTH),
            calendar.get(Calendar.HOUR_OF_DAY),
            calendar.get(Calendar.MINUTE),
            calendar.get(Calendar.SECOND),
            calendar.get(Calendar.MILLISECOND),
            feIdentity);
    }

    public static class LoadResponse {
        public int status;
        public String respMsg;
        public String respContent;

        public LoadResponse(int status, String respMsg, String respContent) {
            this.status = status;
            this.respMsg = respMsg;
            this.respContent = respContent;
        }

        @Override
        public String toString() {
            return "status: " + status + ", resp msg: " + respMsg
                + ", resp content: " + respContent;
        }
    }
}
