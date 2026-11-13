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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InternalSchema;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.GlobalVariable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

public class AuditStreamLoader {
    private static final Logger LOG = LogManager.getLogger(AuditStreamLoader.class);
    private static String loadUrlPattern = "http://%s/api/%s/%s/_stream_load?";
    // timeout for both connection and read. 10 seconds is long enough.
    private static final int HTTP_TIMEOUT_MS = 10000;
    private String hostPort;
    private static final String COMPRESS_TYPE = "gz";
    private static final String REMOTE_WRITE_COLUMN = "spill_write_bytes_to_remote_storage";
    private static final String REMOTE_READ_COLUMN = "spill_read_bytes_from_remote_storage";
    private String db;
    private String auditLogTbl;
    private String auditLogLoadUrlStr;
    private String feIdentity;

    public AuditStreamLoader() {
        this.hostPort = "127.0.0.1:" + Config.http_port;
        this.db = FeConstants.INTERNAL_DB_NAME;
        this.auditLogTbl = AuditLoader.AUDIT_LOG_TABLE;
        this.auditLogLoadUrlStr = String.format(loadUrlPattern, hostPort, db, auditLogTbl);
        // currently, FE identity is FE's IP:port, so we replace the "." and ":" to make it suitable for label
        this.feIdentity = Env.getCurrentEnv().getSelfNode().getIdent().replaceAll("\\.", "_").replaceAll(":", "_");
    }

    private static HttpURLConnection getConnection(
            String urlStr, String label, String clusterToken, String columns) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("token", clusterToken);
        conn.setRequestProperty("Authorization", "Basic YWRtaW46"); // admin
        conn.addRequestProperty("Expect", "100-continue");
        conn.addRequestProperty("Content-Type", "text/plain; charset=UTF-8");
        conn.addRequestProperty("label", label);
        conn.addRequestProperty("compress_type", COMPRESS_TYPE);
        conn.setConnectTimeout(HTTP_TIMEOUT_MS);
        conn.setReadTimeout(HTTP_TIMEOUT_MS);
        conn.setRequestProperty("timeout", String.valueOf(GlobalVariable.auditPluginLoadTimeoutS));
        conn.addRequestProperty("max_filter_ratio", "1.0");
        conn.addRequestProperty("columns", columns);
        conn.addRequestProperty("redirect-policy", "random-be");
        conn.addRequestProperty("column_separator", AuditLoader.AUDIT_TABLE_COL_SEPARATOR_STR);
        conn.addRequestProperty("line_delimiter", AuditLoader.AUDIT_TABLE_LINE_DELIMITER_STR);
        conn.addRequestProperty("skip_record_to_audit_log_table", "true");
        conn.setDoOutput(true);
        conn.setDoInput(true);
        return conn;
    }

    private String toCurl(HttpURLConnection conn) {
        StringBuilder sb = new StringBuilder("curl -v ");
        sb.append("-X ").append(conn.getRequestMethod()).append(" \\\n  ");
        sb.append("-H \"").append("Authorization\":").append("\"Basic YWRtaW46").append("\" \\\n  ");
        sb.append("-H \"").append("Expect\":").append("\"100-continue\" \\\n  ");
        sb.append("-H \"").append("Content-Type\":").append("\"text/plain; charset=UTF-8\" \\\n  ");
        sb.append("-H \"").append("max_filter_ratio\":").append("\"1.0\" \\\n  ");
        sb.append("-H \"").append("columns\":")
                .append("\"" + conn.getRequestProperty("columns") + "\" \\\n  ");
        sb.append("-H \"").append("redirect-policy\":").append("\"random-be").append("\" \\\n  ");
        sb.append("\"").append(conn.getURL()).append("\"");
        return sb.toString();
    }

    private String getContent(HttpURLConnection conn) {
        BufferedReader br = null;
        StringBuilder response = new StringBuilder();
        String line;
        try {
            if (100 <= conn.getResponseCode() && conn.getResponseCode() <= 399) {
                br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            } else {
                br = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
            }
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
        } catch (IOException e) {
            LOG.warn("get content error,", e);
        }

        return response.toString();
    }

    private static void writeCompressedBody(OutputStream outputStream, StringBuilder payload) throws IOException {
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(new BufferedOutputStream(outputStream))) {
            gzipOutputStream.write(payload.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    static boolean hasRemoteSpillColumns(List<Column> columns) {
        return columns.stream().anyMatch(c -> REMOTE_WRITE_COLUMN.equalsIgnoreCase(c.getName()))
                && columns.stream().anyMatch(c -> REMOTE_READ_COLUMN.equalsIgnoreCase(c.getName()));
    }

    static boolean targetHasRemoteSpillColumns() {
        Optional<Database> db = Env.getCurrentEnv().getInternalCatalog().getDb(FeConstants.INTERNAL_DB_NAME);
        return db.flatMap(database -> database.getTable(AuditLoader.AUDIT_LOG_TABLE))
                .map(Table::getBaseSchema).map(AuditStreamLoader::hasRemoteSpillColumns).orElse(false);
    }

    static PreparedBatch prepareBatch(StringBuilder fullBatch, boolean remoteColumnsAvailable) {
        List<String> names = InternalSchema.AUDIT_SCHEMA.stream().map(c -> c.getName())
                .collect(Collectors.toList());
        if (remoteColumnsAvailable) {
            return new PreparedBatch(String.join(",", names), fullBatch);
        }
        int writeIndex = names.indexOf(REMOTE_WRITE_COLUMN);
        int readIndex = names.indexOf(REMOTE_READ_COLUMN);
        String columns = names.stream().filter(name -> !REMOTE_WRITE_COLUMN.equals(name)
                && !REMOTE_READ_COLUMN.equals(name)).collect(Collectors.joining(","));
        StringBuilder projected = new StringBuilder(fullBatch.length());
        int fieldStart = 0;
        int fieldIndex = 0;
        for (int i = 0; i < fullBatch.length(); i++) {
            char c = fullBatch.charAt(i);
            if (c == AuditLoader.AUDIT_TABLE_COL_SEPARATOR || c == AuditLoader.AUDIT_TABLE_LINE_DELIMITER) {
                if (fieldIndex != writeIndex && fieldIndex != readIndex) {
                    projected.append(fullBatch, fieldStart, i + 1);
                }
                fieldStart = i + 1;
                fieldIndex = c == AuditLoader.AUDIT_TABLE_LINE_DELIMITER ? 0 : fieldIndex + 1;
            }
        }
        return new PreparedBatch(columns, projected);
    }

    static final class PreparedBatch {
        final String columns;
        final StringBuilder payload;

        PreparedBatch(String columns, StringBuilder payload) {
            this.columns = columns;
            this.payload = payload;
        }
    }

    public LoadResponse loadBatch(StringBuilder sb, String clusterToken) {
        String label = genLabel();
        // A new follower can run before the old master adds these columns. The buffer always
        // has the new shape; project it at send time so a batch spanning a schema change is safe.
        PreparedBatch batch = prepareBatch(sb, targetHasRemoteSpillColumns());

        HttpURLConnection feConn = null;
        HttpURLConnection beConn = null;
        try {
            // build request and send to fe
            label = "audit" + label;
            feConn = getConnection(auditLogLoadUrlStr, label, clusterToken, batch.columns);
            int status = feConn.getResponseCode();
            // fe send back http response code TEMPORARY_REDIRECT 307 and new be location
            if (status != 307) {
                throw new Exception("status is not TEMPORARY_REDIRECT 307, status: " + status
                        + ", response: " + getContent(feConn) + ", request is: " + toCurl(feConn));
            }
            String location = feConn.getHeaderField("Location");
            if (location == null) {
                throw new Exception("redirect location is null");
            }
            // build request and send to new be location
            beConn = getConnection(location, label, clusterToken, batch.columns);
            // send data to be
            writeCompressedBody(beConn.getOutputStream(), batch.payload);

            // get respond
            status = beConn.getResponseCode();
            String respMsg = beConn.getResponseMessage();
            String response = getContent(beConn);

            LOG.info("AuditLoader plugin load with label: {}, response code: {}, msg: {}, content: {}",
                    label, status, respMsg, response);

            return new LoadResponse(status, respMsg, response);

        } catch (Exception e) {
            e.printStackTrace();
            String err = "failed to load audit via AuditLoader plugin with label: " + label;
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
        return String.format("_log_%s%02d%02d_%02d%02d%02d_%s_%s",
                calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND),
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
            StringBuilder sb = new StringBuilder();
            sb.append("status: ").append(status);
            sb.append(", resp msg: ").append(respMsg);
            sb.append(", resp content: ").append(respContent);
            return sb.toString();
        }
    }
}
