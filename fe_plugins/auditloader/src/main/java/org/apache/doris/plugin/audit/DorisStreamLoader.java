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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Calendar;

public class DorisStreamLoader {
    private final static Logger LOG = LogManager.getLogger(DorisStreamLoader.class);
    private static String loadUrlPattern = "http://%s/api/%s/%s/_stream_load?";
    private String hostPort;
    private String db;
    private String tbl;
    private String user;
    private String passwd;
    private String loadUrlStr;
    private String authEncoding;

    public DorisStreamLoader(AuditLoaderPlugin.AuditLoaderConf conf) {
        this.hostPort = conf.frontendHostPort;
        this.db = conf.database;
        this.tbl = conf.table;
        this.user = conf.user;
        this.passwd = conf.password;

        this.loadUrlStr = String.format(loadUrlPattern, hostPort, db, tbl);
        this.authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
    }

    public static void main(String[] args) {
        try {
            AuditLoaderPlugin.AuditLoaderConf conf = new AuditLoaderPlugin.AuditLoaderConf();
            conf.frontendHostPort = "fe_host";
            conf.database = "db1";
            conf.table = "tbl1";
            conf.user = "root";
            conf.password = "";

            DorisStreamLoader loader = new DorisStreamLoader(conf);

            StringBuilder sb = new StringBuilder();
            sb.append("1\t2\n3\t4\n");

            System.out.println("before load");
            LoadResponse loadResponse = loader.loadBatch(sb);

            System.out.println(loadResponse);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public LoadResponse loadBatch(StringBuilder sb) {
        Calendar calendar = Calendar.getInstance();
        String label = String.format("audit_%s%02d%02d_%02d%02d%02d",
                calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND));


        try {
            // build requst
            URL loadUrl = new URL(loadUrlStr);
            HttpURLConnection conn = (HttpURLConnection) loadUrl.openConnection();
            conn.setRequestMethod("PUT");
            conn.setRequestProperty("Authorization", "Basic " + authEncoding);
            conn.addRequestProperty("Expect", "100-continue");
            conn.addRequestProperty("Content-Type", "text/plain; charset=UTF-8");

            conn.addRequestProperty("label", label);
            conn.addRequestProperty("max_fiter_ratio", "1.0");
            conn.addRequestProperty("columns", "query_id, time, client_ip, user, db, state, query_time, scan_bytes, scan_rows, return_rows, stmt_id, is_query, frontend_ip, stmt");

            conn.setDoOutput(true);
            conn.setDoInput(true);

            // send data
            BufferedOutputStream bos = new BufferedOutputStream(conn.getOutputStream());
            bos.write(sb.toString().getBytes());
            bos.close();

            // get respond
            int status = conn.getResponseCode();
            String respMsg = conn.getResponseMessage();

            InputStream stream = (InputStream) conn.getContent();
            BufferedReader br = new BufferedReader(new InputStreamReader(stream));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }

            LOG.info("AuditLoader plugin load with label: {}, response code: {}, msg: {}, content: {}",
                    label, status, respMsg, response.toString());

            return new LoadResponse(status, respMsg, response.toString());

        } catch (Exception e) {
            e.printStackTrace();
            String err = "failed to load audit via AuditLoader plugin with label: " + label;
            LOG.warn(err, e);
            return new LoadResponse(-1, e.getMessage(), err);
        }
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
