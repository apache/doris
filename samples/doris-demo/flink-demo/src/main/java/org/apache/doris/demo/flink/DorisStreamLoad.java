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

package org.apache.doris.demo.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.Serializable;
import java.io.IOException;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Calendar;
import java.util.UUID;


/**
 * doris streamLoad
 */

public class DorisStreamLoad implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(DorisStreamLoad.class);

    private static String loadUrlPattern = "http://%s/api/%s/%s/_stream_load?";
    private String hostPort;
    private String db;
    private String tbl;
    private String user;
    private String passwd;
    private String loadUrlStr;
    private String authEncoding;


    public DorisStreamLoad(String hostPort, String db, String tbl, String user, String passwd) {
        this.hostPort = hostPort;
        this.db = db;
        this.tbl = tbl;
        this.user = user;
        this.passwd = passwd;
        this.loadUrlStr = String.format(loadUrlPattern, hostPort, db, tbl);
        this.authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
    }

    private HttpURLConnection getConnection(String urlStr, String label, String columns, String jsonformat) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Authorization", "Basic " + authEncoding);
        conn.addRequestProperty("Expect", "100-continue");
        conn.addRequestProperty("Content-Type", "text/plain; charset=UTF-8");
        conn.addRequestProperty("label", label);
        conn.addRequestProperty("max_filter_ratio", "0");
        conn.addRequestProperty("strict_mode", "true");
        conn.addRequestProperty("columns", columns);
        conn.addRequestProperty("format", "json");
        conn.addRequestProperty("jsonpaths", jsonformat);
        conn.addRequestProperty("strip_outer_array", "true");
        conn.setDoOutput(true);
        conn.setDoInput(true);

        return conn;
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

    public LoadResponse loadBatch(String data, String columns, String jsonformat) {
        Calendar calendar = Calendar.getInstance();
        String label = String.format("audit_%s%02d%02d_%02d%02d%02d_%s",
                calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND),
                UUID.randomUUID().toString().replaceAll("-", ""));

        HttpURLConnection feConn = null;
        HttpURLConnection beConn = null;
        try {
            // build request and send to fe
            feConn = getConnection(loadUrlStr, label, columns, jsonformat);
            int status = feConn.getResponseCode();
            // fe send back http response code TEMPORARY_REDIRECT 307 and new be location
            if (status != 307) {
                throw new Exception("status is not TEMPORARY_REDIRECT 307, status: " + status);
            }
            String location = feConn.getHeaderField("Location");
            if (location == null) {
                throw new Exception("redirect location is null");
            }
            // build request and send to new be location
            beConn = getConnection(location, label, columns, jsonformat);
            // send data to be
            BufferedOutputStream bos = new BufferedOutputStream(beConn.getOutputStream());
            bos.write(data.getBytes());
            bos.close();

            // get respond
            status = beConn.getResponseCode();
            String respMsg = beConn.getResponseMessage();
            InputStream stream = (InputStream) beConn.getContent();
            BufferedReader br = new BufferedReader(new InputStreamReader(stream));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
            return new LoadResponse(status, respMsg, response.toString());

        } catch (Exception e) {
            e.printStackTrace();
            String err = "failed to load audit via AuditLoader plugin with label: " + label;
            log.warn(err, e);
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

}
