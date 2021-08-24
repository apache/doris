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

import java.util.Base64;
import java.util.Map;

public class PaloClient {

    protected static final String NAMESPACE = "namespaces";

    protected static final String DATABASE = "databases";

    protected static final String TABLE = "tables";

    protected static final int LOGIN_SUCCESS_CODE = 200;

    protected static final int REQUEST_SUCCESS_CODE = 0;

    protected String getHostUrl(String host, int port) {
        StringBuffer buffer = new StringBuffer();
        buffer.append("http://");
        buffer.append(host);
        buffer.append(":");
        buffer.append(port);
        return buffer.toString();
    }

    protected void setHeaders(Map<String, String> headers) {
        headers.put("Accept", "*/*");
        headers.put("Accept-Encoding", "gzip, deflate");
        headers.put("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8");
        headers.put("Connection", "keep-alive");
    }

    protected void setPostHeaders(Map<String, String> headers) {
        headers.put("Content-Type", "application/json; charset=utf-8");
    }

    protected void setAuthHeaders(Map<String, String> headers, String user, String passwd) {
        String authInfo = getBasicAuthInfo(user, passwd);
        headers.put("Authorization", authInfo);
    }

    /**
     *
     * @param headers
     * @param sessionId
     */
    protected void setCookieHeaders(Map<String, String> headers, String sessionId) {
        headers.put("Cookie", "PALO_SESSION_ID=" + sessionId);
    }

    private String getBasicAuthInfo(String user, String password) {
        if (password == null) {
            password = "";
        }
        String userPass = user + ":" + password;
        Base64.Encoder encoder = Base64.getEncoder();
        return "Basic " + encoder.encodeToString(userPass.getBytes());
    }
}
