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

import com.google.common.base.Strings;

import java.io.BufferedReader;
import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import static org.springframework.http.HttpHeaders.CONNECTION;

public class HttpUtil {
    public static boolean isKeepAlive(HttpServletRequest request) {
        if (!request.getHeader(CONNECTION).equals("close") &&
                (request.getProtocol().equals("") ||
                        request.getHeader(CONNECTION).equals("keep-alive"))) {
            return true;
        }
        return false;
    }

    public static boolean isSslEnable(HttpServletRequest request) {
        String url = request.getRequestURL().toString();
        if (!Strings.isNullOrEmpty(url) && url.startsWith("https")) {
            return true;
        }
        return false;

    }

    public static String getBody(HttpServletRequest request) {
        StringBuffer data = new StringBuffer();
        String line = null;
        BufferedReader reader = null;
        try {
            reader = request.getReader();
            while (null != (line = reader.readLine()))
                data.append(new String(line.getBytes("utf-8")));
        } catch (IOException e) {
        } finally {
        }
        return data.toString();
    }
}
