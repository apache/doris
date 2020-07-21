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

package org.apache.doris.http;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

import io.netty.handler.codec.http.cookie.Cookie;

public class BaseResponse {
    private String contentType;
    protected StringBuilder content = new StringBuilder();

    protected Map<String, List<String>> customHeaders = Maps.newHashMap();
    private Set<Cookie> cookies = Sets.newHashSet();

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public StringBuilder getContent() {
        return content;
    }

    public Set<Cookie> getCookies() {
        return cookies;
    }

    public Map<String, List<String>> getCustomHeaders() {
        return customHeaders;
    }

    // update old key-value mapping of 'name' if Exist, or add new mapping if not exists.
    // It will only change the mapping of 'name', other header will not be changed.
    public void updateHeader(String name, String value) {
        if (customHeaders == null) {
            customHeaders = Maps.newHashMap();
        }
        customHeaders.remove(name);
        addHeader(name, value);
    }

    // Add a custom header.
    private void addHeader(String name, String value) {
        if (customHeaders == null) {
            customHeaders = Maps.newHashMap();
        }
        List<String> header = customHeaders.get(name);
        if (header == null) {
            header = Lists.newArrayList();
            customHeaders.put(name, header);
        }
        header.add(value);
    }

    public void appendContent(String buffer) {
        if (content == null) {
            content = new StringBuilder();
        }
        content.append(buffer);
    }
    
    public void addCookie(Cookie cookie) {
        cookies.add(cookie);
    }
    
    public void updateCookieAge(BaseRequest request, String cookieName, long age) {
        Cookie cookie = request.getCookieByName(cookieName);
        if (cookie != null) {
            cookies.remove(cookie);
            cookie.setMaxAge(age);
            cookies.add(cookie);
        }
    }
}
