// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.http;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.netty.handler.codec.http.Cookie;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class BaseResponse {
    private HttpResponseStatus status;
    private String contentType;
    protected StringBuilder content = new StringBuilder();
    protected Map<String, List<String>> customHeaders = Maps.newHashMap();
    private Set<Cookie> cookies = Sets.newHashSet();
    
    public Map<String, List<String>> getCustomHeaders() {
        return customHeaders;
    }
    public void setCustomHeaders(Map<String, List<String>> customHeaders) {
        this.customHeaders = customHeaders;
    }
    public String getContentType() {
        return contentType;
    }
    public void setContentType(String contentType) {
        this.contentType = contentType;
    }
    public StringBuilder getContent() {
        return content;
    }
    public void setContent(StringBuilder buffer) {
        this.content = buffer;
    }
    public Set<Cookie> getCookies() {
        return cookies;
    }
    public void setCookies(Set<Cookie> cookies) {
        this.cookies = cookies;
    }
    
    public void addHeaders(Map<String, List<String>> headers) {
        if (customHeaders == null) {
            customHeaders = Maps.newHashMapWithExpectedSize(headers.size());
        }
        for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
            List<String> values = customHeaders.get(entry.getKey());
            if (values == null) {
                values = Lists.newArrayList();
                customHeaders.put(entry.getKey(), values);
            }
            values.addAll(entry.getValue());
        }
    }

    // Add a custom header.
    public void addHeader(String name, String value) {
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
    
    public void updateHeaders(Map<String, List<String>> headers) {
        if (customHeaders == null) {
            customHeaders = Maps.newHashMapWithExpectedSize(headers.size());
        }
        for (String keyName : headers.keySet()) {
            customHeaders.remove(keyName);
        }
        this.addHeaders(headers);
    }
    
    // update old key-value mapping of 'name' if Exist, or add new mapping if not exists.
    // It will only change the mapping of 'name', other header will not be changed. 
    public void updateHeader(String name, String value) {
        if (customHeaders == null) {
            customHeaders = Maps.newHashMap();
        }
        customHeaders.remove(name);
        this.addHeader(name, value);
    }

    // Returns custom headers that have been added, or null if none have been set.
    public Map<String, List<String>> getHeaders() {
        return customHeaders;
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
