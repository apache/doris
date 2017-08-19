// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.Cookie;
import io.netty.handler.codec.http.CookieDecoder;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BaseRequest {
    protected ChannelHandlerContext context;
    protected HttpRequest request;
    protected Map<String, String> params = Maps.newHashMap();

    private boolean isAdmin = false;
    private QueryStringDecoder decoder;

    public BaseRequest(ChannelHandlerContext ctx, HttpRequest request) {
        this.context = ctx;
        this.request = request;
    }
    
    public ChannelHandlerContext getContext() {
        return context;
    }
    
    public void setContext(ChannelHandlerContext context) {
        this.context = context;
    }

    public HttpRequest getRequest() {
        return request;
    }
    
    public void setRequest(HttpRequest request) {
        this.request = request;
    }
    
    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }
    
    public boolean isAdmin() {
        return isAdmin;
    }
    
    public void setAdmin(boolean isAdmin) {
        this.isAdmin = isAdmin;
    }
    
    public Cookie getCookieByName(String cookieName) {
        String cookieString = request.headers().get(HttpHeaders.Names.COOKIE);
        if (!Strings.isNullOrEmpty(cookieString)) {
            Set<Cookie> cookies = CookieDecoder.decode(cookieString);
            if (!cookies.isEmpty()) {
                for (Cookie cookie : cookies) {
                    if (cookie.name().equalsIgnoreCase(cookieName)) {
                        return cookie;
                    }
                }
            }
        }
        return null;
    }
    
    public String getCookieValue(String cookieName) {
        Cookie cookie = getCookieByName(cookieName);
        if (cookie != null) {
            return cookie.value();
        }
        return null;
    }
    
    // get a single parameter.
    // return null if key is not exist; return the first value if key is an array
    public String getSingleParameter(String key) {
        String uri = request.uri();
        if (decoder == null) {
            decoder = new QueryStringDecoder(uri);
        }

        List<String> values = decoder.parameters().get(key);
        if (values != null && values.size() > 0) {
            return values.get(0);
        }
        
        return params.get(key);
    }
    
    // get an array patameter.
    // eg.  ?a=1&a=2
    public List<String> getArrayParameter(String key) {
        String uri = request.uri();
        if (decoder == null) {
            decoder = new QueryStringDecoder(uri);
        }

        return decoder.parameters().get(key);
    }
    
    public Map<String, List<String>> getAllParameters() {
        String uri = request.uri();
        if (decoder == null) {
            decoder = new QueryStringDecoder(uri);
        }

        return decoder.parameters();
    }

    public String getAuthorizationHeader() {
        String authString = request.headers().get("Authorization");
        return authString;
    }
    
    public String getHostString() {
     // get client host
        InetSocketAddress clientSocket = (InetSocketAddress) context.channel().remoteAddress();
        String clientIp = clientSocket.getHostString();
        return clientIp;
    }
}
