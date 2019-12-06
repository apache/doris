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

import org.apache.doris.common.DdlException;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;

public class BaseRequest {
    protected ChannelHandlerContext context;
    protected HttpRequest request;
    protected Map<String, String> params = Maps.newHashMap();

    private boolean isAuthorized = false;
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
    
    public boolean isAuthorized() {
        return isAuthorized;
    }
    
    public void setAuthorized(boolean isAuthorized) {
        this.isAuthorized = isAuthorized;
    }
    
    public Cookie getCookieByName(String cookieName) {
        String cookieString = request.headers().get(HttpHeaderNames.COOKIE.toString());
        if (!Strings.isNullOrEmpty(cookieString)) {
            Cookie cookie = ClientCookieDecoder.STRICT.decode(cookieString);
            if (cookie.name().equalsIgnoreCase(cookieName)) {
                return cookie;
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

    public String getContent() throws DdlException {
        if (request instanceof FullHttpRequest) {
            FullHttpRequest fullHttpRequest = (FullHttpRequest)  request;
            return fullHttpRequest.content().toString(Charset.forName("UTF-8"));
        } else {
            throw new DdlException("Invalid request");
        }
    }
    
    // get an array parameter.
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
