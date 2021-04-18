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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.doris.common.path.PathTrie;
import org.apache.doris.http.rest.LoadAction;
import com.google.common.base.Strings;

import io.netty.handler.codec.http.HttpMethod;

public class ActionController {
    private static final Logger LOG = LogManager.getLogger(LoadAction.class);
    private final PathTrie<IAction> getHandlers = new PathTrie<>(WebUtils.REST_DECODER);
    private final PathTrie<IAction> postHandlers = new PathTrie<>(WebUtils.REST_DECODER);
    private final PathTrie<IAction> putHandlers = new PathTrie<>(WebUtils.REST_DECODER);
    private final PathTrie<IAction> deleteHandlers = new PathTrie<>(WebUtils.REST_DECODER);
    private final PathTrie<IAction> headHandlers = new PathTrie<>(WebUtils.REST_DECODER);
    private final PathTrie<IAction> optionsHandlers = new PathTrie<>(WebUtils.REST_DECODER);
    
    // Registers a rest handler to be execute when the provided method and path match the request.
    public void registerHandler(HttpMethod method, String path, IAction handler) 
            throws IllegalArgException {
        if (method.equals(HttpMethod.GET)) {
            getHandlers.insert(path, handler);
        } else if (method.equals(HttpMethod.POST)) {
            postHandlers.insert(path, handler);
        } else if (method.equals(HttpMethod.HEAD)) {
            headHandlers.insert(path, handler);
        } else if (method.equals(HttpMethod.DELETE)) {
            deleteHandlers.insert(path, handler);
        } else if (method.equals(HttpMethod.OPTIONS)) {
            optionsHandlers.insert(path, handler);
        } else if (method.equals(HttpMethod.PUT)) {
            putHandlers.insert(path, handler);
        } else {
            throw new IllegalArgException(
                    "Can't handle [" + method + "] for path [" + path + "]");
        }
    }
    
    public IAction getHandler(BaseRequest request) {
        String path = getPath(request.getRequest().uri());
        HttpMethod method = request.getRequest().method();
        if (method.equals(HttpMethod.GET)) {
            return getHandlers.retrieve(path, request.getParams());
        } else if (method.equals(HttpMethod.POST)) {
            return postHandlers.retrieve(path, request.getParams());
        } else if (method.equals(HttpMethod.PUT)) {
            return putHandlers.retrieve(path, request.getParams());
        } else if (method.equals(HttpMethod.DELETE)) {
            return deleteHandlers.retrieve(path, request.getParams());
        } else if (method.equals(HttpMethod.HEAD)) {
            return headHandlers.retrieve(path, request.getParams());
        } else if (method.equals(HttpMethod.OPTIONS)) {
            return optionsHandlers.retrieve(path, request.getParams());
        } else {
            return null;
        }
    }
    
    // e.g. 
    // in: /www/system?path=//jobs
    // out: /www/system
    private String getPath(String uri) {
        if (Strings.isNullOrEmpty(uri)) {
            return "";
        }
        int pathEndIndex = uri.indexOf('?');
        if (pathEndIndex < 0) {
            return uri;
        } else {
            return uri.substring(0, pathEndIndex);
        }
    }
}
