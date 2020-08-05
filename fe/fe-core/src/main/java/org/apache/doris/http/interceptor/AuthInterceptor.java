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

package org.apache.doris.http.interceptor;

import org.apache.doris.http.HttpAuthManager;
import org.apache.doris.http.HttpAuthManager.SessionValue;
import org.apache.doris.http.controller.BaseController;

import com.google.common.base.Strings;

import org.apache.doris.qe.ConnectContext;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class AuthInterceptor extends BaseController implements HandlerInterceptor {
    private Logger logger = LoggerFactory.getLogger(AuthInterceptor.class);

    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response, Object handler) throws Exception {
        logger.debug("get prehandle. thread: {}", Thread.currentThread().getId());
        String sessionId = getCookieValue(request, BaseController.PALO_SESSION_ID, response);
        SessionValue user = HttpAuthManager.getInstance().getSessionValue(sessionId);
        String method = request.getMethod();
        if (method.equalsIgnoreCase(RequestMethod.OPTIONS.toString())) {
            response.setStatus(HttpStatus.NO_CONTENT.value());
            return true;
        } else {
            String authorization = request.getHeader("Authorization");
            if (!Strings.isNullOrEmpty(authorization) && user == null) {
                request.setAttribute("Authorization", authorization);
                if (checkAuthWithCookie(request, response)) {
                    return true;
                } else {
                    Map<String, Object> map = new HashMap<>();
                    map.put("code", 500);
                    map.put("msg", "Authentication Failed.");
                    response.getOutputStream().println(toJson(map));
                    logger.error("Authentication Failed");
                    return false;
                }
            } else if (user == null || user.equals("")) {
                Map<String, Object> map = new HashMap<>();
                map.put("code", 500);
                map.put("msg", "Authentication Failed.");
                response.getOutputStream().println(toJson(map));
                logger.error("Authentication Failed");
                return false;
            }
        }
        return true;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) throws Exception {
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
    }

    private String toJson(Map<String, Object> map) {
        JSONObject root = new JSONObject(map);
        return root.toString();
    }
}
