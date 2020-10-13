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

package org.apache.doris.httpv2.controller;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import io.netty.util.CharsetUtil;

import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.HttpAuthManager;
import org.apache.doris.httpv2.HttpAuthManager.SessionValue;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.mysql.privilege.PaloPrivilege;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class BaseController {

    private static final Logger LOG = LogManager.getLogger(BaseController.class);

    public static final String PALO_SESSION_ID = "PALO_SESSION_ID";
    private static final int PALO_SESSION_EXPIRED_TIME = 3600 * 24; // one day

    public void checkAuthWithCookie(HttpServletRequest request, HttpServletResponse response) {
        checkWithCookie(request, response, true);
    }

    public ActionAuthorizationInfo checkWithCookie(HttpServletRequest request, HttpServletResponse response, boolean checkAuth) {
        // First we check if the request has Authorization header.
        String encodedAuthString = request.getHeader("Authorization");
        if (encodedAuthString != null) {
            // If has Authorization header, check auth info
            ActionAuthorizationInfo authInfo = getAuthorizationInfo(request);
            UserIdentity currentUser = checkPassword(authInfo);

            if (checkAuth) {
                checkGlobalAuth(currentUser, PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ADMIN_PRIV,
                        PaloPrivilege.NODE_PRIV), CompoundPredicate.Operator.OR));
            }

            SessionValue value = new SessionValue();
            value.currentUser = currentUser;
            value.password = authInfo.password;
            addSession(request, response, value);

            ConnectContext ctx = new ConnectContext(null);
            ctx.setQualifiedUser(authInfo.fullUserName);
            ctx.setRemoteIP(authInfo.remoteIp);
            ctx.setCurrentUserIdentity(currentUser);
            ctx.setCatalog(Catalog.getCurrentCatalog());
            ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
            ctx.setThreadLocalInfo();
            LOG.debug("check auth without cookie success for user: {}, thread: {}",
                    currentUser, Thread.currentThread().getId());
            return authInfo;
        }

        // No Authorization header, check cookie
        ActionAuthorizationInfo authInfo = checkCookie(request, response, checkAuth);
        if (authInfo == null) {
            throw new UnauthorizedException("Cookie is invalid");
        }
        return authInfo;
    }

    protected void addSession(HttpServletRequest request, HttpServletResponse response, SessionValue value) {
        String key = UUID.randomUUID().toString();
        Cookie cookie = new Cookie(PALO_SESSION_ID, key);
        cookie.setMaxAge(PALO_SESSION_EXPIRED_TIME);
        cookie.setPath("/");
        response.addCookie(cookie);
        LOG.debug("add session cookie: {} {}", PALO_SESSION_ID, key);
        HttpAuthManager.getInstance().addSessionValue(key, value);
    }

    private ActionAuthorizationInfo checkCookie(HttpServletRequest request, HttpServletResponse response,
                                                boolean checkAuth) {
        List<String> sessionIds = getCookieValues(request, PALO_SESSION_ID, response);
        if (sessionIds.isEmpty()) {
            return null;
        }

        HttpAuthManager authMgr = HttpAuthManager.getInstance();
        SessionValue sessionValue = authMgr.getSessionValue(sessionIds);
        if (sessionValue == null) {
            return null;
        }

        if (checkAuth && !Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(sessionValue.currentUser,
                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ADMIN_PRIV,
                        PaloPrivilege.NODE_PRIV), CompoundPredicate.Operator.OR))) {
            // need to check auth and check auth failed
            return null;
        }

        updateCookieAge(request, PALO_SESSION_ID, PALO_SESSION_EXPIRED_TIME, response);

        ConnectContext ctx = new ConnectContext(null);
        ctx.setQualifiedUser(sessionValue.currentUser.getQualifiedUser());
        ctx.setRemoteIP(request.getRemoteHost());
        ctx.setCurrentUserIdentity(sessionValue.currentUser);
        ctx.setCatalog(Catalog.getCurrentCatalog());
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        ctx.setThreadLocalInfo();
        LOG.debug("check cookie success for user: {}, thread: {}",
                sessionValue.currentUser, Thread.currentThread().getId());
        ActionAuthorizationInfo authInfo = new ActionAuthorizationInfo();
        authInfo.fullUserName = sessionValue.currentUser.getQualifiedUser();
        authInfo.remoteIp = request.getRemoteHost();
        authInfo.password = sessionValue.password;
        authInfo.cluster = SystemInfoService.DEFAULT_CLUSTER;
        return authInfo;
    }

    public List<String> getCookieValues(HttpServletRequest request, String cookieName, HttpServletResponse response) {
        Cookie[] cookies = request.getCookies();
        List<String> sessionIds = Lists.newArrayList();
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (cookie.getName() != null && cookie.getName().equals(cookieName)) {
                    String sessionId = cookie.getValue();
                    LOG.debug("get cookie value. {}: {}", cookie.getName(), sessionId);
                    sessionIds.add(sessionId);
                }
            }
        }
        return sessionIds;
    }

    public void updateCookieAge(HttpServletRequest request, String cookieName, int age, HttpServletResponse response) {
        Cookie[] cookies = request.getCookies();
        for (Cookie cookie : cookies) {
            if (cookie.getName() != null && cookie.getName().equals(cookieName)) {
                cookie.setMaxAge(age);
                response.addCookie(cookie);
                LOG.debug("get update cookie: {} {}", cookie.getName(), cookie.getValue());
            }
        }
    }

    public static class ActionAuthorizationInfo {
        public String fullUserName;
        public String remoteIp;
        public String password;
        public String cluster;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("user: ").append(fullUserName).append(", remote ip: ").append(remoteIp);
            sb.append(", password: ").append(password).append(", cluster: ").append(cluster);
            return sb.toString();
        }
    }

    protected void checkGlobalAuth(UserIdentity currentUser, PrivPredicate predicate) throws UnauthorizedException {
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(currentUser, predicate)) {
            throw new UnauthorizedException("Access denied; you need (at least one of) the "
                    + predicate.getPrivs().toString() + " privilege(s) for this operation");
        }
    }

    protected void checkDbAuth(UserIdentity currentUser, String db, PrivPredicate predicate)
            throws UnauthorizedException {
        if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(currentUser, db, predicate)) {
            throw new UnauthorizedException("Access denied; you need (at least one of) the "
                    + predicate.getPrivs().toString() + " privilege(s) for this operation");
        }
    }

    protected void checkTblAuth(UserIdentity currentUser, String db, String tbl, PrivPredicate predicate)
            throws UnauthorizedException {
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(currentUser, db, tbl, predicate)) {
            throw new UnauthorizedException("Access denied; you need (at least one of) the "
                    + predicate.getPrivs().toString() + " privilege(s) for this operation");
        }
    }

    // return currentUserIdentity from Doris auth
    protected UserIdentity checkPassword(ActionAuthorizationInfo authInfo)
            throws UnauthorizedException {
        List<UserIdentity> currentUser = Lists.newArrayList();
        if (!Catalog.getCurrentCatalog().getAuth().checkPlainPassword(authInfo.fullUserName,
                authInfo.remoteIp, authInfo.password, currentUser)) {
            throw new UnauthorizedException("Access denied for "
                    + authInfo.fullUserName + "@" + authInfo.remoteIp);
        }
        Preconditions.checkState(currentUser.size() == 1);
        return currentUser.get(0);
    }

    public ActionAuthorizationInfo getAuthorizationInfo(HttpServletRequest request)
            throws UnauthorizedException {
        ActionAuthorizationInfo authInfo = new ActionAuthorizationInfo();
        if (!parseAuthInfo(request, authInfo)) {
            LOG.info("parse auth info failed, Authorization header {}, url {}",
                    request.getHeader("Authorization"), request.getRequestURI());
            throw new UnauthorizedException("Need auth information.");
        }
        LOG.debug("get auth info: {}", authInfo);
        return authInfo;
    }

    private boolean parseAuthInfo(HttpServletRequest request, ActionAuthorizationInfo authInfo) {
        String encodedAuthString = request.getHeader("Authorization");
        if (Strings.isNullOrEmpty(encodedAuthString)) {
            return false;
        }
        String[] parts = encodedAuthString.split(" ");
        if (parts.length != 2) {
            return false;
        }
        encodedAuthString = parts[1];
        ByteBuf buf = null;
        ByteBuf decodeBuf = null;
        try {
            buf = Unpooled.copiedBuffer(ByteBuffer.wrap(encodedAuthString.getBytes()));

            // The authString is a string connecting user-name and password with
            // a colon(':')
            decodeBuf = Base64.decode(buf);
            String authString = decodeBuf.toString(CharsetUtil.UTF_8);
            // Note that password may contain colon, so can not simply use a
            // colon to split.
            int index = authString.indexOf(":");
            authInfo.fullUserName = authString.substring(0, index);
            final String[] elements = authInfo.fullUserName.split("@");
            if (elements != null && elements.length < 2) {
                authInfo.fullUserName = ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER,
                        authInfo.fullUserName);
                authInfo.cluster = SystemInfoService.DEFAULT_CLUSTER;
            } else if (elements != null && elements.length == 2) {
                authInfo.fullUserName = ClusterNamespace.getFullName(elements[1], elements[0]);
                authInfo.cluster = elements[1];
            }
            authInfo.password = authString.substring(index + 1);
            authInfo.remoteIp = request.getRemoteAddr();
        } finally {
            // release the buf and decode buf after using Unpooled.copiedBuffer
            // or it will get memory leak
            if (buf != null) {
                buf.release();
            }

            if (decodeBuf != null) {
                decodeBuf.release();
            }
        }
        return true;
    }

    protected int checkIntParam(String strParam) {
        return Integer.parseInt(strParam);
    }

    protected long checkLongParam(String strParam) {
        return Long.parseLong(strParam);
    }

    protected String getCurrentFrontendURL() {
        return "http://" + FrontendOptions.getLocalHostAddress() + ":" + Config.http_port;
    }
}

