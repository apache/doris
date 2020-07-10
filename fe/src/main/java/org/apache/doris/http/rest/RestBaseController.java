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

package org.apache.doris.http.rest;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.http.controller.BaseController;
import org.apache.doris.http.exception.UnauthorizedException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.servlet.view.RedirectView;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import io.netty.util.CharsetUtil;

public class RestBaseController extends BaseController {

    protected static final String DB_KEY = "db";
    protected static final String TABLE_KEY = "table";
    protected static final String LABEL_KEY = "label";
    private static final Logger LOG = LogManager.getLogger(RestBaseController.class);

    public void executeCheckPassword(HttpServletRequest request,
                                     HttpServletResponse response) throws DdlException {
        ActionAuthorizationInfo authInfo = getAuthorizationInfo(request);
        // check password
        UserIdentity currentUser = checkPassword(authInfo);
        ConnectContext ctx = new ConnectContext(null);
        ctx.setCatalog(Catalog.getCurrentCatalog());
        ctx.setQualifiedUser(authInfo.fullUserName);
        ctx.setRemoteIP(authInfo.remoteIp);
        ctx.setCurrentUserIdentity(currentUser);
        ctx.setCluster(authInfo.cluster);
        ctx.setThreadLocalInfo();
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

    // If user password should be checked, the derived class should implement this method, NOT 'execute()',
    // otherwise, override 'execute()' directly
    protected void executeWithoutPassword(HttpServletRequest request, HttpServletResponse response)
            throws DdlException {
        throw new DdlException("Not implemented");
    }


    public RedirectView redirectTo(HttpServletRequest request, TNetworkAddress addr)
            throws DdlException {
        URI urlObj = null;
        URI resultUriObj = null;
        String urlStr = request.getRequestURI();
        try {
            urlObj = new URI(urlStr);
            resultUriObj = new URI("http", null, addr.getHostname(),
                    addr.getPort(), urlObj.getPath(), "", null);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            throw new DdlException(e.getMessage());
        }
        RedirectView redirectView = new RedirectView(resultUriObj.toASCIIString()  + request.getQueryString());
        redirectView.setContentType("text/html;charset=utf-8");
        redirectView.setStatusCode(org.springframework.http.HttpStatus.TEMPORARY_REDIRECT);
        return redirectView;
    }

    public RedirectView redirectToMaster(HttpServletRequest request,
                                         HttpServletResponse response) throws DdlException {
        Catalog catalog = Catalog.getCurrentCatalog();
        if (catalog.isMaster()) {
            return null;
        }
        RedirectView redirectView = redirectTo(request, new TNetworkAddress(catalog.getMasterIp(), catalog.getMasterHttpPort()));
        return redirectView;
    }

    public boolean getFile(HttpServletRequest request, HttpServletResponse response, Object obj,String fileName) {
        response.setHeader("Content-type","application/octet-stream");
        response.addHeader("Content-Disposition", "attachment;fileName=" + fileName);// 设置文件名
        if(obj instanceof  File){
            File file = (File)obj;
            byte[] buffer = new byte[1024];
            FileInputStream fis = null;
            BufferedInputStream bis = null;
            try {
                fis = new FileInputStream(file);
                bis = new BufferedInputStream(fis);
                OutputStream os = response.getOutputStream();
                int i = bis.read(buffer);
                while (i != -1) {
                    os.write(buffer, 0, i);
                    i = bis.read(buffer);
                }
                return true;
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (bis != null) {
                    try {
                        bis.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (fis != null) {
                    try {
                        fis.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else if(obj instanceof byte[]){
            try {
                OutputStream os = response.getOutputStream();
                os.write((byte[]) obj);
                return true;
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        return false;
    }


}
