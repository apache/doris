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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.http.rest.UnauthorizedException;
import com.baidu.palo.mysql.MysqlPassword;
import com.baidu.palo.qe.QeService;
import com.baidu.palo.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.DefaultFileRegion;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.Cookie;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.ServerCookieEncoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;
import io.netty.util.CharsetUtil;

public abstract class BaseAction implements IAction {
    private static final Logger LOG = LogManager.getLogger(BaseAction.class);

    protected QeService qeService = null;
    protected ActionController controller;
    protected Catalog catalog;

    public BaseAction(ActionController controller) {
        this.controller = controller;
        // TODO(zc): remove this instance
        this.catalog = Catalog.getInstance();
    }

    public QeService getQeService() {
        return qeService;
    }

    public void setQeService(QeService qeService) {
        this.qeService = qeService;
    }

    @Override
    public void handleRequest(BaseRequest request) throws Exception {
        BaseResponse response = new BaseResponse();
        execute(request, response);
    }

    public abstract void execute(BaseRequest request, BaseResponse response) throws DdlException;

    protected void writeResponse(BaseRequest request, BaseResponse response, HttpResponseStatus status) {
        // if (HttpHeaders.is100ContinueExpected(request.getRequest())) {
        // ctx.write(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
        // HttpResponseStatus.CONTINUE));
        // }

        FullHttpResponse responseObj = null;
        try {
            responseObj = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
                    Unpooled.wrappedBuffer(response.getContent().toString().getBytes("UTF-8")));
        } catch (UnsupportedEncodingException e) {
            LOG.warn("get exception.", e);
            responseObj = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
                    Unpooled.wrappedBuffer(response.getContent().toString().getBytes()));
        }
        Preconditions.checkNotNull(responseObj);
        HttpMethod method = request.getRequest().method();

        checkDefaultContentTypeHeader(response, responseObj);
        if (!method.equals(HttpMethod.HEAD)) {
            response.updateHeader(HttpHeaders.Names.CONTENT_LENGTH,
                    String.valueOf(responseObj.content().readableBytes()));
        }
        writeCustomHeaders(response, responseObj);
        writeCookies(response, responseObj);

        boolean keepAlive = HttpHeaders.isKeepAlive(request.getRequest());
        if (!keepAlive) {
            request.getContext().write(responseObj).addListener(ChannelFutureListener.CLOSE);
        } else {
            responseObj.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
            request.getContext().write(responseObj);
        }
    }

    protected void writeFileResponse(BaseRequest request, BaseResponse response, HttpResponseStatus status,
            File resFile) {
        HttpResponse responseObj = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);

        if (HttpHeaders.isKeepAlive(request.getRequest())) {
            response.updateHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        }

        ChannelFuture sendFileFuture;
        ChannelFuture lastContentFuture;

        // Read and return file content
        RandomAccessFile rafFile;
        try {
            rafFile = new RandomAccessFile(resFile, "r");
            long fileLength = 0;
            fileLength = rafFile.length();
            response.updateHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(fileLength));
            writeCookies(response, responseObj);
            writeCustomHeaders(response, responseObj);

            // Write headers
            request.getContext().write(responseObj);

            // Write file
            if (request.getContext().pipeline().get(SslHandler.class) == null) {
                sendFileFuture = request.getContext().write(new DefaultFileRegion(rafFile.getChannel(), 0, fileLength),
                        request.getContext().newProgressivePromise());
                // Write the end marker.
                lastContentFuture = request.getContext().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            } else {
                sendFileFuture = request.getContext().writeAndFlush(
                        new HttpChunkedInput(new ChunkedFile(rafFile, 0, fileLength, 8192)),
                        request.getContext().newProgressivePromise());
                // HttpChunkedInput will write the end marker (LastHttpContent)
                // for us.
                lastContentFuture = sendFileFuture;
            }
        } catch (FileNotFoundException ignore) {
            writeResponse(request, response, HttpResponseStatus.NOT_FOUND);
            return;
        } catch (IOException e1) {
            writeResponse(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
        }

        sendFileFuture.addListener(new ChannelProgressiveFutureListener() {
            @Override
            public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
                if (total < 0) { // total unknown
                    LOG.debug("{} Transfer progress: {}", future.channel(), progress);
                } else {
                    LOG.debug("{} Transfer progress: {} / {}", future.channel(), progress, total);
                }
            }

            @Override
            public void operationComplete(ChannelProgressiveFuture future) {
                LOG.debug("{} Transfer complete.", future.channel());
                if (!future.isSuccess()) {
                    Throwable cause = future.cause();
                    LOG.error("something wrong. ", cause);
                }
            }
        });

        // Decide whether to close the connection or not.
        boolean keepAlive = HttpHeaders.isKeepAlive(request.getRequest());
        if (!keepAlive) {
            // Close the connection when the whole content is written out.
            lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        }
    }

    // Set 'CONTENT_TYPE' header if it havn't been set.
    protected void checkDefaultContentTypeHeader(BaseResponse response, Object responseOj) {
        List<String> header = response.getCustomHeaders().get(HttpHeaders.Names.CONTENT_TYPE);
        if (header == null) {
            response.updateHeader(HttpHeaders.Names.CONTENT_TYPE, "text/html");
        }
    }

    protected void writeCustomHeaders(BaseResponse response, HttpResponse responseObj) {
        for (Map.Entry<String, List<String>> entry : response.getHeaders().entrySet()) {
            responseObj.headers().add(entry.getKey(), entry.getValue());
        }
    }

    protected void writeCookies(BaseResponse response, HttpResponse responseObj) {
        for (Cookie cookie : response.getCookies()) {
            responseObj.headers().add(HttpHeaders.Names.SET_COOKIE, ServerCookieEncoder.encode(cookie));
        }
    }

    public static class AuthorizationInfo {
        public String fullUserName;
        public String password;
        public String cluster;
    }

    public boolean parseAuth(BaseRequest request, AuthorizationInfo authInfo) {
        String encodedAuthString = request.getAuthorizationHeader();
        if (Strings.isNullOrEmpty(encodedAuthString)) {
            return false;
        }
        String[] parts = encodedAuthString.split(" ");
        if (parts.length != 2) {
            return false;
        }
        encodedAuthString = parts[1];
        ByteBuf buf = null;
        try {
            buf = Unpooled.copiedBuffer(ByteBuffer.wrap(encodedAuthString.getBytes()));

            // The authString is a string connecting user-name and password with
            // a colon(':')
            String authString = Base64.decode(buf).toString(CharsetUtil.UTF_8);
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
        } finally {
            // release the buf after using Unpooled.copiedBuffer
            // or it will get memory leak
            if (buf != null) {
                buf.release();
            }
        }
        return true;
    }

    // check authenticate information
    private AuthorizationInfo checkAndGetUser(BaseRequest request) throws DdlException {
        AuthorizationInfo authInfo = new AuthorizationInfo();
        if (!parseAuth(request, authInfo)) {
            throw new UnauthorizedException("Need auth information.");
        }
        byte[] hashedPasswd = catalog.getUserMgr().getPassword(authInfo.fullUserName);
        if (hashedPasswd == null) {
            // No such user
            throw new DdlException("No such user(" + authInfo.fullUserName + ")");
        }
        if (!MysqlPassword.checkPlainPass(hashedPasswd, authInfo.password)) {
            throw new DdlException("Password error");
        }
        return authInfo;
    }

    protected void checkAdmin(BaseRequest request) throws DdlException {
        final AuthorizationInfo authInfo = checkAndGetUser(request);
        if (!catalog.getUserMgr().isAdmin(authInfo.fullUserName)) {
            throw new DdlException("Administrator needed");
        }
    }

    protected void checkReadPriv(String fullUserName, String fullDbName) throws DdlException {
        if (!catalog.getUserMgr().checkAccess(fullUserName, fullDbName, AccessPrivilege.READ_ONLY)) {
            throw new DdlException("Read Privilege needed");
        }
    }

    protected void checkWritePriv(String fullUserName, String fullDbName) throws DdlException {
        if (!catalog.getUserMgr().checkAccess(fullUserName, fullDbName, AccessPrivilege.READ_WRITE)) {
            throw new DdlException("Write Privilege needed");
        }
    }

    public AuthorizationInfo getAuthorizationInfo(BaseRequest request) throws DdlException {
        return checkAndGetUser(request);
    }

}
