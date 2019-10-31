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

import org.apache.doris.http.action.IndexAction;
import org.apache.doris.http.action.NotFoundAction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URISyntaxException;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;

public class HttpServerHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LogManager.getLogger(HttpServerHandler.class);

    private ActionController controller = null;
    protected FullHttpRequest fullRequest = null;
    protected HttpRequest request = null;
    private BaseAction action = null;
    
    public HttpServerHandler(ActionController controller) {
        super();
        this.controller = controller;
    }
    
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpRequest) {
            this.request = (HttpRequest) msg;
            LOG.debug("request: url:[{}]", request.uri());
            if (!isRequestValid(ctx, request)) {
                writeResponse(ctx, HttpResponseStatus.BAD_REQUEST, "this is a bad request.");
                return;
            }
            BaseRequest req = new BaseRequest(ctx, request);
            
            action = getAction(req);
            if (action != null) {
                LOG.debug("action: {} ", action.getClass().getName());
                action.handleRequest(req);
            }
        } else {
            ReferenceCountUtil.release(msg);
        }
    }

    private boolean isRequestValid(ChannelHandlerContext ctx, HttpRequest request) throws URISyntaxException {
        return true;
    }

    private void writeResponse(ChannelHandlerContext context , HttpResponseStatus status, String content) {
        FullHttpResponse responseObj = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, 
                status, 
                Unpooled.wrappedBuffer(content.getBytes()));
        responseObj.headers().set(HttpHeaderNames.CONTENT_TYPE.toString(), "text/html");
        responseObj.headers().set(HttpHeaderNames.CONTENT_LENGTH.toString(), responseObj.content().readableBytes());
        context.writeAndFlush(responseObj).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
    
    private BaseAction getAction(BaseRequest request) {
        String uri = request.getRequest().uri();
        // ignore this request, which is a default request from client's browser.
        if (uri.endsWith("/favicon.ico")) {
            return NotFoundAction.getNotFoundAction();
        } else if (uri.equals("/")) {
            return new IndexAction(controller);
        }
        
        // Map<String, String> params = Maps.newHashMap();
        BaseAction action = (BaseAction) controller.getHandler(request);
        if (action == null) {
            action = NotFoundAction.getNotFoundAction();
        }
        
        return action;
    }
}
