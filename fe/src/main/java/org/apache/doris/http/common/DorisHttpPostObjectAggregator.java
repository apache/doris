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

package org.apache.doris.http.common;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;

/*
 * only handle post request, don't handle 100-continue and chunked transfer-encoding http header
 */
public class DorisHttpPostObjectAggregator extends HttpObjectAggregator {
    // the flag for aggregator whether has started
    // in order not to handle chunked transfer-encoding header in {@link isContentMessage} method
    private boolean startAggregated = false;

    public DorisHttpPostObjectAggregator(int maxContentLength) {
        super(maxContentLength, false);
    }

    @Override
    protected boolean isStartMessage(HttpObject msg) throws Exception {
        if (msg instanceof HttpMessage) {
            // Doris FE don't handle chunked transfer-encoding header
            HttpRequest request = (HttpRequest) msg;
            if (request.method().equals(HttpMethod.POST) && !HttpUtil.isTransferEncodingChunked(request)) {
                startAggregated = true;
                return true;
            }
        }
        return false;
    }

    @Override
    protected boolean isContentMessage(HttpObject msg) throws Exception {
        return msg instanceof HttpContent && startAggregated;
    }

    // Doris FE needn't handle 100-continue header
    @Override
    protected Object newContinueResponse(HttpMessage start, int maxContentLength, ChannelPipeline pipeline) {
        return null;
    }
}
