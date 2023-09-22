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
// This file is copied from
// https://github.com/dremio/dremio-oss/blob/master/services/arrow-flight/src/main/java/com/dremio/service/flight/ServerCookieMiddleware.java
// and modified by Doris

package org.apache.doris.service.arrowflight.auth2;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

/**
 * ServerCookieMiddleware allows a FlightServer to retrieve cookies from the request as well as set outgoing cookies
 * TODO
 */
public final class FlightCookieMiddleware implements FlightServerMiddleware {
    private RequestContext requestContext;
    private Map<String, String> cookieValues;
    private final CallHeaders incomingHeaders;

    public static class Factory implements FlightServerMiddleware.Factory<FlightCookieMiddleware> {
        /**
        * Construct a factory for receiving call headers.
        */
        public Factory() {
        }

        @Override
        public FlightCookieMiddleware onCallStarted(CallInfo callInfo, CallHeaders incomingHeaders,
                                                    RequestContext context) {
            return new FlightCookieMiddleware(callInfo, incomingHeaders, context);
        }
    }


    private FlightCookieMiddleware(CallInfo callInfo, CallHeaders incomingHeaders,
            RequestContext requestContext) {
        this.incomingHeaders = incomingHeaders;
        this.requestContext = requestContext;
        this.cookieValues = new HashMap<String, String>();
        cookieValues.put("SESSION_ID", "1");
    }

    /**
    * Retrieve the headers for this call.
    */
    public CallHeaders headers() {
        return this.incomingHeaders;
    }

    public void addCookie(@NotNull String key, @NotNull String value) {
        // Add to the internal map of values
        this.cookieValues.put(key, value);
    }

    public RequestContext getRequestContext() {
        return this.requestContext;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
        // if internal values are not empty
        if (cookieValues.isEmpty()) {
            return;
        }

        final String cookies = cookieValues.entrySet()
                .stream()
                .map((entry) -> String.format("%s=%s", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(";"));

        // set it in the headers
        outgoingHeaders.insert("Set-Cookie", cookies);
    }

    @Override
    public void onCallCompleted(CallStatus status) {
    }

    @Override
    public void onCallErrored(Throwable err) {
    }
}
