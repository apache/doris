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

package org.apache.doris.httpv2.interceptor;

import org.apache.doris.qe.ConnectContext;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;

import java.io.IOException;

/** Owns each HTTP dispatch's context, including initialization and authentication failures. */
public class ConnectContextFilter implements Filter {
    private static final String CONTEXT_ATTRIBUTE = ConnectContextFilter.class.getName() + ".context";

    /**
     * Transfers a new context to this dispatch before initializing or binding it. Authentication may
     * inspect the context explicitly before making it current. Reauthentication releases only the
     * previous context owned by this dispatch, never a borrowed thread-local context.
     */
    public static ConnectContext createRequestContext(ServletRequest request) {
        ConnectContext previous = (ConnectContext) request.getAttribute(CONTEXT_ATTRIBUTE);
        if (previous != null) {
            previous.cleanup();
        }
        ConnectContext context = new ConnectContext();
        request.setAttribute(CONTEXT_ATTRIBUTE, context);
        return context;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        ConnectContext previous = ConnectContext.get();
        Object previousRequestContext = request.getAttribute(CONTEXT_ATTRIBUTE);
        try {
            // Nested dispatches borrow neither the outer request's identity nor its resources.
            ConnectContext.remove();
            request.removeAttribute(CONTEXT_ATTRIBUTE);
            chain.doFilter(request, response);
        } finally {
            closeRequestContext(request, previous, previousRequestContext);
        }
    }

    private static void closeRequestContext(ServletRequest request, ConnectContext previous,
            Object previousRequestContext) {
        ConnectContext current = (ConnectContext) request.getAttribute(CONTEXT_ATTRIBUTE);
        try {
            if (current != null) {
                current.cleanup();
            }
        } finally {
            if (previousRequestContext == null) {
                request.removeAttribute(CONTEXT_ATTRIBUTE);
            } else {
                request.setAttribute(CONTEXT_ATTRIBUTE, previousRequestContext);
            }
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }
}
