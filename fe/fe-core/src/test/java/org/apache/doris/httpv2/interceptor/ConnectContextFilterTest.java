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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.config.WebConfigurer;
import org.apache.doris.httpv2.controller.BaseController.ActionAuthorizationInfo;
import org.apache.doris.httpv2.rest.LoadAction;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterRegistration;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.core.Ordered;

import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;

class ConnectContextFilterTest {
    private final ConnectContextFilter filter = new ConnectContextFilter();
    private final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    private final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    private boolean savedRunningUnitTest;
    private MockedStatic<Env> envMock;

    @BeforeEach
    void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        envMock = Mockito.mockStatic(Env.class);
        envMock.when(Env::getCurrentEnv).thenReturn(env);
        ConnectContext.remove();
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
        envMock.close();
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    @Test
    void closesAuthenticatedRequestAndDoesNotLeakIntoReusedThread() throws Exception {
        RestBaseController controller = new RestBaseController() {
            @Override
            public ActionAuthorizationInfo getAuthorizationInfo(HttpServletRequest request) {
                return new ActionAuthorizationInfo();
            }

            @Override
            protected UserIdentity checkPassword(ActionAuthorizationInfo authInfo, HttpServletRequest request) {
                return UserIdentity.ROOT;
            }
        };
        AtomicReference<ConnectContext> first = new AtomicReference<>();
        StatementContext statement = Mockito.mock(StatementContext.class);
        filter.doFilter(request, response, (req, res) -> {
            Assertions.assertNull(ConnectContext.get());
            controller.executeCheckPassword(request, response);
            first.set(ConnectContext.get());
            first.get().setStatementContext(statement);
        });
        Assertions.assertNull(ConnectContext.get());
        Assertions.assertTrue(first.get().isClosingOrClosed());
        Assertions.assertNull(first.get().getStatementContext());
        Mockito.verify(statement).close();

        filter.doFilter(request, response, (req, res) -> {
            Assertions.assertNull(ConnectContext.get());
            controller.executeCheckPassword(request, response);
            Assertions.assertNotSame(first.get(), ConnectContext.get());
        });
        Assertions.assertNull(ConnectContext.get());
    }

    @Test
    void closesFailedRequestAndRestoresBorrowedCaller() {
        ConnectContext caller = new ConnectContext();
        caller.setThreadLocalInfo();
        ConnectContext current = new ConnectContext();
        StatementContext statement = Mockito.mock(StatementContext.class);
        current.setStatementContext(statement);
        ServletException failure = new ServletException("request failed");
        try {
            Assertions.assertSame(failure, Assertions.assertThrows(ServletException.class,
                    () -> filter.doFilter(request, response, (req, res) -> {
                        Assertions.assertNull(ConnectContext.get());
                        current.setThreadLocalInfo();
                        throw failure;
                    })));
            Assertions.assertTrue(current.isClosingOrClosed());
            Mockito.verify(statement).close();
            Assertions.assertSame(caller, ConnectContext.get());
            Assertions.assertFalse(caller.isClosingOrClosed());
        } finally {
            caller.cleanup();
        }
    }

    @Test
    void statementCloseFailureStillClosesContextAndPreservesRequestFailure() {
        ConnectContext caller = new ConnectContext();
        caller.setThreadLocalInfo();
        ConnectContext current = new ConnectContext();
        StatementContext statement = Mockito.mock(StatementContext.class);
        IllegalStateException cleanupFailure = new IllegalStateException("statement close failed");
        Mockito.doThrow(cleanupFailure).when(statement).close();
        current.setStatementContext(statement);
        ServletException requestFailure = new ServletException("request failed");
        try {
            Assertions.assertSame(requestFailure, Assertions.assertThrows(ServletException.class,
                    () -> filter.doFilter(request, response, (req, res) -> {
                        current.setThreadLocalInfo();
                        throw requestFailure;
                    })));
            Mockito.verify(statement).close();
            Assertions.assertEquals(0, requestFailure.getSuppressed().length);
            Assertions.assertTrue(current.isClosingOrClosed());
            Assertions.assertNull(current.getStatementContext());
            Assertions.assertSame(caller, ConnectContext.get());
        } finally {
            caller.cleanup();
        }
    }

    @Test
    void nestedDispatchClosesOnlyItsOwnContext() throws Exception {
        ConnectContext outer = new ConnectContext();
        ConnectContext inner = new ConnectContext();
        filter.doFilter(request, response, (req, res) -> {
            outer.setThreadLocalInfo();
            filter.doFilter(request, response, (nestedRequest, nestedResponse) -> {
                Assertions.assertNull(ConnectContext.get());
                inner.setThreadLocalInfo();
            });
            Assertions.assertTrue(inner.isClosingOrClosed());
            Assertions.assertSame(outer, ConnectContext.get());
            Assertions.assertFalse(outer.isClosingOrClosed());
        });
        Assertions.assertTrue(outer.isClosingOrClosed());
        Assertions.assertNull(ConnectContext.get());
    }

    @Test
    void doesNotCloseBorrowedContextWhenRequestRebindsIt() throws Exception {
        ConnectContext borrowed = new ConnectContext();
        borrowed.setThreadLocalInfo();
        try {
            filter.doFilter(request, response, (req, res) -> borrowed.setThreadLocalInfo());
            Assertions.assertSame(borrowed, ConnectContext.get());
            Assertions.assertFalse(borrowed.isClosingOrClosed());
        } finally {
            borrowed.cleanup();
        }
    }

    @Test
    void requestWithoutContextRestoresCallerOnEarlyExit() throws Exception {
        ConnectContext caller = new ConnectContext();
        caller.setThreadLocalInfo();
        try {
            filter.doFilter(request, response, (req, res) -> Assertions.assertNull(ConnectContext.get()));
            Assertions.assertSame(caller, ConnectContext.get());
            Assertions.assertFalse(caller.isClosingOrClosed());
        } finally {
            caller.cleanup();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void streamLoadEarlyResponseRetainsContextUntilRequestScopeCloses(boolean clusterToken) throws Exception {
        LoadAction action = new LoadAction() {
            @Override
            public ActionAuthorizationInfo executeCheckPassword(HttpServletRequest request,
                    HttpServletResponse response) {
                ConnectContext current = new ConnectContext();
                current.setCurrentUserIdentity(UserIdentity.ROOT);
                current.setThreadLocalInfo();
                return new ActionAuthorizationInfo();
            }

            @Override
            protected boolean checkClusterToken(String token) {
                return true;
            }
        };
        Mockito.when(request.getHeaderNames()).thenReturn(Collections.emptyEnumeration());
        if (clusterToken) {
            Mockito.when(request.getHeader("token")).thenReturn("cluster-token");
        }
        AtomicReference<ConnectContext> current = new AtomicReference<>();
        filter.doFilter(request, response, (req, res) -> {
            // Missing Expect causes a normal early error response before backend selection.
            Assertions.assertNotNull(action.streamLoad(request, response, "db", "table"));
            current.set(ConnectContext.get());
            Assertions.assertNotNull(current.get());
            Assertions.assertFalse(current.get().isClosingOrClosed());
        });
        Assertions.assertTrue(current.get().isClosingOrClosed());
        Assertions.assertNull(ConnectContext.get());
    }

    @Test
    void registeredFilterWrapsEveryDispatchBeforeOtherFilters() throws Exception {
        ServletContext servletContext = Mockito.mock(ServletContext.class);
        FilterRegistration.Dynamic registration = Mockito.mock(FilterRegistration.Dynamic.class);
        Mockito.when(servletContext.addFilter(Mockito.anyString(), Mockito.any(Filter.class)))
                .thenReturn(registration);
        FilterRegistrationBean<ConnectContextFilter> filterRegistration = new WebConfigurer().connectContextFilter();
        Assertions.assertEquals(Ordered.HIGHEST_PRECEDENCE, filterRegistration.getOrder());
        filterRegistration.onStartup(servletContext);
        Mockito.verify(registration).addMappingForUrlPatterns(
                EnumSet.allOf(DispatcherType.class), false, "/*");
    }
}
