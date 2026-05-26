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

package org.apache.doris.httpv2.meta;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.httpv2.rest.RestApiStatusCode;
import org.apache.doris.master.MetaHelper;
import org.apache.doris.persist.Storage;
import org.apache.doris.system.Frontend;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.ResponseEntity;

import java.io.File;
import java.lang.reflect.Field;

public class MetaServiceTest {
    private static final String CLUSTER_TOKEN = "cluster-token";
    private static final String CONFIG_TOKEN = "config-token";
    private static final String BAD_TOKEN = "bad-token";
    private static final String FE_HOST = "127.0.0.1";
    private static final int FE_EDIT_LOG_PORT = 9010;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private boolean oldLegacyNodeIdentAuth;
    private boolean oldEnableAllHttpAuth;
    private String oldAuthToken;
    private int oldHttpPort;
    private Env env;
    private MockedStatic<Env> envStatic;

    @Before
    public void setUp() {
        oldLegacyNodeIdentAuth = Config.enable_meta_service_legacy_node_ident_auth;
        oldEnableAllHttpAuth = Config.enable_all_http_auth;
        oldAuthToken = Config.auth_token;
        oldHttpPort = Config.http_port;
        Config.enable_meta_service_legacy_node_ident_auth = true;
        Config.enable_all_http_auth = false;
        Config.auth_token = CONFIG_TOKEN;
        Config.http_port = 8030;

        env = Mockito.mock(Env.class);
        envStatic = Mockito.mockStatic(Env.class);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);

        Frontend frontend = new Frontend(FrontendNodeType.FOLLOWER, "fe1", FE_HOST, FE_EDIT_LOG_PORT);
        Mockito.when(env.checkFeExist(FE_HOST, FE_EDIT_LOG_PORT)).thenReturn(frontend);
        Mockito.when(env.getToken()).thenReturn(CLUSTER_TOKEN);
        Mockito.when(env.getImageDir()).thenReturn(temporaryFolder.getRoot().getAbsolutePath());
    }

    @After
    public void tearDown() {
        Config.enable_meta_service_legacy_node_ident_auth = oldLegacyNodeIdentAuth;
        Config.enable_all_http_auth = oldEnableAllHttpAuth;
        Config.auth_token = oldAuthToken;
        Config.http_port = oldHttpPort;
        if (envStatic != null) {
            envStatic.close();
        }
    }

    @Test
    public void testTokenAllowsMetaRequestFromProxyAddress() throws Exception {
        MetaService service = new MetaService();
        HttpServletRequest request = newRequest("192.0.2.10", CLUSTER_TOKEN);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Object result = service.role(request, response);

        Assert.assertEquals(RestApiStatusCode.OK.code, responseCode(result));
        Mockito.verify(response).setHeader("role", FrontendNodeType.FOLLOWER.name());
    }

    @Test
    public void testConfigAuthTokenAllowsMetaRequestWhenEnvTokenIsEmpty() throws Exception {
        Mockito.when(env.getToken()).thenReturn("");
        MetaService service = new MetaService();
        HttpServletRequest request = newRequest("192.0.2.10", CONFIG_TOKEN);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Object result = service.role(request, response);

        Assert.assertEquals(RestApiStatusCode.OK.code, responseCode(result));
        Mockito.verify(response).setHeader("role", FrontendNodeType.FOLLOWER.name());
    }

    @Test
    public void testLegacyFallbackAllowsProxyAddress() throws Exception {
        MetaService service = new MetaService();
        HttpServletRequest request = newRequest("192.0.2.10", null);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Object result = service.role(request, response);

        Assert.assertEquals(RestApiStatusCode.OK.code, responseCode(result));
        Mockito.verify(response).setHeader("role", FrontendNodeType.FOLLOWER.name());
    }

    @Test
    public void testLegacyFallbackAllowsCheckFromProxyAddress() throws Exception {
        MetaService service = serviceWithImageDir();
        HttpServletRequest request = newRequest("192.0.2.10", null);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Object result = service.check(request, response);

        Assert.assertEquals(RestApiStatusCode.OK.code, responseCode(result));
        Mockito.verify(response).setHeader(MetaBaseAction.TOKEN, CLUSTER_TOKEN);
    }

    @Test
    public void testWrongTokenDoesNotFallBackToLegacyAddressCheck() {
        MetaService service = new MetaService();
        HttpServletRequest request = newRequest(FE_HOST, BAD_TOKEN);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Assert.assertThrows(UnauthorizedException.class, () -> service.role(request, response));
    }

    @Test
    public void testLegacyFallbackCanBeDisabled() {
        Config.enable_meta_service_legacy_node_ident_auth = false;
        MetaService service = new MetaService();
        HttpServletRequest request = newRequest(FE_HOST, null);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Assert.assertThrows(UnauthorizedException.class, () -> service.role(request, response));
    }

    @Test
    public void testPutRejectsUnexpectedHttpPort() throws Exception {
        MetaService service = new MetaService();
        HttpServletRequest request = newRequest(FE_HOST, CLUSTER_TOKEN);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getParameter("version")).thenReturn("100");
        Mockito.when(request.getParameter("port")).thenReturn(Integer.toString(Config.http_port + 1));

        try (MockedStatic<MetaHelper> metaHelper = Mockito.mockStatic(MetaHelper.class)) {
            File partialFile = temporaryFolder.newFile("image.100.part");
            metaHelper.when(() -> MetaHelper.getFile(Mockito.anyString(), Mockito.any(File.class)))
                    .thenReturn(partialFile);

            Object result = service.put(request, response);

            Assert.assertEquals(RestApiStatusCode.BAD_REQUEST.code, responseCode(result));
            metaHelper.verify(() -> MetaHelper.getRemoteFile(Mockito.anyString(), Mockito.anyInt(),
                    Mockito.any(File.class)), Mockito.never());
        }
    }

    @Test
    public void testDumpAlwaysChecksPassword() throws Exception {
        MetaService service = Mockito.spy(new MetaService());
        HttpServletRequest request = newRequest(FE_HOST, CLUSTER_TOKEN);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.doReturn(null).when(service).executeCheckPassword(request, response);
        Mockito.when(env.dumpImage()).thenReturn(null);

        service.dump(request, response);

        Mockito.verify(service).executeCheckPassword(request, response);
    }

    private MetaService serviceWithImageDir() throws Exception {
        File imageDir = temporaryFolder.newFolder("image");
        Storage storage = new Storage(12345, CLUSTER_TOKEN, imageDir.getAbsolutePath());
        storage.writeClusterIdAndToken();

        MetaService service = new MetaService();
        Field imageDirField = MetaService.class.getDeclaredField("imageDir");
        imageDirField.setAccessible(true);
        imageDirField.set(service, imageDir);
        return service;
    }

    private HttpServletRequest newRequest(String remoteAddr, String token) {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeader(Env.CLIENT_NODE_HOST_KEY)).thenReturn(FE_HOST);
        Mockito.when(request.getHeader(Env.CLIENT_NODE_PORT_KEY)).thenReturn(Integer.toString(FE_EDIT_LOG_PORT));
        Mockito.when(request.getHeader(MetaBaseAction.TOKEN)).thenReturn(token);
        Mockito.when(request.getRemoteAddr()).thenReturn(remoteAddr);
        Mockito.when(request.getRemoteHost()).thenReturn(remoteAddr);
        Mockito.when(request.getParameter("host")).thenReturn(FE_HOST);
        Mockito.when(request.getParameter("port")).thenReturn(Integer.toString(FE_EDIT_LOG_PORT));
        return request;
    }

    private int responseCode(Object result) {
        ResponseEntity<?> responseEntity = (ResponseEntity<?>) result;
        ResponseBody<?> body = (ResponseBody<?>) responseEntity.getBody();
        return body.getCode();
    }
}
