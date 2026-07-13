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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.httpv2.controller.BaseController;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.httpv2.rest.RestApiStatusCode;
import org.apache.doris.master.MetaHelper;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
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
    // Value configured via Config.fe_meta_auth_token -- the cluster meta auth token.
    private static final String META_TOKEN = "meta-auth-token";
    // Token persisted in the local Storage, returned by the /check endpoint. Intentionally
    // different from META_TOKEN to show the two are independent.
    private static final String STORAGE_TOKEN = "storage-token";
    private static final String BAD_TOKEN = "bad-token";
    private static final String FE_HOST = "127.0.0.1";
    private static final int FE_EDIT_LOG_PORT = 9010;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private String oldMetaAuthToken;
    private boolean oldEnableAllHttpAuth;
    private int oldHttpPort;
    private int oldHttpsPort;
    private boolean oldEnableHttps;
    private Env env;
    private MockedStatic<Env> envStatic;

    @Before
    public void setUp() {
        oldMetaAuthToken = Config.fe_meta_auth_token;
        oldEnableAllHttpAuth = Config.enable_all_http_auth;
        oldHttpPort = Config.http_port;
        oldHttpsPort = Config.https_port;
        oldEnableHttps = Config.enable_https;
        // Token required by default; individual tests clear it to exercise the legacy path.
        Config.fe_meta_auth_token = META_TOKEN;
        Config.enable_all_http_auth = false;
        Config.http_port = 8030;
        Config.https_port = 8050;
        Config.enable_https = false;

        env = Mockito.mock(Env.class);
        envStatic = Mockito.mockStatic(Env.class);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);

        Frontend frontend = new Frontend(FrontendNodeType.FOLLOWER, "fe1", FE_HOST, FE_EDIT_LOG_PORT);
        Mockito.when(env.checkFeExist(FE_HOST, FE_EDIT_LOG_PORT)).thenReturn(frontend);
        Mockito.when(env.getImageDir()).thenReturn(temporaryFolder.getRoot().getAbsolutePath());
    }

    @After
    public void tearDown() {
        Config.fe_meta_auth_token = oldMetaAuthToken;
        Config.enable_all_http_auth = oldEnableAllHttpAuth;
        Config.http_port = oldHttpPort;
        Config.https_port = oldHttpsPort;
        Config.enable_https = oldEnableHttps;
        if (envStatic != null) {
            envStatic.close();
        }
    }

    // A matching token passes even when the request originates from a proxy address
    // (remoteAddr differs from the registered FE host).
    @Test
    public void testMatchingTokenAllowsMetaRequestFromProxyAddress() throws Exception {
        MetaService service = new MetaService();
        HttpServletRequest request = newRequest("192.0.2.10", FE_HOST, META_TOKEN);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Object result = service.role(request, response);

        Assert.assertEquals(RestApiStatusCode.OK.code, responseCode(result));
        Mockito.verify(response).setHeader("role", FrontendNodeType.FOLLOWER.name());
    }

    // When no token is configured, the node-host check alone is sufficient. This keeps
    // existing clusters and rolling upgrades working (a peer that sends no token is allowed).
    @Test
    public void testNoTokenRequiredWhenTokenNotConfigured() throws Exception {
        Config.fe_meta_auth_token = "";
        MetaService service = new MetaService();
        HttpServletRequest request = newRequest("192.0.2.10", FE_HOST, null);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Object result = service.role(request, response);

        Assert.assertEquals(RestApiStatusCode.OK.code, responseCode(result));
        Mockito.verify(response).setHeader("role", FrontendNodeType.FOLLOWER.name());
    }

    // /check returns the local Storage token once the request passes authentication.
    @Test
    public void testCheckReturnsStorageTokenWhenAuthPasses() throws Exception {
        Config.fe_meta_auth_token = "";
        MetaService service = serviceWithImageDir();
        HttpServletRequest request = newRequest("192.0.2.10", FE_HOST, null);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Object result = service.check(request, response);

        Assert.assertEquals(RestApiStatusCode.OK.code, responseCode(result));
        Mockito.verify(response).setHeader(MetaBaseAction.TOKEN, STORAGE_TOKEN);
    }

    // A wrong token is rejected even though the node-host check would pass.
    @Test
    public void testWrongTokenRejected() {
        MetaService service = new MetaService();
        HttpServletRequest request = newRequest(FE_HOST, FE_HOST, BAD_TOKEN);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Assert.assertThrows(UnauthorizedException.class, () -> service.role(request, response));
    }

    // A missing token is rejected when a cluster token is configured.
    @Test
    public void testMissingTokenRejectedWhenConfigured() {
        MetaService service = new MetaService();
        HttpServletRequest request = newRequest(FE_HOST, FE_HOST, null);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Assert.assertThrows(UnauthorizedException.class, () -> service.role(request, response));
    }

    // The node-host check is always enforced: an unknown FE host is rejected even with a
    // matching token (the token check is additive, it does not replace the host check).
    @Test
    public void testUnknownHostRejectedEvenWithValidToken() {
        MetaService service = new MetaService();
        HttpServletRequest request = newRequest("192.0.2.99", "192.0.2.99", META_TOKEN);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Assert.assertThrows(UnauthorizedException.class, () -> service.role(request, response));
    }

    @Test
    public void testPutRejectsUnexpectedHttpPort() throws Exception {
        MetaService service = new MetaService();
        HttpServletRequest request = newRequest(FE_HOST, FE_HOST, META_TOKEN);
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

    // When HTTPS is enabled the master pushes image using https_port, so /put must accept
    // https_port. The expected port follows HttpURLUtil.getHttpPort(), not a hard-coded http_port.
    @Test
    public void testPutAcceptsHttpsPortWhenHttpsEnabled() throws Exception {
        Config.enable_https = true;
        MetaService service = new MetaService();
        HttpServletRequest request = newRequest(FE_HOST, FE_HOST, META_TOKEN);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getParameter("version")).thenReturn("100");
        Mockito.when(request.getParameter("port")).thenReturn(Integer.toString(Config.https_port));

        try (MockedStatic<MetaHelper> metaHelper = Mockito.mockStatic(MetaHelper.class)) {
            File partialFile = temporaryFolder.newFile("image.100.part");
            metaHelper.when(() -> MetaHelper.getFile(Mockito.anyString(), Mockito.any(File.class)))
                    .thenReturn(partialFile);

            Object result = service.put(request, response);

            // Passes the port check and proceeds to fetch the remote image (no BAD_REQUEST).
            Assert.assertEquals(RestApiStatusCode.OK.code, responseCode(result));
            metaHelper.verify(() -> MetaHelper.getRemoteFile(Mockito.anyString(), Mockito.anyInt(),
                    Mockito.any(File.class)), Mockito.times(1));
        }
    }

    // /dump authenticates and then requires ADMIN. An authenticated admin passes the privilege
    // check and proceeds to dump.
    @Test
    public void testDumpAllowsAdmin() throws Exception {
        MetaService service = Mockito.spy(new MetaService());
        HttpServletRequest request = newRequest(FE_HOST, FE_HOST, META_TOKEN);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.doReturn(authInfo(UserIdentity.ADMIN)).when(service).executeCheckPassword(request, response);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(accessManager.checkGlobalPriv(UserIdentity.ADMIN, PrivPredicate.ADMIN)).thenReturn(true);
        Mockito.when(env.dumpImage()).thenReturn(null);

        service.dump(request, response);

        Mockito.verify(service).executeCheckPassword(request, response);
        Mockito.verify(env).dumpImage();
    }

    // An authenticated but non-admin user is rejected before any dump happens.
    @Test
    public void testDumpRejectsNonAdmin() throws Exception {
        MetaService service = Mockito.spy(new MetaService());
        HttpServletRequest request = newRequest(FE_HOST, FE_HOST, META_TOKEN);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        UserIdentity nonAdmin = UserIdentity.createAnalyzedUserIdentWithIp("user", "%");
        Mockito.doReturn(authInfo(nonAdmin)).when(service).executeCheckPassword(request, response);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(accessManager.checkGlobalPriv(nonAdmin, PrivPredicate.ADMIN)).thenReturn(false);

        Assert.assertThrows(UnauthorizedException.class, () -> service.dump(request, response));
        Mockito.verify(env, Mockito.never()).dumpImage();
    }

    private static BaseController.ActionAuthorizationInfo authInfo(UserIdentity userIdentity) {
        BaseController.ActionAuthorizationInfo info = new BaseController.ActionAuthorizationInfo();
        info.userIdentity = userIdentity;
        return info;
    }

    private MetaService serviceWithImageDir() throws Exception {
        File imageDir = temporaryFolder.newFolder("image");
        Storage storage = new Storage(12345, STORAGE_TOKEN, imageDir.getAbsolutePath());
        storage.writeClusterIdAndToken();

        MetaService service = new MetaService();
        Field imageDirField = MetaService.class.getDeclaredField("imageDir");
        imageDirField.setAccessible(true);
        imageDirField.set(service, imageDir);
        return service;
    }

    private HttpServletRequest newRequest(String remoteAddr, String clientHost, String token) {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeader(Env.CLIENT_NODE_HOST_KEY)).thenReturn(clientHost);
        Mockito.when(request.getHeader(Env.CLIENT_NODE_PORT_KEY)).thenReturn(Integer.toString(FE_EDIT_LOG_PORT));
        Mockito.when(request.getHeader(MetaBaseAction.TOKEN)).thenReturn(token);
        Mockito.when(request.getRemoteAddr()).thenReturn(remoteAddr);
        Mockito.when(request.getRemoteHost()).thenReturn(remoteAddr);
        Mockito.when(request.getParameter("host")).thenReturn(clientHost);
        Mockito.when(request.getParameter("port")).thenReturn(Integer.toString(FE_EDIT_LOG_PORT));
        return request;
    }

    private int responseCode(Object result) {
        ResponseEntity<?> responseEntity = (ResponseEntity<?>) result;
        ResponseBody<?> body = (ResponseBody<?>) responseEntity.getBody();
        return body.getCode();
    }
}
