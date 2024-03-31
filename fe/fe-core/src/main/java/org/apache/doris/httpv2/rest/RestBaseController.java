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

package org.apache.doris.httpv2.rest;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.httpv2.controller.BaseController;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.master.MetaHelper;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.view.RedirectView;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class RestBaseController extends BaseController {

    protected static final String NS_KEY = "ns";
    protected static final String DB_KEY = "db";
    protected static final String TABLE_KEY = "table";
    protected static final String LABEL_KEY = "label";
    protected static final String TXN_ID_KEY = "txn_id";
    protected static final String TXN_OPERATION_KEY = "txn_operation";
    protected static final String SINGLE_REPLICA_KEY = "single_replica";
    private static final Logger LOG = LogManager.getLogger(RestBaseController.class);

    public ActionAuthorizationInfo executeCheckPassword(HttpServletRequest request,
                                                        HttpServletResponse response) throws UnauthorizedException {
        ActionAuthorizationInfo authInfo = getAuthorizationInfo(request);
        // check password
        UserIdentity currentUser = checkPassword(authInfo);
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setQualifiedUser(authInfo.fullUserName);
        ctx.setRemoteIP(authInfo.remoteIp);
        ctx.setCurrentUserIdentity(currentUser);
        ctx.setThreadLocalInfo();
        return authInfo;
    }

    public RedirectView redirectTo(HttpServletRequest request, TNetworkAddress addr) {
        URI urlObj = null;
        URI resultUriObj = null;
        String urlStr = request.getRequestURI();
        String userInfo = null;
        if (!Strings.isNullOrEmpty(request.getHeader("Authorization"))) {
            ActionAuthorizationInfo authInfo = getAuthorizationInfo(request);
            userInfo = ClusterNamespace.getNameFromFullName(authInfo.fullUserName)
                    + ":" + authInfo.password;
        }
        try {
            urlObj = new URI(urlStr);
            resultUriObj = new URI("http", userInfo, addr.getHostname(),
                    addr.getPort(), urlObj.getPath(), "", null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String redirectUrl = resultUriObj.toASCIIString();
        if (!Strings.isNullOrEmpty(request.getQueryString())) {
            redirectUrl += request.getQueryString();
        }
        LOG.info("Redirect url: {}", "http://" + addr.getHostname() + ":"
                    + addr.getPort() + urlObj.getPath());
        RedirectView redirectView = new RedirectView(redirectUrl);
        redirectView.setContentType("text/html;charset=utf-8");
        redirectView.setStatusCode(org.springframework.http.HttpStatus.TEMPORARY_REDIRECT);
        return redirectView;
    }

    public RedirectView redirectToObj(String sign) throws URISyntaxException {
        RedirectView redirectView = new RedirectView(sign);
        redirectView.setContentType("text/html;charset=utf-8");
        redirectView.setStatusCode(org.springframework.http.HttpStatus.TEMPORARY_REDIRECT);
        return redirectView;
    }

    public RedirectView redirectToMasterOrException(HttpServletRequest request, HttpServletResponse response)
                    throws Exception {
        Env env = Env.getCurrentEnv();
        if (env.isMaster()) {
            return null;
        }
        env.checkReadyOrThrow();
        return redirectTo(request, new TNetworkAddress(env.getMasterHost(), env.getMasterHttpPort()));
    }

    public Object redirectToMaster(HttpServletRequest request, HttpServletResponse response) {
        try {
            return redirectToMasterOrException(request, response);
        } catch (Exception e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }

    public void getFile(HttpServletRequest request, HttpServletResponse response, Object obj, String fileName)
            throws IOException {
        response.setHeader("Content-type", "application/octet-stream");
        response.addHeader("Content-Disposition", "attachment;fileName=" + fileName); // set file name
        if (obj instanceof File) {
            File file = (File) obj;
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
                return;
            } finally {
                if (bis != null) {
                    try {
                        bis.close();
                    } catch (IOException e) {
                        LOG.warn("", e);
                    }
                }
                if (fis != null) {
                    try {
                        fis.close();
                    } catch (IOException e) {
                        LOG.warn("", e);
                    }
                }
            }
        } else if (obj instanceof byte[]) {
            OutputStream os = response.getOutputStream();
            os.write((byte[]) obj);
        }
    }

    public void writeFileResponse(HttpServletRequest request,
            HttpServletResponse response, File imageFile) throws IOException {
        Preconditions.checkArgument(imageFile != null && imageFile.exists());
        response.setHeader("Content-type", "application/octet-stream");
        response.addHeader("Content-Disposition", "attachment;fileName=" + imageFile.getName());
        response.setHeader(MetaHelper.X_IMAGE_SIZE, imageFile.length() + "");
        response.setHeader(MetaHelper.X_IMAGE_MD5, DigestUtils.md5Hex(new FileInputStream(imageFile)));
        getFile(request, response, imageFile, imageFile.getName());
    }

    public String getFullDbName(String dbName) {
        return ClusterNamespace.getNameFromFullName(dbName);
    }

    public boolean needRedirect(String scheme) {
        return Config.enable_https && "http".equalsIgnoreCase(scheme);
    }

    public Object redirectToHttps(HttpServletRequest request) {
        String serverName = request.getServerName();
        String uri = request.getRequestURI();
        String query = request.getQueryString();
        query = query == null ? "" : query;
        String newUrl = "https://" + NetUtils.getHostPortInAccessibleFormat(serverName, Config.https_port) + uri + "?"
                + query;
        LOG.info("redirect to new url: {}", newUrl);
        RedirectView redirectView = new RedirectView(newUrl);
        redirectView.setStatusCode(HttpStatus.TEMPORARY_REDIRECT);
        return redirectView;
    }
}
