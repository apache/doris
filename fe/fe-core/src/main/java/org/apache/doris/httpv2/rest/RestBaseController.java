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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.httpv2.controller.BaseController;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.servlet.view.RedirectView;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class RestBaseController extends BaseController {

    protected static final String NS_KEY = "ns";
    protected static final String DB_KEY = "db";
    protected static final String TABLE_KEY = "table";
    protected static final String LABEL_KEY = "label";
    private static final Logger LOG = LogManager.getLogger(RestBaseController.class);

    public ActionAuthorizationInfo executeCheckPassword(HttpServletRequest request,
                                                        HttpServletResponse response) throws UnauthorizedException {
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
        return authInfo;
    }


    public RedirectView redirectTo(HttpServletRequest request, TNetworkAddress addr) {
        URI urlObj = null;
        URI resultUriObj = null;
        String urlStr = request.getRequestURI();
        try {
            urlObj = new URI(urlStr);
            resultUriObj = new URI("http", null, addr.getHostname(),
                    addr.getPort(), urlObj.getPath(), "", null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String redirectUrl = resultUriObj.toASCIIString();
        if (!Strings.isNullOrEmpty(request.getQueryString())) {
            redirectUrl += request.getQueryString();
        }
        LOG.info("redirect url: {}", redirectUrl);
        RedirectView redirectView = new RedirectView(redirectUrl);
        redirectView.setContentType("text/html;charset=utf-8");
        redirectView.setStatusCode(org.springframework.http.HttpStatus.TEMPORARY_REDIRECT);
        return redirectView;
    }

    public RedirectView redirectToMaster(HttpServletRequest request, HttpServletResponse response) {
        Catalog catalog = Catalog.getCurrentCatalog();
        if (catalog.isMaster()) {
            return null;
        }
        RedirectView redirectView = redirectTo(request, new TNetworkAddress(catalog.getMasterIp(), catalog.getMasterHttpPort()));
        return redirectView;
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
        } else if (obj instanceof byte[]) {
            OutputStream os = response.getOutputStream();
            os.write((byte[]) obj);
        }
    }

    public void writeFileResponse(HttpServletRequest request, HttpServletResponse response, File imageFile) throws IOException {
        Preconditions.checkArgument(imageFile != null && imageFile.exists());
        response.setHeader("Content-type", "application/octet-stream");
        response.addHeader("Content-Disposition", "attachment;fileName=" + imageFile.getName());
        response.setHeader("X-Image-Size", imageFile.length() + "");
        getFile(request, response, imageFile, imageFile.getName());
    }

    public String getFullDbName(String dbName) {
        String fullDbName = dbName;
        String clusterName = ClusterNamespace.getClusterNameFromFullName(fullDbName);
        if (clusterName == null) {
            fullDbName = ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, dbName);
        }
        return fullDbName;
    }
}
