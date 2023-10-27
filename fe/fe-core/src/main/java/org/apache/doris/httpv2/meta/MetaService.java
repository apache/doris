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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.master.MetaHelper;
import org.apache.doris.persist.MetaCleaner;
import org.apache.doris.persist.Storage;
import org.apache.doris.persist.StorageInfo;
import org.apache.doris.system.Frontend;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class MetaService extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(MetaService.class);

    private static final int TIMEOUT_SECOND = 10;

    private static final String VERSION = "version";
    private static final String HOST = "host";
    private static final String PORT = "port";

    private File imageDir = MetaHelper.getMasterImageDir();

    private boolean isFromValidFe(HttpServletRequest request) {
        String clientHost = request.getHeader(Env.CLIENT_NODE_HOST_KEY);
        String clientPortStr = request.getHeader(Env.CLIENT_NODE_PORT_KEY);
        Integer clientPort;
        try {
            clientPort = Integer.valueOf(clientPortStr);
        } catch (Exception e) {
            LOG.warn("get clientPort error. clientPortStr: {}", clientPortStr, e.getMessage());
            return false;
        }

        Frontend fe = Env.getCurrentEnv().checkFeExist(clientHost, clientPort);
        if (fe == null) {
            LOG.warn("request is not from valid FE. client: {}, {}", clientHost, clientPortStr);
            return false;
        }
        return true;
    }


    private void checkFromValidFe(HttpServletRequest request)
            throws InvalidClientException {
        if (!isFromValidFe(request)) {
            throw new InvalidClientException("invalid client host: " + request.getRemoteHost());
        }
    }

    @RequestMapping(path = "/image", method = RequestMethod.GET)
    public Object image(HttpServletRequest request, HttpServletResponse response) {
        checkFromValidFe(request);

        String versionStr = request.getParameter(VERSION);

        if (Strings.isNullOrEmpty(versionStr)) {
            return ResponseEntityBuilder.badRequest("Miss version parameter");
        }

        long version = checkLongParam(versionStr);
        if (version < 0) {
            return ResponseEntityBuilder.badRequest("The version number cannot be less than 0");
        }

        File imageFile = Storage.getImageFile(imageDir, version);
        if (!imageFile.exists()) {
            return ResponseEntityBuilder.notFound("image file not found");
        }

        try {
            writeFileResponse(request, response, imageFile);
            return null;
        } catch (IOException e) {
            return ResponseEntityBuilder.internalError(e.getMessage());
        }
    }

    @RequestMapping(path = "/info", method = RequestMethod.GET)
    public Object info(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        checkFromValidFe(request);

        try {
            Storage currentStorageInfo = new Storage(imageDir.getAbsolutePath());
            StorageInfo storageInfo = new StorageInfo(currentStorageInfo.getClusterID(),
                    currentStorageInfo.getLatestImageSeq(), currentStorageInfo.getEditsSeq());
            return ResponseEntityBuilder.ok(storageInfo);
        } catch (IOException e) {
            return ResponseEntityBuilder.internalError(e.getMessage());
        }
    }

    @RequestMapping(path = "/version", method = RequestMethod.GET)
    public void version(HttpServletRequest request, HttpServletResponse response) throws IOException, DdlException {
        checkFromValidFe(request);
        File versionFile = new File(imageDir, Storage.VERSION_FILE);
        writeFileResponse(request, response, versionFile);
    }

    @RequestMapping(path = "/put", method = RequestMethod.GET)
    public Object put(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        checkFromValidFe(request);

        String portStr = request.getParameter(PORT);

        // check port to avoid SSRF(Server-Side Request Forgery)
        if (Strings.isNullOrEmpty(portStr)) {
            return ResponseEntityBuilder.badRequest("Port number cannot be empty");
        }

        int port = Integer.parseInt(portStr);
        if (port < 0 || port > 65535) {
            return ResponseEntityBuilder.badRequest("port is invalid. The port number is between 0-65535");
        }

        String versionStr = request.getParameter(VERSION);
        if (Strings.isNullOrEmpty(versionStr)) {
            return ResponseEntityBuilder.badRequest("Miss version parameter");
        }

        checkLongParam(versionStr);

        String machine = request.getRemoteHost();
        String url = "http://" + NetUtils.getHostPortInAccessibleFormat(machine, Integer.valueOf(portStr))
                + "/image?version=" + versionStr;
        String filename = Storage.IMAGE + "." + versionStr;
        File dir = new File(Env.getCurrentEnv().getImageDir());
        try {
            MetaHelper.getRemoteFile(url, Config.sync_image_timeout_second * 1000, MetaHelper.getFile(filename, dir));
            MetaHelper.complete(filename, dir);
        } catch (FileNotFoundException e) {
            return ResponseEntityBuilder.notFound("file not found.");
        } catch (IOException e) {
            LOG.warn("failed to get remote file. url: {}", url, e);
            return ResponseEntityBuilder.internalError("failed to get remote file: " + e.getMessage());
        }

        // Delete old image files
        try {
            MetaCleaner cleaner = new MetaCleaner(Config.meta_dir + "/image");
            cleaner.clean();
        } catch (Exception e) {
            LOG.error("Follower/Observer delete old image file fail.", e);
        }
        return ResponseEntityBuilder.ok();
    }

    @RequestMapping(path = "/journal_id", method = RequestMethod.GET)
    public Object journal_id(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        checkFromValidFe(request);
        long id = Env.getCurrentEnv().getReplayedJournalId();
        response.setHeader("id", Long.toString(id));
        return ResponseEntityBuilder.ok();
    }

    @RequestMapping(path = "/role", method = RequestMethod.GET)
    public Object role(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        checkFromValidFe(request);
        // For upgrade compatibility, the host parameter name remains the same
        // and the new hostname parameter is added.
        // host = ip
        String host = request.getParameter(HOST);
        String portString = request.getParameter(PORT);
        if (!Strings.isNullOrEmpty(host) && !Strings.isNullOrEmpty(portString)) {
            int port = Integer.parseInt(portString);
            Frontend fe = Env.getCurrentEnv().checkFeExist(host, port);
            if (fe == null) {
                response.setHeader("role", FrontendNodeType.UNKNOWN.name());
            } else {
                response.setHeader("role", fe.getRole().name());
                response.setHeader("name", fe.getNodeName());
            }
            return ResponseEntityBuilder.ok();
        } else {
            return ResponseEntityBuilder.badRequest("Miss parameter");
        }
    }

    /*
     * This action is used to get the electable_nodes config and the cluster id of
     * the fe with the given ip and port. When one frontend start, it should check
     * the local electable_nodes config and local cluster id with other frontends.
     * If there is any difference, local fe will exit. This is designed to protect
     * the consistency of the cluster.
     */
    @RequestMapping(path = "/check", method = RequestMethod.GET)
    public Object check(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        checkFromValidFe(request);

        try {
            Storage storage = new Storage(imageDir.getAbsolutePath());
            response.setHeader(MetaBaseAction.CLUSTER_ID, Integer.toString(storage.getClusterID()));
            response.setHeader(MetaBaseAction.TOKEN, storage.getToken());
        } catch (IOException e) {
            return ResponseEntityBuilder.internalError(e.getMessage());
        }
        return ResponseEntityBuilder.ok();
    }

    @RequestMapping(value = "/dump", method = RequestMethod.GET)
    public Object dump(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        if (Config.enable_all_http_auth) {
            executeCheckPassword(request, response);
        }

        /*
         * Before dump, we acquired the catalog read lock and all databases' read lock and all
         * the jobs' read lock. This will guarantee the consistency of database and job queues.
         * But Backend may still inconsistent.
         *
         * TODO: Still need to lock ClusterInfoService to prevent add or drop Backends
         */
        String dumpFilePath = Env.getCurrentEnv().dumpImage();

        if (dumpFilePath == null) {
            return ResponseEntityBuilder.okWithCommonError("dump failed.");
        }
        Map<String, String> res = Maps.newHashMap();
        res.put("dumpFilePath", dumpFilePath);
        return ResponseEntityBuilder.ok(res);
    }

}
