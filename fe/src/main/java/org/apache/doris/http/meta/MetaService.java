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

package org.apache.doris.http.meta;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.http.rest.RestBaseController;
import org.apache.doris.master.MetaHelper;
import org.apache.doris.persist.MetaCleaner;
import org.apache.doris.persist.Storage;
import org.apache.doris.persist.StorageInfo;
import org.apache.doris.system.Frontend;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class MetaService extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(MetaService.class);

    private static final int TIMEOUT_SECOND = 10;

    private static final String VERSION = "version";
    private static final String PORT = "port";

    private File imageDir = MetaHelper.getMasterImageDir();

    public Object executeCheck(HttpServletRequest request, HttpServletResponse response,boolean needCheckClientIsFe) {
        if (needCheckClientIsFe) {
            try {
                checkFromValidFe(request, response);
                return null;
            } catch (InvalidClientException e) {
                ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build();
                entity.setCode(HttpStatus.BAD_REQUEST.value());
                entity.setMsg("invalid client host.");
                return entity;
            }
        }
        return null;
    }
    private boolean isFromValidFe(HttpServletRequest request) {
        String clientHost = request.getRemoteHost();
        Frontend fe = Catalog.getCurrentCatalog().getFeByHost(clientHost);
        if (fe == null) {
            LOG.warn("request is not from valid FE. client: {}", clientHost);
            return false;
        }
        return true;
    }

    private void checkFromValidFe(HttpServletRequest request, HttpServletResponse response)
            throws InvalidClientException {
        if (!isFromValidFe(request)) {
            throw new InvalidClientException("invalid client host");
        }
    }

    @RequestMapping(path = "/image", method = RequestMethod.GET)
    public Object image(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        executeCheckPassword(request,response);
        Object obj = executeCheck(request,response,true);
        if(obj != null){
            return obj;
        }
        String versionStr = request.getParameter(VERSION);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build();

        if (Strings.isNullOrEmpty(versionStr)) {
            entity.setMsg("Miss version parameter");
            entity.setCode(HttpStatus.BAD_REQUEST.value());
            return entity;
        }

        long version = checkLongParam(versionStr);
        if (version < 0) {
            entity.setMsg("The version number cannot be less than 0");
            entity.setCode(HttpStatus.BAD_REQUEST.value());
            return entity;
        }

        File imageFile = Storage.getImageFile(imageDir, version);
        if (!imageFile.exists()) {
            entity.setMsg("The version number cannot be less than 0");
            entity.setCode(HttpStatus.NOT_FOUND.value());
            return entity;
        }

        if (writeFileResponse(request, response, imageFile)) {
            entity.setMsg("success");
            return entity;
        } else {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            return entity;
        }
    }


    @RequestMapping(path = "/info", method = RequestMethod.GET)
    public Object info(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        executeCheckPassword(request,response);
        Object obj = executeCheck(request,response,true);
        if(obj != null){
            return obj;
        }
        try {
            Storage currentStorageInfo = new Storage(imageDir.getAbsolutePath());
            StorageInfo storageInfo = new StorageInfo(currentStorageInfo.getClusterID(),
                    currentStorageInfo.getImageSeq(), currentStorageInfo.getEditsSeq());
            ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build(storageInfo);

            return entity;
        } catch (IOException e) {
            ResponseEntity entity = ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
            LOG.warn("IO error.", e);
            entity.setMsg("failed to get master info.");
            return entity;
        }
    }

    @RequestMapping(path = "/version", method = RequestMethod.GET)
    public void version(HttpServletRequest request, HttpServletResponse response) throws IOException, DdlException {
        executeCheckPassword(request,response);
        Object obj = executeCheck(request,response,true);
        if(obj != null){
            response.setStatus(HttpStatus.BAD_REQUEST.value());
            response.getWriter().write("invalid client host");
        }
        File versionFile = new File(imageDir, Storage.VERSION_FILE);
        writeFileResponse(request, response, versionFile);
    }


    @RequestMapping(path = "/put", method = RequestMethod.GET)
    public Object put(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        executeCheckPassword(request,response);
        String machine = request.getRemoteHost();
        String portStr = request.getParameter(PORT);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build();
        Object obj = executeCheck(request,response,true);
        if(obj != null){
            return obj;
        }
        // check port to avoid SSRF(Server-Side Request Forgery)
        if (Strings.isNullOrEmpty(portStr)) {
            entity.setMsg("Port number cannot be empty");
            entity.setCode(HttpStatus.BAD_REQUEST.value());
            return entity;
        }
        {
            int port = Integer.parseInt(portStr);
            if (port < 0 || port > 65535) {
                LOG.warn("port is invalid. port={}", port);
                entity.setMsg("port is invalid. The port number is between 0-65535");
                entity.setCode(HttpStatus.BAD_REQUEST.value());
                return entity;
            }
        }

        String versionStr = request.getParameter(VERSION);
        if (Strings.isNullOrEmpty(versionStr)) {
            entity.setMsg("Miss version parameter");
            entity.setCode(HttpStatus.BAD_REQUEST.value());
            return entity;
        }
        checkLongParam(versionStr);

        String url = "http://" + machine + ":" + portStr + "/image?version=" + versionStr;
        String filename = Storage.IMAGE + "." + versionStr;
        File dir = new File(Catalog.getCurrentCatalog().getImageDir());
        try {
            OutputStream out = MetaHelper.getOutputStream(filename, dir);
            MetaHelper.getRemoteFile(url, TIMEOUT_SECOND * 1000, out);
            MetaHelper.complete(filename, dir);
//                writeResponse(request, response);
        } catch (FileNotFoundException e) {
            LOG.warn("file not found. file: {}", filename, e);
            entity.setMsg("file not found.");
            entity.setCode(HttpStatus.NOT_FOUND.value());
            return entity;
        } catch (IOException e) {
            LOG.warn("failed to get remote file. url: {}", url, e);
            entity.setMsg("failed to get remote file.");
            entity.setCode(HttpStatus.INTERNAL_SERVER_ERROR.value());
            return entity;
        }
        // Delete old image files
        try {
            MetaCleaner cleaner = new MetaCleaner(Config.meta_dir + "/image");
            cleaner.clean();
        } catch (Exception e) {
            LOG.error("Follower/Observer delete old image file fail.", e);
        }
        return entity;
    }

    @RequestMapping(path = "/journal_id", method = RequestMethod.GET)
    public Object journal_id(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        executeCheckPassword(request,response);
        Object obj = executeCheck(request,response,true);
        if(obj != null){
            return obj;
        }
        long id = Catalog.getCurrentCatalog().getReplayedJournalId();
        response.setHeader("id", Long.toString(id));
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build();
        return entity;
    }


    private static final String HOST = "host";


    @RequestMapping(path = "/role", method = RequestMethod.GET)
    public Object role(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        executeCheckPassword(request,response);
        String host = request.getParameter(HOST);
        String portString = request.getParameter(PORT);
        Object obj = executeCheck(request,response,true);
        if(obj != null){
            return obj;
        }
        if (!Strings.isNullOrEmpty(host) && !Strings.isNullOrEmpty(portString)) {
            int port = Integer.parseInt(portString);
            Frontend fe = Catalog.getCurrentCatalog().checkFeExist(host, port);
            if (fe == null) {
                response.setHeader("role", FrontendNodeType.UNKNOWN.name());
            } else {
                response.setHeader("role", fe.getRole().name());
                response.setHeader("name", fe.getNodeName());
            }
            //writeResponse(request, response);
            ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build();
            return entity;
        } else {
            ResponseEntity entity = ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
            entity.setMsg("Miss parameter");
            return entity;
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
        executeCheckPassword(request,response);
        Object obj = executeCheck(request,response,true);
        if(obj != null){
            return obj;
        }
        try {
            Storage storage = new Storage(imageDir.getAbsolutePath());
            response.setHeader(MetaBaseAction.CLUSTER_ID, Integer.toString(storage.getClusterID()));
            response.setHeader(MetaBaseAction.TOKEN, storage.getToken());
        } catch (IOException e) {
            LOG.error(e);
        }
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build();
        return entity;
    }

    @RequestMapping(value = "/dump", method = RequestMethod.GET)
    public Object dump(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        /*
         * Before dump, we acquired the catalog read lock and all databases' read lock and all
         * the jobs' read lock. This will guarantee the consistency of database and job queues.
         * But Backend may still inconsistent.
         */
        executeCheckPassword(request,response);
        Object obj = executeCheck(request,response,false);
        if(obj != null){
            return obj;
        }
        //needAdmin=true
        executeCheckPassword(request, response);
        // TODO: Still need to lock ClusterInfoService to prevent add or drop Backends
        String dumpFilePath = Catalog.getCurrentCatalog().dumpImage();
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build();

        if (dumpFilePath == null) {
            entity.setMsg("dump failed. " + dumpFilePath);
            entity.setCode(HttpStatus.NOT_FOUND.value());
            return entity;
        }
        entity.setMsg("dump finished. " + dumpFilePath);
        //writeResponse(request, response);
        return entity;
    }

}
