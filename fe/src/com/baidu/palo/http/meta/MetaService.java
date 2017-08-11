// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.http.meta;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.common.Config;
import com.baidu.palo.ha.FrontendNodeType;
import com.baidu.palo.http.ActionController;
import com.baidu.palo.http.BaseRequest;
import com.baidu.palo.http.BaseResponse;
import com.baidu.palo.http.IllegalArgException;
import com.baidu.palo.master.MetaHelper;
import com.baidu.palo.persist.MetaCleaner;
import com.baidu.palo.persist.Storage;
import com.baidu.palo.persist.StorageInfo;
import com.baidu.palo.system.Frontend;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class MetaService {
    private static final int TIMEOUT_SECOND = 10;

    public static class ImageAction extends MetaBaseAction {
        private static final String VERSION = "version";

        public ImageAction(ActionController controller, File imageDir) {
            super(controller, imageDir);
        }

        public static void registerAction(ActionController controller, File imageDir)
                throws IllegalArgException {
            controller.registerHandler(HttpMethod.GET, "/image", new ImageAction(controller, imageDir));
        }

        @Override
        public void executeGet(BaseRequest request, BaseResponse response) {
            String strVersion = request.getSingleParameter(VERSION);

            if (!Strings.isNullOrEmpty(strVersion)) {
                long version = Long.parseLong(strVersion);
                File imageFile = Storage.getImageFile(imageDir, version);
                writeFileResponse(request, response, imageFile);
            } else {
                response.appendContent("Miss version parameter");
                writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                return;
            }
        }
    }

    public static class EditsAction extends MetaBaseAction {
        private static final String SEQ = "seq";

        public EditsAction(ActionController controller, File imageDir) {
            super(controller, imageDir);
        }

        public static void registerAction(ActionController controller, File imageDir)
                throws IllegalArgException {
            controller.registerHandler(HttpMethod.GET, "/edits", new EditsAction(controller, imageDir));
        }

        @Override
        public void executeGet(BaseRequest request, BaseResponse response) {
            String strSeq = request.getSingleParameter(SEQ);
            File edits = null;

            if (Strings.isNullOrEmpty(strSeq)) {
                long seq = Long.parseLong(strSeq);
                edits = Storage.getEditsFile(imageDir, seq);
            } else {
                edits = Storage.getCurrentEditsFile(imageDir);
            }

            writeFileResponse(request, response, edits);
        }
    }

    public static class InfoAction extends MetaBaseAction {
        private static final Logger LOG = LogManager.getLogger(InfoAction.class);

        public InfoAction(ActionController controller, File imageDir) {
            super(controller, imageDir);
        }

        public static void registerAction (ActionController controller, File imageDir)
                throws IllegalArgException {
            controller.registerHandler(HttpMethod.GET, "/info", new InfoAction(controller, imageDir));
        }

        @Override
        public void executeGet(BaseRequest request, BaseResponse response) {
            try {
                Storage currentStorageInfo = new Storage(imageDir.getAbsolutePath());
                StorageInfo storageInfo = new StorageInfo(currentStorageInfo.getClusterID(),
                        currentStorageInfo.getImageSeq(), currentStorageInfo.getEditsSeq());

                response.addHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json");
                Gson gson = new Gson();
                response.appendContent(gson.toJson(storageInfo));
                writeResponse(request, response);
            } catch (IOException e) {
                LOG.warn("IO error.", e);
                response.appendContent("Miss version parameter");
                writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                return;
            }
        }
    }

    public static class VersionAction extends MetaBaseAction {

        public VersionAction(ActionController controller, File imageDir) {
            super(controller, imageDir);
        }

        public static void registerAction (ActionController controller, File imageDir)
                throws IllegalArgException {
            controller.registerHandler(HttpMethod.GET, "/version", new VersionAction(controller, imageDir));
        }

        @Override
        public void executeGet(BaseRequest request, BaseResponse response) {
            File versionFile = new File(imageDir, Storage.VERSION_FILE);
            writeFileResponse(request, response, versionFile);
        }
    }

    public static class PutAction extends MetaBaseAction {
        private static final Logger LOG = LogManager.getLogger(PutAction.class);

        private static final String VERSION = "version";
        private static final String PORT = "port";

        public PutAction(ActionController controller, File imageDir) {
            super(controller, imageDir);
        }

        public static void registerAction (ActionController controller, File imageDir)
                throws IllegalArgException {
            controller.registerHandler(HttpMethod.GET, "/put", new PutAction(controller, imageDir));
        }

        @Override
        public void executeGet(BaseRequest request, BaseResponse response) {
            File dir = new File(Catalog.IMAGE_DIR);
            String strVersion = request.getSingleParameter(VERSION);

            if (!Strings.isNullOrEmpty(strVersion)) {
                long version = Long.parseLong(strVersion);
                String machine = request.getHostString();
                String port = request.getSingleParameter(PORT);
                String url = "http://" + machine + ":" + port + "/image?version=" + version;
                String filename = Storage.IMAGE + "." + version;
                try {
                    OutputStream out = MetaHelper.getOutputStream(filename, dir);
                    MetaHelper.getRemoteFile(url, TIMEOUT_SECOND * 1000, out);
                    MetaHelper.complete(filename, dir);
                    writeResponse(request, response);
                } catch (FileNotFoundException e) {
                    LOG.warn("file not found. file: {}", filename, e);
                    writeResponse(request, response, HttpResponseStatus.NOT_FOUND);
                    return;
                } catch (IOException e) {
                    LOG.warn("failed to get remote file. url: {}", url, e);
                    writeResponse(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                    return;
                }

                // Delete old image files
                MetaCleaner cleaner = new MetaCleaner(Config.meta_dir + "/image");
                try {
                    cleaner.clean();
                } catch (IOException e) {
                    LOG.error("Follower/Observer delete old image file fail.", e);
                }
            } else {
                response.appendContent("Miss version parameter");
                writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                return;
            }
        }
    }

    public static class JournalIdAction extends MetaBaseAction {
        public JournalIdAction(ActionController controller, File imageDir) {
            super(controller, imageDir);
        }

        public static void registerAction (ActionController controller, File imageDir)
                throws IllegalArgException {
            controller.registerHandler(HttpMethod.GET, "/journal_id", new JournalIdAction(controller, imageDir));
        }

        @Override
        public void executeGet(BaseRequest request, BaseResponse response) {
            long id = Catalog.getInstance().getReplayedJournalId();
            response.addHeader("id", Long.toString(id));
            writeResponse(request, response);
        }
    }
    
    public static class RoleAction extends MetaBaseAction {
        private static final String HOST = "host";
        private static final String PORT = "port";
        
        public RoleAction(ActionController controller, File imageDir) {
            super(controller, imageDir);
        }

        public static void registerAction (ActionController controller, File imageDir)
                throws IllegalArgException {
            controller.registerHandler(HttpMethod.GET, "/role", new RoleAction(controller, imageDir));
        }

        @Override
        public void executeGet(BaseRequest request, BaseResponse response) {
            String host = request.getSingleParameter(HOST);
            String portString = request.getSingleParameter(PORT);

            if (!Strings.isNullOrEmpty(host) && !Strings.isNullOrEmpty(portString)) {
                int port = Integer.parseInt(portString);
                Frontend fe = Catalog.getInstance().checkFeExist(host, port);
                if (fe == null) {
                    response.addHeader("role", FrontendNodeType.UNKNOWN.name());
                } else {
                    response.addHeader("role", fe.getRole().name());
                }
                writeResponse(request, response);
            } else {
                response.appendContent("Miss parameter");
                writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                return;
            }
        }
    }

    /*
     * This action is used to get the electable_nodes config and the cluster id of
     * the fe with the given ip and port. When one frontend start, it should check
     * the local electable_nodes config and local cluster id with other frontends.
     * If there is any difference, local fe will exit. This is designed to protect
     * the consistance of the cluster.
     */
    public static class CheckAction extends MetaBaseAction {
        private static final Logger LOG = LogManager.getLogger(CheckAction.class);
        
        public CheckAction(ActionController controller, File imageDir) {
            super(controller, imageDir);
        }
        
        public static void registerAction (ActionController controller, File imageDir) 
                throws IllegalArgException {
            controller.registerHandler(HttpMethod.GET, "/check", 
                    new CheckAction(controller, imageDir));
        }
        
        @Override
        public void executeGet(BaseRequest request, BaseResponse response) {
            try {
                Storage storage = new Storage(imageDir.getAbsolutePath());
                response.addHeader("cluster_id", Integer.toString(storage.getClusterID()));
            } catch (IOException e) {
                e.printStackTrace();
                LOG.error(e);
            }
            writeResponse(request, response);
        } 
    }
    
    public static class DumpAction extends MetaBaseAction {
        private static final Logger LOG = LogManager.getLogger(CheckAction.class);
        
        public DumpAction(ActionController controller, File imageDir) {
            super(controller, imageDir);
        }

        public static void registerAction (ActionController controller, File imageDir)
                throws IllegalArgException {
            controller.registerHandler(HttpMethod.GET, "/dump", new DumpAction(controller, imageDir));
        }

        @Override
        public boolean needAdmin() {
            return true;
        }

        @Override
        public void executeGet(BaseRequest request, BaseResponse response) {
            /*
             * Before dump, we acquired the catalog read lock and all databases' read lock and all
             * the jobs' read lock. This will guarantee the consistance of database and job queues.
             * But Backend may still inconsistent.
             */
            
            // TODO: Still need to lock ClusterInfoService to prevent add or drop Backends
            LOG.info("begin to dump meta data.");
            Catalog catalog = Catalog.getInstance();
            Map<String, Database> lockedDbMap = Maps.newTreeMap();

            String dumpFilePath;
            catalog.readLock();
            try {
                List<String> dbNames = catalog.getDbNames();
                if (dbNames == null || dbNames.isEmpty()) {
                    return;
                }

                // sort all dbs
                for (String dbName : dbNames) {
                    Database db = catalog.getDb(dbName);
                    Preconditions.checkNotNull(db);
                    lockedDbMap.put(dbName, db);
                }

                // lock all dbs
                for (Database db : lockedDbMap.values()) {
                    db.readLock();
                }
                LOG.info("acquired all the dbs' read lock.");
                
                catalog.getAlterInstance().getRollupHandler().readLock();
                catalog.getAlterInstance().getSchemaChangeHandler().readLock();
                catalog.getLoadInstance().readLock();
                
                LOG.info("acquired all jobs' read lock.");
                long journalId = catalog.getMaxJournalId();
                File dumpFile = new File(Config.meta_dir, "image." + journalId);
                dumpFilePath = dumpFile.getAbsolutePath();
                try {
                    LOG.info("begin to dump {}", dumpFilePath);
                    catalog.saveImage(dumpFile, journalId);
                } catch (IOException e) {
                    LOG.error(e);
                }
                
            } finally {
                // unlock all

                catalog.getLoadInstance().readUnlock();
                catalog.getAlterInstance().getSchemaChangeHandler().readUnLock();
                catalog.getAlterInstance().getRollupHandler().readUnLock();

                for (Database db : lockedDbMap.values()) {
                    db.readUnlock();
                }

                catalog.readUnlock();
            }
            
            LOG.info("dump finished.");
            
            response.appendContent("dump finished. " + dumpFilePath);
            writeResponse(request, response);
            return;
        }
    }
}
