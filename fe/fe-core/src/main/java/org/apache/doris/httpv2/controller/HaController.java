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

package org.apache.doris.httpv2.controller;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.ha.HAProtocol;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.persist.Storage;
import org.apache.doris.system.Frontend;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/rest/v1")
public class HaController {

    @RequestMapping(path = "/ha", method = RequestMethod.GET)
    public Object ha() {
        Map<String, Object> result = new HashMap<>();
        appendRoleInfo(result);
        appendJournalInfo(result);
        appendCanReadInfo(result);
        appendNodesInfo(result);
        appendImageInfo(result);
        appendDbNames(result);
        appendFe(result);
        appendRemovedFe(result);
        return ResponseEntityBuilder.ok(result);
    }

    private void appendRoleInfo(Map<String, Object> result) {
        Map<String, Object> info = new HashMap<>();
        List<Map<String, Object>> list = new ArrayList<>();

        info.put("Name", "FrontendRole");
        info.put("Value", Catalog.getCurrentCatalog().getFeType());
        list.add(info);
        result.put("FrontendRole", list);
    }

    private void appendJournalInfo(Map<String, Object> result) {
        Map<String, Object> info = new HashMap<>();
        List<Map<String, Object>> list = new ArrayList<>();

        if (Catalog.getCurrentCatalog().isMaster()) {
            info.put("Name", "FrontendRole");
            info.put("Value", Catalog.getCurrentCatalog().getEditLog().getMaxJournalId());
        } else {
            info.put("Name", "FrontendRole");
            info.put("Value", Catalog.getCurrentCatalog().getReplayedJournalId());
        }
        list.add(info);
        result.put("CurrentJournalId", list);
    }

    private void appendNodesInfo(Map<String, Object> result) {
        HAProtocol haProtocol = Catalog.getCurrentCatalog().getHaProtocol();
        if (haProtocol == null) {
            return;
        }
        List<InetSocketAddress> electableNodes = haProtocol.getElectableNodes(true);
        if (electableNodes.isEmpty()) {
            return;
        }

        //buffer.append("<h2>Electable nodes</h2>");
        //buffer.append("<pre>");
        List<Map<String, Object>> eleclist = new ArrayList<>();

        for (InetSocketAddress node : electableNodes) {
            Map<String, Object> info = new HashMap<>();
            info.put("Name", node.getHostName());
            info.put("Value", node.getAddress());
            eleclist.add(info);
        }
        result.put("Electablenodes", eleclist);


        List<InetSocketAddress> observerNodes = haProtocol.getObserverNodes();
        if (observerNodes == null) {
            return;
        }
        List<Map<String, Object>> list = new ArrayList<>();

        for (InetSocketAddress node : observerNodes) {
            Map<String, Object> observer = new HashMap<>();
            observer.put("Name", node.getHostName());
            observer.put("Value", node.getHostString());
            list.add(observer);
        }
        result.put("Observernodes", list);

    }

    private void appendCanReadInfo(Map<String, Object> result) {
        Map<String, Object> canRead = new HashMap<>();
        List<Map<String, Object>> list = new ArrayList<>();

        canRead.put("Name", "Status");
        canRead.put("Value", Catalog.getCurrentCatalog().canRead());
        list.add(canRead);
        result.put("CanRead", list);
    }

    private void appendImageInfo(Map<String, Object> result) {
        try {
            List<Map<String, Object>> list = new ArrayList<>();
            Map<String, Object> checkPoint = new HashMap<>();
            Storage storage = new Storage(Config.meta_dir + "/image");
            checkPoint.put("Name", "Version");
            checkPoint.put("Value", storage.getLatestImageSeq());
            list.add(checkPoint);
            long lastCheckpointTime = storage.getCurrentImageFile().lastModified();
            Date date = new Date(lastCheckpointTime);
            Map<String, Object> checkPoint1 = new HashMap<>();
            checkPoint1.put("Name", "lastCheckPointTime");
            checkPoint1.put("Value", date);
            list.add(checkPoint1);
            result.put("CheckpointInfo", list);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void appendDbNames(Map<String, Object> result) {
        Map<String, Object> dbs = new HashMap<>();

        List<Long> names = Catalog.getCurrentCatalog().getEditLog().getDatabaseNames();
        if (names == null) {
            return;
        }

        String msg = "";
        for (long name : names) {
            msg += name + " ";
        }
        List<Map<String, Object>> list = new ArrayList<>();

        dbs.put("Name", "DatabaseNames");
        dbs.put("Value", msg);
        list.add(dbs);
        result.put("databaseNames", list);
    }

    private void appendFe(Map<String, Object> result) {
        List<Frontend> fes = Catalog.getCurrentCatalog().getFrontends(null /* all */);
        if (fes == null) {
            return;
        }
        List<Map<String, Object>> list = new ArrayList<>();
        for (Frontend fe : fes) {
            Map<String, Object> allowed = new HashMap<>();
            allowed.put("Name", fe.getNodeName());
            allowed.put("Value", fe.toString());
            list.add(allowed);
        }
        result.put("allowedFrontends", list);
    }

    private void appendRemovedFe(Map<String, Object> result) {
        List<String> feNames = Catalog.getCurrentCatalog().getRemovedFrontendNames();
        List<Map<String, Object>> list = new ArrayList<>();
        for (String feName : feNames) {
            Map<String, Object> removed = new HashMap<>();
            removed.put("Name", feName);
            removed.put("Value", feName);
            list.add(removed);
        }
        result.put("removedFrontends", list);
    }
}
