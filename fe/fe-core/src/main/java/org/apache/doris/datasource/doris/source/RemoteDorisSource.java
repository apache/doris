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

package org.apache.doris.datasource.doris.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.doris.RemoteDorisExternalCatalog;
import org.apache.doris.datasource.doris.RemoteDorisExternalTable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class RemoteDorisSource {
    private final TupleDescriptor desc;
    private final RemoteDorisExternalCatalog remoteDorisExternalCatalog;
    private final RemoteDorisExternalTable remoteDorisExtTable;
    private final List<Pair<String, Integer>> hostsAndArrowPort;
    private int currentHostIndex;
    private Pair<String, Integer> currentHostAndArrowPort;

    public RemoteDorisSource(TupleDescriptor desc) {
        this.desc = desc;
        this.remoteDorisExtTable = (RemoteDorisExternalTable) desc.getTable();
        this.remoteDorisExternalCatalog = (RemoteDorisExternalCatalog) remoteDorisExtTable.getCatalog();
        this.hostsAndArrowPort = parseArrowNodes(remoteDorisExternalCatalog.getFeArrowNodes());
        this.currentHostIndex = ThreadLocalRandom.current().nextInt(hostsAndArrowPort.size());
    }

    public TupleDescriptor getDesc() {
        return desc;
    }

    public RemoteDorisExternalTable getTargetTable() {
        return remoteDorisExtTable;
    }

    public RemoteDorisExternalCatalog getCatalog() {
        return remoteDorisExternalCatalog;
    }

    public Pair<String, Integer> nextHostAndArrowPort() {
        return nextHostAndPort();
    }

    public Pair<String, Integer> getHostAndArrowPort() {
        return currentHostAndArrowPort;
    }

    private List<Pair<String, Integer>> parseArrowNodes(List<String> feArrowNodes) {
        if (feArrowNodes == null || feArrowNodes.isEmpty()) {
            throw new RuntimeException("fe arrow nodes not set");
        }

        List<Pair<String, Integer>> hostsAndArrowPort = new ArrayList<>();
        for (String feArrowNode : feArrowNodes) {
            String[] split = feArrowNode.split(":");
            if (split.length != 2) {
                throw new RuntimeException("fe arrow nodes format error, must ip:arrow_port,ip:arrow_port..");
            }
            hostsAndArrowPort.add(Pair.of(split[0].trim(), Integer.parseInt(split[1].trim())));
        }

        return hostsAndArrowPort;
    }

    private Pair<String, Integer> nextHostAndPort() {
        currentHostAndArrowPort = this.hostsAndArrowPort.get(currentHostIndex);
        currentHostIndex++;
        currentHostIndex = currentHostIndex % hostsAndArrowPort.size();
        return currentHostAndArrowPort;
    }
}
